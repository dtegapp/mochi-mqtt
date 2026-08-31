// Package client는 서버간 통신용 MQTT 5 클라이언트다.
//
// mochi-mqtt는 본래 브로커 전용이라 클라이언트가 없다. DCS는 CDN·CTRL이 브로커,
// GW·WEB·CDN이 클라이언트로 붙는 구조라 양쪽이 필요한데, 외부 클라이언트
// 라이브러리를 따로 쓰면 MQTT 구현이 둘로 갈린다. 같은 packets 코덱을 공유해
// 한 벌로 유지하려고 이 패키지를 포크에 넣는다.
//
// 서버간 통신에 필요한 것만 담는다 — QoS 0/1, 요청-응답(Response Topic +
// Correlation Data), 자동 재연결. QoS 2와 Will·Retain은 쓰지 않는다.
//
// D-TEG 20260831
package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
)

var (
	ErrNotConnected = errors.New("mqtt client: not connected")
	ErrTimeout      = errors.New("mqtt client: timeout")
	ErrRejected     = errors.New("mqtt client: connection rejected")
	ErrClosed       = errors.New("mqtt client: closed")
)

// Handler는 수신 메시지 처리기다. pk는 응답 토픽 등 속성이 필요할 때만 쓴다.
type Handler func(topic string, payload []byte, pk *packets.Packet)

// Options는 접속 설정이다. Addr와 ClientId만 필수다.
type Options struct {
	Addr      string // "10.0.0.1:38223"
	ClientId  string
	Username  string
	Password  string
	Keepalive uint16 // 초. 0이면 30

	// ResponseTopic이 있으면 접속 직후 자동 구독한다. Request를 쓰려면 필요하다.
	ResponseTopic string

	AckTimeout   time.Duration // QoS1 PUBACK·SUBACK 대기. 0이면 10초
	RetryWait    time.Duration // 재연결 간격. 0이면 5초
	ConnTimeout  time.Duration // TCP dial + CONNACK 대기. 0이면 10초
	WriteTimeout time.Duration // 한 패킷을 내보내는 기한. 0이면 30초
	MaxPacketSize int          // 받아들일 최대 패킷 크기(바이트). 0이면 16MB

	// RequireSubscribe가 참이면 구독을 하나라도 걸지 못했을 때 접속을 실패로 본다.
	// 붙어는 있는데 아무것도 받지 못하는 상태를 만들지 않으려는 것이다.
	RequireSubscribe bool
	OnConnect    func(*Client)
	OnDisconnect func(*Client, error)
	OnLog        func(format string, a ...any)
}

type subscription struct {
	filter  string
	qos     byte
	handler Handler
}

// Client는 브로커 하나에 붙는 세션이다. 연결이 끊기면 스스로 다시 붙고,
// 등록해 둔 구독을 재연결 때마다 다시 건다.
type Client struct {
	opt Options

	wmu  sync.Mutex // 소켓 쓰기 직렬화
	conn net.Conn

	up     atomic.Bool
	closed atomic.Bool

	pid atomic.Uint32

	submu sync.RWMutex
	subs  []subscription

	ackmu sync.Mutex
	acks  map[uint16]chan byte

	reqmu sync.Mutex
	reqs  map[string]chan *packets.Packet

	corr atomic.Uint64

	recv chan *packets.Packet
	done chan struct{}

	// readLimit: 이 시간 동안 아무 패킷도 오지 않으면 끊긴 것으로 본다.
	// PINGRESP도 패킷이므로, 이 기한이 곧 keepalive 응답을 확인하는 장치다.
	readLimit time.Duration

	// cbwg: 접속·단절 통지가 끝났는지 추적한다. Close가 이걸 기다려야 호출부가
	// 정리한 상태를 뒤늦은 통지가 다시 건드리지 않는다.
	cbwg sync.WaitGroup
}

// New는 클라이언트를 만든다. 실제 접속은 Start에서 한다.
func New(opt Options) *Client {
	if opt.Keepalive == 0 {
		opt.Keepalive = 30
	}
	if opt.AckTimeout == 0 {
		opt.AckTimeout = 10 * time.Second
	}
	if opt.RetryWait == 0 {
		opt.RetryWait = 5 * time.Second
	}
	if opt.ConnTimeout == 0 {
		opt.ConnTimeout = 10 * time.Second
	}
	if opt.WriteTimeout == 0 {
		opt.WriteTimeout = 30 * time.Second
	}
	if opt.MaxPacketSize == 0 {
		opt.MaxPacketSize = 16 << 20 // 16MB — 서버간 최대 페이로드(스냅샷·MDT)를 넉넉히 덮는다
	}
	return &Client{
		opt: opt,
		// 살아 있으면 keepalive 절반 주기마다 PINGREQ가 나가고 PINGRESP가 돌아온다.
		// 두 주기 동안 아무것도 없으면 조용히 끊긴 것으로 본다.
		readLimit: time.Duration(opt.Keepalive) * time.Second * 2,
		acks: make(map[uint16]chan byte),
		reqs: make(map[string]chan *packets.Packet),
		recv: make(chan *packets.Packet, 256),
		done: make(chan struct{}),
	}
}

// guard는 고루틴·콜백에서 올라온 패닉을 붙잡아 로그로 바꾼다. 이 클라이언트가
// 띄우는 고루틴은 전부 이걸 통과해야 한다 — 하나라도 맨몸이면 호출부가 등록한
// 핸들러의 패닉이 프로세스 전체를 내린다.
func (c *Client) guard(where string) {
	if v := recover(); v != nil {
		c.logf("mqtt client panic recovered (%s): %v\n%s", where, v, debug.Stack())
	}
}

func (c *Client) logf(format string, a ...any) {
	if c.opt.OnLog != nil {
		c.opt.OnLog(format, a...)
	}
}

// IsConnected는 지금 브로커에 붙어 있는지를 알려준다.
func (c *Client) IsConnected() bool { return c.up.Load() }

// ClientId는 이 세션의 식별자다.
func (c *Client) ClientId() string { return c.opt.ClientId }

// Addr은 접속 대상 브로커 주소다.
func (c *Client) Addr() string { return c.opt.Addr }

// Start는 한 번 접속을 시도하고, 성공하면 유지 루프를 띄운다. 첫 접속에
// 실패해도 루프는 계속 재시도하므로 호출부가 직접 재시도할 필요는 없다.
func (c *Client) Start() error {
	err := c.dial()
	go c.dispatchLoop()
	go c.keeper()
	return err
}

// Close는 세션을 끝낸다. 재연결 루프도 멈추고, 진행 중인 접속·단절 통지가 끝나기를
// 잠시 기다린다 — 기다리지 않으면 호출부가 정리한 상태를 뒤늦은 통지가 건드린다.
//
// 통지 함수 안에서 불러도 멈추지 않는다. 그 경우 자기 자신을 기다리게 되므로
// 기한을 두고 그냥 진행한다.
func (c *Client) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.done)
	c.disconnect(ErrClosed)

	done := make(chan struct{})
	go func() {
		defer c.guard("Close.wait")
		c.cbwg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeWaitLimit):
		c.logf("mqtt close: 통지가 끝나기를 기다리다 %s가 지나 그대로 진행한다(통지 안에서 Close를 불렀을 수 있다)", closeWaitLimit)
	}
}

// closeWaitLimit: Close가 진행 중인 통지를 기다리는 한도.
const closeWaitLimit = 2 * time.Second

// keeper는 연결이 끊긴 동안 재접속을 반복한다.
func (c *Client) keeper() {
	defer c.guard("keeper")
	for {
		select {
		case <-c.done:
			return
		case <-time.After(c.opt.RetryWait):
		}
		if c.closed.Load() {
			return
		}
		if c.up.Load() {
			continue
		}
		if err := c.dial(); err != nil {
			c.logf("mqtt reconnect %s: %v", c.opt.Addr, err)
		}
	}
}

// dial은 TCP 연결부터 CONNACK 확인, 구독 복구까지 한 번에 처리한다.
func (c *Client) dial() error {
	if c.closed.Load() {
		return ErrClosed
	}
	conn, err := net.DialTimeout("tcp", c.opt.Addr, c.opt.ConnTimeout)
	if err != nil {
		return err
	}

	br := bufio.NewReaderSize(conn, 4096)

	conn.SetDeadline(time.Now().Add(c.opt.ConnTimeout))
	if err := c.writeTo(conn, c.connectPacket()); err != nil {
		conn.Close()
		return err
	}
	pk, err := readPacket(br)
	if err != nil {
		conn.Close()
		return err
	}
	if pk.FixedHeader.Type != packets.Connack {
		conn.Close()
		return fmt.Errorf("mqtt client: expected connack, got %d", pk.FixedHeader.Type)
	}
	if pk.ReasonCode >= packets.ErrUnspecifiedError.Code {
		conn.Close()
		return fmt.Errorf("%w: reason=0x%02x", ErrRejected, pk.ReasonCode)
	}
	// 접속 기한은 풀되 읽기 기한은 계속 건다 — 풀어 두면 상대가 조용히 사라져도
	// 읽기가 영원히 대기하고, 그동안 붙어 있다고 믿는다.
	conn.SetWriteDeadline(time.Time{})
	conn.SetReadDeadline(time.Now().Add(c.readLimit))

	c.wmu.Lock()
	c.conn = conn
	c.wmu.Unlock()
	c.up.Store(true)

	go c.readLoop(conn, br)
	go c.pinger(conn)

	// 응답 토픽과 기존 구독을 되건다 — 재연결 때 조용히 수신이 끊기는 걸 막는다.
	var subErr error
	if c.opt.ResponseTopic != "" {
		if err := c.sendSubscribe(c.opt.ResponseTopic, 1); err != nil {
			c.logf("mqtt resubscribe(response) %s: %v", c.opt.ResponseTopic, err)
			subErr = err
		}
	}
	c.submu.RLock()
	subs := make([]subscription, len(c.subs))
	copy(subs, c.subs)
	c.submu.RUnlock()
	for _, s := range subs {
		if err := c.sendSubscribe(s.filter, s.qos); err != nil {
			c.logf("mqtt resubscribe %s: %v", s.filter, err)
			subErr = err
		}
	}

	// 구독을 걸지 못한 채로 붙어 있으면 "연결은 됐는데 아무것도 받지 못하는" 상태가
	// 된다. 겉으로는 정상이라 어디에도 드러나지 않으므로, 접속 자체를 실패로 돌려
	// 재시도에 맡긴다.
	if subErr != nil && c.opt.RequireSubscribe {
		c.disconnect(subErr)
		return fmt.Errorf("mqtt client: 구독을 걸지 못해 접속을 취소한다: %w", subErr)
	}

	if c.opt.OnConnect != nil {
		c.cbwg.Add(1)
		go func() {
			defer c.cbwg.Done()
			defer c.guard("OnConnect")
			c.opt.OnConnect(c)
		}()
	}
	return nil
}

func (c *Client) connectPacket() *packets.Packet {
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: c.opt.ClientId,
			Clean:            true,
			Keepalive:        c.opt.Keepalive,
		},
	}
	if c.opt.Username != "" {
		pk.Connect.UsernameFlag = true
		pk.Connect.Username = []byte(c.opt.Username)
	}
	if c.opt.Password != "" {
		pk.Connect.PasswordFlag = true
		pk.Connect.Password = []byte(c.opt.Password)
	}
	return pk
}

// disconnect는 현재 연결을 닫고 대기 중인 호출을 모두 깨운다.
func (c *Client) disconnect(cause error) {
	if !c.up.Swap(false) {
		return
	}
	c.wmu.Lock()
	conn := c.conn
	c.conn = nil
	c.wmu.Unlock()
	if conn != nil {
		conn.Close()
	}

	// 대기 중인 ack·요청을 닫아 준다 — 안 그러면 타임아웃까지 붙잡혀 있다.
	c.ackmu.Lock()
	for id, ch := range c.acks {
		close(ch)
		delete(c.acks, id)
	}
	c.ackmu.Unlock()

	c.reqmu.Lock()
	for id, ch := range c.reqs {
		close(ch)
		delete(c.reqs, id)
	}
	c.reqmu.Unlock()

	if c.opt.OnDisconnect != nil {
		c.cbwg.Add(1)
		go func() {
			defer c.cbwg.Done()
			defer c.guard("OnDisconnect")
			c.opt.OnDisconnect(c, cause)
		}()
	}
}

// readLoop는 소켓에서 패킷을 읽어 종류별로 나눈다. 연결이 끊기면 반환한다.
func (c *Client) readLoop(conn net.Conn, br *bufio.Reader) {
	defer c.guard("readLoop")
	for {
		// 다음 패킷을 기다릴 기한. 정상 상태라면 keepalive 주기마다 최소 PINGRESP가 온다.
		if err := conn.SetReadDeadline(time.Now().Add(c.readLimit)); err != nil {
			c.disconnect(err)
			return
		}
		pk, err := readPacketLimit(br, c.opt.MaxPacketSize)
		if err != nil {
			if !c.closed.Load() {
				c.logf("mqtt read %s: %v", c.opt.Addr, err)
			}
			c.disconnect(err)
			return
		}
		switch pk.FixedHeader.Type {
		case packets.Publish:
			c.onPublish(pk)
		case packets.Puback, packets.Suback, packets.Unsuback:
			c.onAck(pk)
		case packets.Pingresp:
			// 살아 있다는 신호. 따로 할 일 없다.
		case packets.Disconnect:
			c.disconnect(fmt.Errorf("broker disconnect: reason=0x%02x", pk.ReasonCode))
			return
		}
	}
}

// onPublish는 수신 메시지를 응답 대기자 또는 구독 핸들러로 보낸다.
func (c *Client) onPublish(pk *packets.Packet) {
	if pk.FixedHeader.Qos == 1 && pk.PacketID != 0 {
		ack := &packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pk.PacketID,
			// PUBACK은 v3.1.1 형태로 보낸다 — 이유 코드·속성 없이 2바이트면 충분하고,
			// v5로 보내면 브로커가 이유 코드를 기대하므로 굳이 늘리지 않는다.
			ProtocolVersion: 4,
		}
		if err := c.write(ack); err != nil {
			c.logf("mqtt puback: %v", err)
		}
	}

	// 응답 토픽으로 온 것은 Request 대기자에게 넘긴다.
	if len(pk.Properties.CorrelationData) > 0 {
		key := string(pk.Properties.CorrelationData)
		c.reqmu.Lock()
		ch, ok := c.reqs[key]
		if ok {
			delete(c.reqs, key)
		}
		c.reqmu.Unlock()
		if ok {
			ch <- pk
			close(ch)
			return
		}
	}

	// 핸들러는 전용 고루틴에서 순서대로 돌린다. 수신 루프에서 직접 부르면 핸들러가
	// 무엇을 기다리는 순간 응답·ack 수신까지 함께 멈춘다.
	select {
	case c.recv <- pk:
	default:
		// 처리가 밀렸다. 순서를 포기하더라도 수신 루프는 막지 않는다.
		c.logf("mqtt recv queue full — %s를 순서 밖에서 처리한다", pk.TopicName)
		go c.dispatch(pk)
	}
}

// dispatchLoop는 수신 메시지를 순서대로 핸들러에 넘긴다.
func (c *Client) dispatchLoop() {
	defer c.guard("dispatchLoop")
	for {
		select {
		case <-c.done:
			return
		case pk := <-c.recv:
			c.dispatch(pk)
		}
	}
}

// dispatch는 등록된 수신 핸들러를 부른다. 핸들러 패닉이 여기서 멈추지 않으면
// 이 고루틴이 죽고, 그게 곧 프로세스 종료다.
func (c *Client) dispatch(pk *packets.Packet) {
	defer c.guard("dispatch:" + pk.TopicName)
	c.submu.RLock()
	subs := make([]subscription, len(c.subs))
	copy(subs, c.subs)
	c.submu.RUnlock()
	for _, s := range subs {
		if TopicMatch(s.filter, pk.TopicName) {
			s.handler(pk.TopicName, pk.Payload, pk)
		}
	}
}

// onAck는 PacketID로 대기 중인 발행·구독을 깨운다.
func (c *Client) onAck(pk *packets.Packet) {
	c.ackmu.Lock()
	ch, ok := c.acks[pk.PacketID]
	if ok {
		delete(c.acks, pk.PacketID)
	}
	c.ackmu.Unlock()
	if !ok {
		return
	}
	code := pk.ReasonCode
	if len(pk.ReasonCodes) > 0 {
		code = pk.ReasonCodes[0]
	}
	ch <- code
	close(ch)
}

// pinger는 keepalive의 절반 주기로 PINGREQ를 보낸다.
func (c *Client) pinger(conn net.Conn) {
	defer c.guard("pinger")
	iv := time.Duration(c.opt.Keepalive) * time.Second / 2
	if iv < time.Second {
		iv = time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		c.wmu.Lock()
		cur := c.conn
		c.wmu.Unlock()
		if cur != conn { // 재연결됐다 — 이 pinger는 물러난다
			return
		}
		if err := c.write(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}}); err != nil {
			return
		}
	}
}

// nextPacketID는 1~65535를 돌려 쓴다. 0은 규격상 못 쓴다.
func (c *Client) nextPacketID() uint16 {
	for {
		v := uint16(c.pid.Add(1))
		if v != 0 {
			return v
		}
	}
}

func (c *Client) newCorrelation() []byte {
	return []byte(fmt.Sprintf("%s-%d", c.opt.ClientId, c.corr.Add(1)))
}

// Subscribe는 구독을 등록하고 브로커에 건다. 등록은 남아 있어서 재연결 때
// 자동으로 다시 걸린다.
func (c *Client) Subscribe(filter string, qos byte, h Handler) error {
	if h == nil {
		return errors.New("mqtt client: nil handler")
	}
	c.submu.Lock()
	replaced := false
	for i := range c.subs {
		if c.subs[i].filter == filter {
			c.subs[i] = subscription{filter: filter, qos: qos, handler: h}
			replaced = true
			break
		}
	}
	if !replaced {
		c.subs = append(c.subs, subscription{filter: filter, qos: qos, handler: h})
	}
	c.submu.Unlock()

	if !c.up.Load() {
		return nil // 등록만 해 둔다 — 접속되면 dial이 되건다
	}
	return c.sendSubscribe(filter, qos)
}

func (c *Client) sendSubscribe(filter string, qos byte) error {
	id := c.nextPacketID()
	pk := &packets.Packet{
		// SUBSCRIBE의 고정 헤더 하위 비트는 규격상 0010이라 Qos 자리에 1을 넣는다.
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 5,
		PacketID:        id,
		Filters:         packets.Subscriptions{{Filter: filter, Qos: qos}},
	}
	code, err := c.writeAwaitAck(pk, id)
	if err != nil {
		return err
	}
	if code > 2 { // 0·1·2는 허가된 최대 QoS, 그 위는 거부
		return fmt.Errorf("mqtt client: subscribe %s rejected: 0x%02x", filter, code)
	}
	return nil
}

// Publish는 메시지를 보낸다. qos 1이면 PUBACK까지 기다린다.
func (c *Client) Publish(topic string, payload []byte, qos byte) error {
	return c.PublishWith(topic, payload, qos, nil)
}

// PublishWith는 MQTT 5 속성을 실어 보낸다. 응답 토픽·상관 데이터를 직접
// 지정할 때 쓴다.
func (c *Client) PublishWith(topic string, payload []byte, qos byte, props *packets.Properties) error {
	if !c.up.Load() {
		return ErrNotConnected
	}
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: qos},
		ProtocolVersion: 5,
		TopicName:       topic,
		Payload:         payload,
	}
	if props != nil {
		pk.Properties = *props
		// mochi는 이 플래그가 있어야 응답 토픽·상관 데이터를 실어 보낸다.
		pk.Mods.AllowResponseInfo = true
	}
	if qos > 0 {
		pk.PacketID = c.nextPacketID()
	}
	// PUBACK은 기다리지 않는다. 핸들러가 수신 루프에서 도는데 거기서 응답을 보내며
	// 자기 PUBACK을 기다리면 그 루프가 스스로 막힌다. 브로커까지의 도달은 TCP가
	// 보장하므로 서버간 통신에는 이걸로 충분하다.
	return c.write(pk)
}

// Request는 응답이 올 때까지 기다리는 요청이다. Options.ResponseTopic이
// 있어야 쓸 수 있다 — 응답을 받을 곳이 없으면 답을 못 듣는다.
func (c *Client) Request(topic string, payload []byte, timeout time.Duration) ([]byte, error) {
	if c.opt.ResponseTopic == "" {
		return nil, errors.New("mqtt client: ResponseTopic not set")
	}
	if !c.up.Load() {
		return nil, ErrNotConnected
	}
	corr := c.newCorrelation()
	ch := make(chan *packets.Packet, 1)
	c.reqmu.Lock()
	c.reqs[string(corr)] = ch
	c.reqmu.Unlock()

	err := c.PublishWith(topic, payload, 1, &packets.Properties{
		ResponseTopic:   c.opt.ResponseTopic,
		CorrelationData: corr,
	})
	if err != nil {
		c.reqmu.Lock()
		delete(c.reqs, string(corr))
		c.reqmu.Unlock()
		return nil, err
	}

	if timeout <= 0 {
		timeout = c.opt.AckTimeout
	}
	select {
	case pk, ok := <-ch:
		if !ok || pk == nil {
			return nil, ErrNotConnected // 연결이 끊겨 대기가 정리됐다
		}
		return pk.Payload, nil
	case <-time.After(timeout):
		c.reqmu.Lock()
		delete(c.reqs, string(corr))
		c.reqmu.Unlock()
		return nil, ErrTimeout
	}
}

// Reply는 받은 요청의 응답 토픽으로 답을 보낸다. 상관 데이터를 그대로
// 돌려줘야 요청자가 짝을 찾는다.
func (c *Client) Reply(req *packets.Packet, payload []byte) error {
	if req == nil || req.Properties.ResponseTopic == "" {
		return errors.New("mqtt client: no response topic")
	}
	return c.PublishWith(req.Properties.ResponseTopic, payload, 1, &packets.Properties{
		CorrelationData: req.Properties.CorrelationData,
	})
}

// writeAwaitAck는 패킷을 보내고 같은 PacketID의 ack를 기다린다.
func (c *Client) writeAwaitAck(pk *packets.Packet, id uint16) (byte, error) {
	ch := make(chan byte, 1)
	c.ackmu.Lock()
	c.acks[id] = ch
	c.ackmu.Unlock()

	if err := c.write(pk); err != nil {
		c.ackmu.Lock()
		delete(c.acks, id)
		c.ackmu.Unlock()
		return 0, err
	}
	select {
	case code, ok := <-ch:
		if !ok {
			return 0, ErrNotConnected
		}
		return code, nil
	case <-time.After(c.opt.AckTimeout):
		c.ackmu.Lock()
		delete(c.acks, id)
		c.ackmu.Unlock()
		return 0, ErrTimeout
	}
}

func (c *Client) write(pk *packets.Packet) error {
	c.wmu.Lock()
	conn := c.conn
	c.wmu.Unlock()
	if conn == nil {
		return ErrNotConnected
	}
	return c.writeTo(conn, pk)
}

// writeTo는 한 패킷을 통째로 내보낸다. 소켓 쓰기는 직렬화해야 패킷이 섞이지 않는다.
func (c *Client) writeTo(conn net.Conn, pk *packets.Packet) error {
	buf := new(bytes.Buffer)
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(buf)
	case packets.Publish:
		err = pk.PublishEncode(buf)
	case packets.Puback:
		err = pk.PubackEncode(buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(buf)
	default:
		err = fmt.Errorf("mqtt client: unsupported packet type %d", pk.FixedHeader.Type)
	}
	if err != nil {
		return err
	}

	c.wmu.Lock()
	defer c.wmu.Unlock()
	// 기한이 없으면 상대가 읽지 않을 때 이 쓰기가 무한정 잡히고, 락을 쥔 채라
	// 그 연결의 모든 송신(PUBACK·PINGREQ 포함)이 함께 멈춘다.
	if err := conn.SetWriteDeadline(time.Now().Add(c.opt.WriteTimeout)); err != nil {
		return err
	}
	_, err = conn.Write(buf.Bytes())
	return err
}

// readPacket은 소켓에서 패킷 하나를 읽어 낸다(크기 제한 없음 — 접속 단계 전용).
func readPacket(br *bufio.Reader) (*packets.Packet, error) {
	return readPacketLimit(br, 0)
}

// readPacketLimit은 최대 크기를 넘는 패킷을 거절한다. 남은 길이를 그대로 믿고
// 할당하면 상대가 잘못된 값 하나로 최대 256MB를 잡게 만들 수 있다.
func readPacketLimit(br *bufio.Reader, maxSize int) (*packets.Packet, error) {
	hb, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	pk := &packets.Packet{ProtocolVersion: 5}
	if err := pk.FixedHeader.Decode(hb); err != nil {
		return nil, err
	}
	n, _, err := packets.DecodeLength(br)
	if err != nil {
		return nil, err
	}
	pk.FixedHeader.Remaining = n
	if maxSize > 0 && n > maxSize {
		return nil, fmt.Errorf("mqtt client: packet too large: %d > %d", n, maxSize)
	}

	body := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, err
		}
	}

	switch pk.FixedHeader.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	case packets.Unsuback:
		err = pk.UnsubackDecode(body)
	case packets.Pingresp:
		err = pk.PingrespDecode(body)
	case packets.Disconnect:
		err = pk.DisconnectDecode(body)
	default:
		// 서버간 통신에서 쓰지 않는 종류는 조용히 흘린다.
	}
	if err != nil {
		return nil, err
	}
	return pk, nil
}

// TopicMatch는 구독 필터가 토픽에 맞는지 본다. +는 한 단계, #는 그 아래 전부다.
func TopicMatch(filter, topic string) bool {
	if filter == topic {
		return true
	}
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := range f {
		if f[i] == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if f[i] != "+" && f[i] != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}
