package client

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	mqtt "github.com/dtegapp/mochi-mqtt/v2"
	"github.com/dtegapp/mochi-mqtt/v2/hooks/auth"
	"github.com/dtegapp/mochi-mqtt/v2/listeners"
	"github.com/dtegapp/mochi-mqtt/v2/packets"
)

// 이 패키지는 직접 만든 클라이언트라 실제 브로커와 붙여 검증한다.
// 접속·구독·발행·요청응답·재연결을 실제 TCP로 확인한다.
// D-TEG 20260831

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("포트 확보 실패: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startBroker는 서버간 통신과 같은 설정(평문 TCP, 인라인 클라이언트)으로 브로커를 띄운다.
func startBroker(t *testing.T) (*mqtt.Server, string) {
	t.Helper()
	addr := freePort(t)
	srv := mqtt.New(&mqtt.Options{
		InlineClient:           true,
		SplitPublishWrite:      true,
		SysTopicResendInterval: mqtt.SysTopicsDisabled,
		SharePublishPayload:    true,
	})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("AddHook: %v", err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })

	// 리스너가 실제로 받을 때까지 기다린다.
	for i := 0; i < 100; i++ {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.Close()
			return srv, addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("브로커가 %s에서 받지 않는다", addr)
	return nil, ""
}

func TestConnectPublishSubscribe(t *testing.T) {
	_, addr := startBroker(t)

	got := make(chan []byte, 1)
	sub := New(Options{Addr: addr, ClientId: "srv-sub"})
	if err := sub.Start(); err != nil {
		t.Fatalf("구독자 접속 실패: %v", err)
	}
	defer sub.Close()
	if err := sub.Subscribe("CdnRpc/PubGwMsg/+", 1, func(topic string, payload []byte, pk *packets.Packet) {
		got <- payload
	}); err != nil {
		t.Fatalf("구독 실패: %v", err)
	}

	pub := New(Options{Addr: addr, ClientId: "srv-pub"})
	if err := pub.Start(); err != nil {
		t.Fatalf("발행자 접속 실패: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("CdnRpc/PubGwMsg/gwjp", []byte("hello"), 1); err != nil {
		t.Fatalf("QoS1 발행 실패: %v", err)
	}
	select {
	case p := <-got:
		if string(p) != "hello" {
			t.Errorf("페이로드가 다르다: %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("발행한 메시지가 구독자에게 오지 않았다")
	}
}

// 와일드카드가 아닌 토픽으로 발행한 것이 다른 구독자에게 새지 않아야 한다.
func TestTopicIsolation(t *testing.T) {
	_, addr := startBroker(t)

	hit := make(chan string, 4)
	sub := New(Options{Addr: addr, ClientId: "srv-iso"})
	if err := sub.Start(); err != nil {
		t.Fatalf("접속 실패: %v", err)
	}
	defer sub.Close()
	if err := sub.Subscribe("CdnRpc/PubDeviceCmd/gwjp", 1, func(topic string, payload []byte, pk *packets.Packet) {
		hit <- topic
	}); err != nil {
		t.Fatalf("구독 실패: %v", err)
	}

	pub := New(Options{Addr: addr, ClientId: "srv-iso-pub"})
	if err := pub.Start(); err != nil {
		t.Fatalf("접속 실패: %v", err)
	}
	defer pub.Close()

	_ = pub.Publish("CdnRpc/PubDeviceCmd/gwa", []byte("x"), 1)  // 다른 GW — 오면 안 된다
	_ = pub.Publish("CdnRpc/PubDeviceCmd/gwjp", []byte("y"), 1) // 이건 와야 한다

	select {
	case topic := <-hit:
		if topic != "CdnRpc/PubDeviceCmd/gwjp" {
			t.Errorf("다른 GW 토픽이 샜다: %s", topic)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("자기 토픽 메시지가 오지 않았다")
	}
	select {
	case topic := <-hit:
		t.Errorf("받지 말아야 할 토픽을 받았다: %s", topic)
	case <-time.After(300 * time.Millisecond):
	}
}

// 요청-응답(Response Topic + Correlation Data) — CdnRpc/ReqGw의 대역이다.
func TestRequestReply(t *testing.T) {
	_, addr := startBroker(t)

	// 응답자: 요청을 받아 상관 데이터를 그대로 붙여 응답 토픽으로 돌려준다.
	responder := New(Options{Addr: addr, ClientId: "srv-cdn"})
	if err := responder.Start(); err != nil {
		t.Fatalf("응답자 접속 실패: %v", err)
	}
	defer responder.Close()
	if err := responder.Subscribe("CdnRpc/ReqGw/+", 1, func(topic string, payload []byte, pk *packets.Packet) {
		if err := responder.Reply(pk, append([]byte("re:"), payload...)); err != nil {
			t.Errorf("응답 실패: %v", err)
		}
	}); err != nil {
		t.Fatalf("구독 실패: %v", err)
	}

	req := New(Options{Addr: addr, ClientId: "srv-gwjp", ResponseTopic: "CdnRpc/Resp/gwjp"})
	if err := req.Start(); err != nil {
		t.Fatalf("요청자 접속 실패: %v", err)
	}
	defer req.Close()

	res, err := req.Request("CdnRpc/ReqGw/gwjp", []byte("LastDrvTime"), 3*time.Second)
	if err != nil {
		t.Fatalf("요청 실패: %v", err)
	}
	if string(res) != "re:LastDrvTime" {
		t.Errorf("응답이 다르다: %q", res)
	}
}

// 요청 여러 건이 동시에 나가도 상관 데이터로 짝이 맞아야 한다.
func TestRequestConcurrent(t *testing.T) {
	_, addr := startBroker(t)

	responder := New(Options{Addr: addr, ClientId: "srv-cdn2"})
	if err := responder.Start(); err != nil {
		t.Fatalf("응답자 접속 실패: %v", err)
	}
	defer responder.Close()
	_ = responder.Subscribe("CdnRpc/ReqGw/+", 1, func(topic string, payload []byte, pk *packets.Packet) {
		// 응답 순서를 섞어 짝짓기가 순서에 기대지 않는지 본다.
		time.Sleep(time.Duration(len(payload)%5) * 20 * time.Millisecond)
		_ = responder.Reply(pk, payload)
	})

	req := New(Options{Addr: addr, ClientId: "srv-gwa", ResponseTopic: "CdnRpc/Resp/gwa"})
	if err := req.Start(); err != nil {
		t.Fatalf("요청자 접속 실패: %v", err)
	}
	defer req.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			want := fmt.Sprintf("req-%02d", n)
			res, err := req.Request("CdnRpc/ReqGw/gwa", []byte(want), 5*time.Second)
			if err != nil {
				t.Errorf("요청 %d 실패: %v", n, err)
				return
			}
			if string(res) != want {
				t.Errorf("응답이 뒤바뀌었다: got=%q want=%q", res, want)
			}
		}(i)
	}
	wg.Wait()
}

// 브로커가 끊겨도 다시 붙고, 등록해 둔 구독이 되살아나야 한다.
func TestReconnectRestoresSubscription(t *testing.T) {
	srv, addr := startBroker(t)

	got := make(chan []byte, 2)
	sub := New(Options{Addr: addr, ClientId: "srv-recon", RetryWait: 200 * time.Millisecond})
	if err := sub.Start(); err != nil {
		t.Fatalf("접속 실패: %v", err)
	}
	defer sub.Close()
	_ = sub.Subscribe("CdnRpc/PubGwNoti/+", 1, func(topic string, payload []byte, pk *packets.Packet) {
		got <- payload
	})

	// 브로커가 세션을 끊는다.
	for _, cl := range srv.Clients.GetAll() {
		if cl.ID == "srv-recon" {
			cl.Stop(fmt.Errorf("test disconnect"))
		}
	}

	// 다시 붙을 때까지 기다린다.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sub.IsConnected() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sub.IsConnected() {
		t.Fatal("재연결되지 않았다")
	}
	// 구독 복구가 브로커에 반영될 틈을 준다.
	time.Sleep(300 * time.Millisecond)

	pub := New(Options{Addr: addr, ClientId: "srv-recon-pub"})
	if err := pub.Start(); err != nil {
		t.Fatalf("발행자 접속 실패: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("CdnRpc/PubGwNoti/gwjp", []byte("after"), 1); err != nil {
		t.Fatalf("발행 실패: %v", err)
	}

	select {
	case p := <-got:
		if string(p) != "after" {
			t.Errorf("페이로드가 다르다: %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("재연결 후 구독이 살아나지 않았다 — 조용히 수신이 끊긴다")
	}
}

// 끊긴 상태에서 발행하면 붙잡히지 않고 바로 실패해야 한다.
func TestPublishWhileDown(t *testing.T) {
	c := New(Options{Addr: "127.0.0.1:1", ClientId: "srv-down", RetryWait: time.Hour})
	_ = c.Start()
	defer c.Close()
	if err := c.Publish("CdnRpc/PubGwMsg/x", []byte("v"), 1); err != ErrNotConnected {
		t.Errorf("끊긴 상태 발행은 ErrNotConnected여야 한다: %v", err)
	}
}

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"CdnRpc/PubGwMsg/+", "CdnRpc/PubGwMsg/gwjp", true},
		{"CdnRpc/PubGwMsg/+", "CdnRpc/PubGwMsg/gwjp/extra", false},
		{"CdnRpc/PubGwMsg/+", "CdnRpc/PubGwMsg", false},
		{"CdnRpc/#", "CdnRpc/PubGwMsg/gwjp", true},
		{"CdnRpc/#", "CtrlRpc/PubKwObj/cdnjp", false},
		{"CtrlRpc/PubKwMsg", "CtrlRpc/PubKwMsg", true},
		{"CtrlRpc/PubKwMsg", "CtrlRpc/PubKwObj", false},
	}
	for _, c := range cases {
		if got := TopicMatch(c.filter, c.topic); got != c.want {
			t.Errorf("TopicMatch(%q,%q)=%v want=%v", c.filter, c.topic, got, c.want)
		}
	}
}
