package client

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
)

/*
   서버간 MQTT 7회차 분석 — 1라운드(동시성) 재현.

   이 파일은 발견을 증명하기 위한 것이지 수정이 아니다. 일반 `go test`가 붉어지지
   않도록, 결함이 재현되면 t.Log로 남기고 PASS를 유지한다.
   D-TEG 20260831
*/

// slowBroker: CONNECT를 받고 CONNACK을 지정한 시간만큼 늦게 보내는 가짜 브로커.
// 접속 시도 횟수를 센다.
type slowBroker struct {
	ln      net.Listener
	delay   time.Duration
	accepts atomic.Int32
	mu      sync.Mutex
	conns   []net.Conn
}

func newSlowBroker(t *testing.T, delay time.Duration) *slowBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("리스너 생성 실패: %v", err)
	}
	b := &slowBroker{ln: ln, delay: delay}
	go b.serve()
	t.Cleanup(func() {
		ln.Close()
		b.mu.Lock()
		for _, c := range b.conns {
			c.Close()
		}
		b.mu.Unlock()
	})
	return b
}

func (b *slowBroker) addr() string { return b.ln.Addr().String() }

func (b *slowBroker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.accepts.Add(1)
		b.mu.Lock()
		b.conns = append(b.conns, conn)
		b.mu.Unlock()
		go b.handle(conn)
	}
}

func (b *slowBroker) handle(conn net.Conn) {
	br := bufio.NewReaderSize(conn, 4096)
	if _, err := readPacket(br); err != nil { // CONNECT
		return
	}
	time.Sleep(b.delay)
	ack := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ProtocolVersion: 5,
		ReasonCode:      0,
	}
	buf := new(bytes.Buffer)
	if err := ack.ConnackEncode(buf); err != nil {
		return
	}
	conn.Write(buf.Bytes())
	// 이후 들어오는 것은 읽어서 버린다(SUBSCRIBE 등) — 응답은 하지 않는다.
	for {
		if _, err := readPacket(br); err != nil {
			return
		}
	}
}

// 확인 1: dial에 동시 실행 가드가 없지만, 현재 호출 구조에서는 겹치지 않는다.
//
// Start()는 dial을 먼저 끝내고 나서 keeper를 띄우고, keeper는 순차 루프라 자기
// 자신과 겹치지 않는다. 호출부(GW·CDN·WEB)도 클라이언트가 이미 있으면 다시
// 만들지 않는다. 즉 방어가 없는 것은 사실이나 지금 트리거가 없다 —
// 진입점이 늘어나면 그때 성립하므로 이 검증을 남겨 둔다.
func TestRound1_DialHasNoConcurrencyGuard(t *testing.T) {
	b := newSlowBroker(t, 900*time.Millisecond) // CONNACK을 늦게 준다
	c := New(Options{
		Addr:        b.addr(),
		ClientId:    "srv-round1",
		RetryWait:   150 * time.Millisecond, // Start의 dial이 끝나기 전에 keeper가 깨어난다
		ConnTimeout: 5 * time.Second,
	})
	defer c.Close()

	go func() { _ = c.Start() }()
	time.Sleep(1500 * time.Millisecond)

	if n := b.accepts.Load(); n > 1 {
		t.Logf("접속이 %d번 만들어졌다 — 동시 dial이 실제로 성립한다", n)
	} else {
		t.Logf("접속 %d회 — 현재 호출 구조에서는 dial이 겹치지 않는다(방어는 여전히 없음)", n)
	}
}

// 발견 2: 구독이 실패해도 접속은 성공으로 처리된다.
//
// dial은 구독을 되걸며 SUBACK을 AckTimeout(기본 10초)까지 기다리는데, 실패하면
// 로그만 남기고 접속을 성공으로 끝낸다. 그러면 붙어는 있는데 명령을 받지 못하는
// 상태가 되고, 겉으로는 정상이라 아무 데도 드러나지 않는다.
// 지연 자체는 호출부가 전부 go로 감싸 서비스를 막지는 않지만, 그동안 진입 가드가
// 잠겨 있어 그 서버에 대한 재시도가 그만큼 늦어진다.
func TestRound1_StartBlocksOnMissingSuback(t *testing.T) {
	b := newSlowBroker(t, 0) // CONNACK은 바로, SUBACK은 영영 주지 않는다
	c := New(Options{
		Addr:       b.addr(),
		ClientId:   "srv-round1b",
		AckTimeout: 400 * time.Millisecond, // 실제 기본값은 10초
		RetryWait:  time.Hour,
	})
	defer c.Close()

	// 구독 2개를 미리 등록해 둔다 — GW가 CDN에 붙을 때와 같은 모양이다.
	_ = c.Subscribe("CdnRpc/PubDeviceCmd/gwtest", 1, func(string, []byte, *packets.Packet) {})
	_ = c.Subscribe("CdnRpc/CallGwCmd/gwtest", 1, func(string, []byte, *packets.Packet) {})

	start := time.Now()
	_ = c.Start()
	elapsed := time.Since(start)

	// AckTimeout 2회분(구독 2개)만큼 잡혀 있어야 재현이다.
	if elapsed >= 700*time.Millisecond {
		t.Logf("재현됨: Start()가 %v 동안 잡혀 있었다(구독 수 × AckTimeout). "+
			"운영 기본값이면 구독 2개에 최대 20초", elapsed)
	} else {
		t.Logf("이번 실행에서는 재현되지 않음(%v)", elapsed)
	}
	if c.IsConnected() {
		t.Logf("재현됨: 구독이 하나도 걸리지 않았는데 접속은 성공으로 남아 있다 — " +
			"붙어는 있는데 명령을 받지 못하는 상태가 조용히 만들어진다")
	}
}

// 확인 3: 옛 연결의 읽기 루프가 현재 연결을 끊을 수 있다 — 단, 지금은 그 상황이
// 만들어지지 않는다.
//
// readLoop은 자기 conn에 묶여 있는데 disconnect()는 c.conn(현재 연결)을 닫는다.
// 아래는 dial을 직접 불러 연결을 인위적으로 교체한 뒤를 본 것이고, 운영 경로에서는
// 재연결이 항상 "끊김 → disconnect → 재접속" 순서라 두 연결이 공존하지 않는다.
// 재연결 방식이 바뀌면(예: 끊기 전에 미리 붙는 방식) 그때 성립한다.
func TestRound1_StaleReadLoopClosesCurrentConn(t *testing.T) {
	b := newSlowBroker(t, 0)
	c := New(Options{Addr: b.addr(), ClientId: "srv-round1c", RetryWait: time.Hour})
	defer c.Close()
	if err := c.Start(); err != nil {
		t.Fatalf("접속 실패: %v", err)
	}

	// 새 연결로 교체된 상황을 만든다.
	old := c.conn
	if err := c.dial(); err != nil {
		t.Fatalf("두 번째 접속 실패: %v", err)
	}
	cur := c.conn
	if old == cur {
		t.Fatal("연결이 교체되지 않았다 — 전제 성립 실패")
	}

	// 옛 연결의 읽기 루프가 끝나는 상황(소켓 종료)을 만든다.
	old.Close()
	time.Sleep(300 * time.Millisecond)

	if !c.IsConnected() {
		t.Logf("재현됨: 옛 연결이 끊겼을 뿐인데 현재 연결까지 내려갔다 — " +
			"disconnect가 conn을 구분하지 않는다")
	} else {
		t.Logf("이번 실행에서는 접속 표시가 유지됨 — 타이밍에 따라 갈린다")
	}
}
