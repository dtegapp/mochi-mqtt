package client

import (
	"bufio"
	"bytes"
	"os"
	"strings"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
)

/*
   서버간 MQTT 7회차 분석 — 2라운드(입력검증·외부입력 방어) 재현.
   발견을 증명하기 위한 것이지 수정이 아니다. 일반 `go test`는 그린을 유지한다.
   D-TEG 20260831
*/

// deafBroker: CONNECT에 CONNACK만 주고, 그 뒤로는 소켓에서 아무것도 읽지 않는
// 브로커. PINGRESP도 주지 않는다.
type deafBroker struct {
	ln      net.Listener
	reads   bool // false면 CONNECT 이후 읽지 않는다
	pings   bool // false면 PINGRESP를 주지 않는다
	accepts atomic.Int32
}

func newDeafBroker(t *testing.T, keepReading, answerPings bool) *deafBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("리스너 생성 실패: %v", err)
	}
	b := &deafBroker{ln: ln, reads: keepReading, pings: answerPings}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			b.accepts.Add(1)
			go b.handle(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return b
}

func (b *deafBroker) addr() string { return b.ln.Addr().String() }

func (b *deafBroker) handle(conn net.Conn) {
	br := bufio.NewReaderSize(conn, 4096)
	if _, err := readPacket(br); err != nil { // CONNECT
		return
	}
	ack := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ProtocolVersion: 5,
	}
	buf := new(bytes.Buffer)
	if err := ack.ConnackEncode(buf); err != nil {
		return
	}
	conn.Write(buf.Bytes())

	if !b.reads {
		select {} // 소켓을 열어둔 채 읽지 않는다 — 상대의 송신 버퍼를 채운다
	}
	for {
		pk, err := readPacket(br)
		if err != nil {
			return
		}
		if pk.FixedHeader.Type == packets.Pingreq && b.pings {
			out := &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingresp}}
			pb := new(bytes.Buffer)
			if err := out.PingrespEncode(pb); err == nil {
				conn.Write(pb.Bytes())
			}
		}
	}
}

// 발견 4: 소켓 읽기·쓰기에 기한이 없다.
//
// 접속 단계에서만 기한을 걸고 그 뒤 해제한다(dial의 SetDeadline(time.Time{})).
// 그래서 상대가 읽지 않으면 발행이 커널 송신 버퍼가 찬 시점부터 무한정 잡히고,
// 그동안 쓰기 락을 쥐고 있어 PUBACK·PINGREQ까지 함께 멈춘다.
//
// 실제로 막히는 시점은 커널 버퍼 크기에 좌우돼 재현이 환경을 탄다 — 여기서는
// 기한을 거는 호출 자체가 없다는 사실을 확인한다.
func TestRound2_NoIOReadWriteDeadline(t *testing.T) {
	buf, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("소스 읽기 실패: %v", err)
	}
	src := string(buf)

	if strings.Contains(src, "SetWriteDeadline") {
		t.Log("쓰기 기한을 거는 호출이 있다")
	} else {
		t.Log("재현됨: SetWriteDeadline 호출이 없다 — 상대가 읽지 않으면 발행이 무한정 잡히고, " +
			"쓰기 락을 쥔 채라 그 연결의 모든 송신이 함께 멈춘다")
	}

	// 접속 뒤 기한을 해제하고 다시 걸지 않는지 확인한다.
	if strings.Contains(src, "SetDeadline(time.Time{})") && !strings.Contains(src, "SetReadDeadline") {
		t.Log("재현됨: 접속 뒤 기한을 해제하고 읽기 기한을 다시 걸지 않는다 — " +
			"상대가 조용히 사라져도 읽기가 영원히 대기한다")
	}
}

// 발견 5: PINGRESP를 받지 못해도 연결을 살아 있다고 본다.
//
// keepalive는 상대가 조용히 사라진 것을 알아내려고 있는 장치인데, 보내기만 하고
// 응답을 확인하지 않는다. TCP가 끊기지 않는 단절(중간 장비 정지 등)에서는
// "붙어 있다"고 믿는 상태가 무한정 이어지고, 재연결도 걸리지 않는다.
func TestRound2_NoPingRespTimeout(t *testing.T) {
	b := newDeafBroker(t, true, false) // 읽기는 하되 PINGRESP는 주지 않는다
	c := New(Options{
		Addr:      b.addr(),
		ClientId:  "srv-round2b",
		Keepalive: 2, // PINGREQ를 1초마다 보낸다
		RetryWait: time.Hour,
	})
	defer c.Close()
	if err := c.Start(); err != nil {
		t.Fatalf("접속 실패: %v", err)
	}

	// keepalive 주기의 여러 배를 기다린다 — 규격대로면 1.5배 안에 끊어야 한다.
	time.Sleep(4 * time.Second)

	if c.IsConnected() {
		t.Logf("재현됨: PINGRESP를 %s 동안 한 번도 받지 못했는데 접속 표시가 그대로다 — "+
			"keepalive 응답을 확인하지 않아 조용한 단절을 못 잡는다", 4*time.Second)
	} else {
		t.Logf("접속 표시가 내려갔다 — 응답 확인이 동작한다")
	}
}

// 발견 6: 수신 패킷 크기에 상한이 없다.
//
// 남은 길이(Remaining Length)를 그대로 믿고 그만큼 한 번에 할당한다. 규격상
// 최대 256MB까지 올 수 있어, 상대가 잘못된 값을 보내면 그만큼을 한 번에 잡는다.
// 브로커에는 최대 크기 제한이 있지만 이 클라이언트에는 없다.
func TestRound2_ReadHasNoSizeLimit(t *testing.T) {
	// 소켓을 직접 만들어 큰 남은 길이만 흘려 넣는다(본문은 보내지 않는다).
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	go func() {
		// PUBLISH, 남은 길이 = 200MB
		srv.Write([]byte{0x30, 0x80, 0x80, 0x80, 0x64})
		time.Sleep(500 * time.Millisecond)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReaderSize(cli, 4096)
		_, _ = readPacket(br) // 본문을 기다리며 200MB를 미리 잡는다
	}()

	select {
	case <-done:
		t.Log("읽기가 곧바로 끝났다")
	case <-time.After(700 * time.Millisecond):
		t.Logf("재현됨: 남은 길이를 검사 없이 받아들여 그 크기만큼 미리 할당한 채 본문을 기다린다 — " +
			"상한이 없다")
	}
}
