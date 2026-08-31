package client

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
)

/*
   서버간 MQTT 7회차 분석 — 5라운드(에러처리·복구) 재현.
   발견을 증명하기 위한 것이지 수정이 아니다.
   D-TEG 20260831
*/

// 발견 9: 클라이언트가 띄우는 고루틴과 통지 콜백 어디에도 복구 처리가 없다.
//
// readLoop·dispatchLoop·dispatch·pinger·keeper 다섯 고루틴과, 접속·단절 통지를
// 띄우는 두 자리가 전부 맨몸이다. 특히 dispatch는 호출부가 등록한 수신 핸들러를
// 직접 부르므로, 그 핸들러가 패닉하면 프로세스 전체가 내려간다.
//
// GW·WEB은 자기 구독 래퍼(mqttSrvSubscribe)에서 감싸고 있어 그 경로는 보호되지만,
// 라이브러리 자체가 보장하지 않으므로 감싸지 않은 호출부가 하나만 생겨도 뚫린다.
// 접속·단절 통지(OnConnect/OnDisconnect)는 감싸는 자리가 아예 없다.
func TestRound5_NoRecoverInClientGoroutines(t *testing.T) {
	buf, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("소스 읽기 실패: %v", err)
	}
	src := string(buf)

	// 고루틴으로 도는 함수와 콜백을 띄우는 자리
	targets := []string{"readLoop", "dispatchLoop", "dispatch", "pinger", "keeper"}
	var missing []string
	for _, fn := range targets {
		re := regexp.MustCompile(`func \(c \*Client\) ` + fn + `\(`)
		loc := re.FindStringIndex(src)
		if loc == nil {
			continue
		}
		end := strings.Index(src[loc[0]:], "\n}\n")
		if end < 0 {
			continue
		}
		if !strings.Contains(src[loc[0]:loc[0]+end], "recover()") {
			missing = append(missing, fn)
		}
	}
	if len(missing) > 0 {
		t.Logf("재현됨: 복구 처리가 없는 고루틴 %d개 — %v", len(missing), missing)
	}

	if strings.Contains(src, "go func() { defer c.cbwg.Done(); c.opt.OnConnect(c) }()") &&
		!strings.Contains(src, "OnConnect(c) }(); recover") {
		t.Log("재현됨: 접속·단절 통지를 감싸는 복구 처리가 없다 — 통지 함수가 패닉하면 프로세스가 내려간다")
	}
}

// 수신 핸들러의 패닉이 그대로 올라오는 것을 실제로 확인한다.
// (테스트 프로세스가 죽지 않도록 여기서 붙잡아 관찰만 한다)
func TestRound5_HandlerPanicPropagates(t *testing.T) {
	c := New(Options{Addr: "127.0.0.1:1", ClientId: "srv-round5", RetryWait: time.Hour})
	defer c.Close()

	_ = c.Subscribe("CdnRpc/PubGwMsg/+", 1, func(string, []byte, *packets.Packet) {
		panic("수신 핸들러 패닉 재현")
	})

	propagated := func() (out bool) {
		defer func() {
			if r := recover(); r != nil {
				out = true
			}
		}()
		c.dispatch(&packets.Packet{TopicName: "CdnRpc/PubGwMsg/gwtest"})
		return false
	}()

	if propagated {
		t.Log("재현됨: 수신 핸들러의 패닉이 클라이언트를 그대로 통과한다 — " +
			"이 자리가 고루틴이라 감싸는 쪽이 없으면 프로세스가 내려간다")
	} else {
		t.Log("클라이언트가 패닉을 붙잡았다")
	}
}

// 발견 10: 단절 통지 안에서 Close를 부르면 스스로를 기다리며 멈춘다.
//
// Close는 진행 중인 통지가 끝나기를 기다리는데, 그 통지 안에서 Close를 부르면
// 자기 자신을 기다린다. 주석에는 적어 뒀지만 코드로 막지는 않는다 — 호출부가
// 단절 통지에서 정리하려다 걸릴 수 있는 자리다.
func TestRound5_CloseInsideDisconnectCallbackDeadlocks(t *testing.T) {
	var c *Client
	done := make(chan struct{})
	c = New(Options{
		Addr:      "127.0.0.1:1",
		ClientId:  "srv-round5b",
		RetryWait: time.Hour,
		OnDisconnect: func(_ *Client, _ error) {
			c.Close() // 호출부가 흔히 저지를 수 있는 정리 방식
			close(done)
		},
	})
	_ = c.Start()

	// 단절 통지를 유발한다.
	go c.Close()

	select {
	case <-done:
		t.Log("통지 안에서 Close를 불러도 빠져나왔다")
	case <-time.After(2 * time.Second):
		t.Log("재현됨: 단절 통지 안에서 Close를 부르면 스스로를 기다리며 멈춘다 — " +
			"주석 경고만 있고 코드로 막지 않는다")
	}
}
