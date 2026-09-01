/*
------------------- D-TEG APP TEAM  DCS -------------------
mochi-mqtt 포크 - z_readpacket_alloc_bench_test.go :

	ReadPacket의 패킷당 할당량을 잰다. 여기가 CDN 전체 할당의 1위였다
	(cdnjp 실측 15.85GB / 14.5%).

	읽기 버퍼를 한 번 더 복사하던 것을 걷어냈으므로, 패킷당 할당이 페이로드
	크기의 2배에서 1배로 줄어야 한다.

Taiminko 20260901
------------------------------------------------------------
*/
package mqtt

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
	"github.com/dtegapp/mochi-mqtt/v2/system"
)

// readPacketBenchConn: 같은 PUBLISH 패킷을 무한히 돌려주는 읽기 전용 연결.
type readPacketBenchConn struct {
	net.Conn
	data []byte
	pos  int
}

func (c *readPacketBenchConn) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if c.pos >= len(c.data) {
			c.pos = 0
		}
		m := copy(p[n:], c.data[c.pos:])
		c.pos += m
		n += m
	}
	return n, nil
}

// publishPacketBytes: QoS 0 PUBLISH 한 건을 바이트로 만든다(고정헤더 제외분).
func publishPacketBytes(topic string, payloadLen int) []byte {
	var b []byte
	b = append(b, byte(len(topic)>>8), byte(len(topic)))
	b = append(b, topic...)
	b = append(b, bytes.Repeat([]byte{0x41}, payloadLen)...)
	return b
}

func benchReadPacket(b *testing.B, payloadLen int) {
	b.Helper()
	body := publishPacketBytes("dcs/bench", payloadLen)

	cl := &Client{ops: &ops{info: new(system.Info), hooks: new(Hooks), log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	cl.Properties.ProtocolVersion = 4
	cl.Net.bconn = bufio.NewReaderSize(&readPacketBenchConn{data: body}, 4096)

	fh := &packets.FixedHeader{Type: packets.Publish, Remaining: len(body)}

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cl.ReadPacket(fh); err != nil {
			b.Fatalf("ReadPacket: %v", err)
		}
	}
}

// 서버간 통지(수백 바이트) — 가장 잦은 크기다.
func BenchmarkReadPacket_512B(b *testing.B) { benchReadPacket(b, 512) }

// 프론트엔드 CBOR 응답(수 KB).
func BenchmarkReadPacket_8KB(b *testing.B) { benchReadPacket(b, 8*1024) }

// 스냅샷·MDT 조각(수십 KB) — 복사 제거 효과가 가장 크게 드러나는 구간.
func BenchmarkReadPacket_64KB(b *testing.B) { benchReadPacket(b, 64*1024) }
