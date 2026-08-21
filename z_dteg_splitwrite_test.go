/*
SplitPublishWrite(D-TEG 20260821) 검증 — 켜고 끈 결과가 회선 바이트로 동일한지,
그리고 구독자가 여럿일 때 페이로드 복사가 실제로 줄어드는지 확인한다.
*/
package mqtt

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
	"github.com/dtegapp/mochi-mqtt/v2/system"
)

func newSplitTestClient(split bool) (*Client, net.Conn) {
	r, w := net.Pipe()
	cl := newClient(w, &ops{
		info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{
			SplitPublishWrite: split,
			Capabilities: &Capabilities{
				ReceiveMaximum: 10, MaximumInflight: 5, TopicAliasMaximum: 10000,
				MaximumClientWritesPending: 3, maximumPacketID: 10,
			},
		},
	})
	cl.ID = "t"
	cl.State.Inflight.maximumSendQuota = 5
	cl.State.Inflight.sendQuota = 5
	go cl.WriteLoop()
	return cl, r
}

func writeOnce(t *testing.T, split bool, payload []byte) []byte {
	t.Helper()
	cl, r := newSplitTestClient(split)
	cl.Properties.ProtocolVersion = 5
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
		ProtocolVersion: 5, TopicName: "cdn/pubcbor/12345", Payload: payload,
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = cl.WritePacket(pk)
		time.Sleep(80 * time.Millisecond)
		cl.Net.Conn.Close()
	}()
	out, _ := io.ReadAll(r)
	return out
}

// 1) 켜든 끄든 회선 바이트가 완전히 같아야 한다
func TestSplitPublishWrite_ByteIdentical(t *testing.T) {
	for _, n := range []int{0, 1, 1024, 256 * 1024} {
		payload := make([]byte, n)
		for i := range payload {
			payload[i] = byte(i * 31)
		}
		off := writeOnce(t, false, payload)
		on := writeOnce(t, true, payload)
		if !bytes.Equal(off, on) {
			t.Fatalf("payload %d bytes: 회선 바이트 불일치 (off %d / on %d)", n, len(off), len(on))
		}
		t.Logf("payload %7d bytes → 회선 %7d bytes, 켜고 끈 결과 완전 일치", n, len(off))
	}
}

// 2) 페이로드 크기만큼의 추가 할당이 사라지는지
func BenchmarkSplitPublishWrite(b *testing.B) {
	payload := make([]byte, 512*1024)
	run := func(b *testing.B, split bool) {
		cl, r := newSplitTestClient(split)
		cl.Properties.ProtocolVersion = 5
		go func() { _, _ = io.Copy(io.Discard, r) }()
		pk := packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
			ProtocolVersion: 5, TopicName: "cdn/pubcbor/12345", Payload: payload,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cl.WritePacket(pk); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		cl.Net.Conn.Close()
	}
	b.Run("512KB/off_상류", func(b *testing.B) { run(b, false) })
	b.Run("512KB/on_분리", func(b *testing.B) { run(b, true) })
}
