/*
SharePublishPayload(D-TEG 20260825) 검증 — 페이로드를 공유해도 구독자가 받는 바이트가
동일한지, 그리고 실제로 복제가 사라졌는지 확인한다.

publishToClient은 구독자마다 pk.Copy로 페이로드를 통째로 복제하는데 out.Payload를 수정하는
코드가 없어(쓰기·인플라이트 보관 모두 읽기 전용) 그 복제가 낭비다. 다만 발행측이 버퍼를
재사용하면 다른 구독자에게 깨진 데이터가 나가므로 기본값은 상류와 같은 복제로 둔다.
*/
package mqtt

import (
	"bytes"
	"testing"
	"time"

	"github.com/dtegapp/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

func makePk(payload []byte) packets.Packet {
	return packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:       "cdn/pubcbor/1",
		Payload:         payload,
		ProtocolVersion: 5,
	}
}

// 1) 공유본과 복제본의 내용이 같아야 한다 — 회선 바이트가 달라지면 안 된다.
func TestCopySharePayload_SameContent(t *testing.T) {
	payload := []byte("DCS-PAYLOAD-0123456789")
	pk := makePk(payload)
	copied := pk.Copy(false)
	shared := pk.CopySharePayload(false)
	require.True(t, bytes.Equal(copied.Payload, shared.Payload))
	require.Equal(t, copied.TopicName, shared.TopicName)
	require.Equal(t, copied.FixedHeader.Qos, shared.FixedHeader.Qos)
}

//  2. 공유본은 원본과 같은 배열을 가리키고, 복제본은 아니어야 한다.
//     (이 성질이 곧 "복사가 사라졌다"의 증거다)
func TestCopySharePayload_AliasesOriginal(t *testing.T) {
	payload := []byte("SHARED")
	pk := makePk(payload)

	shared := pk.CopySharePayload(false)
	shared.Payload[0] = 'X'
	require.Equal(t, byte('X'), payload[0], "공유본은 원본과 같은 배열이어야 한다")

	payload[0] = 'S'
	copied := pk.Copy(false)
	copied.Payload[0] = 'Y'
	require.Equal(t, byte('S'), payload[0], "복제본은 원본과 분리돼야 한다(상류 동작)")
}

// 3) 빈 페이로드에서도 양쪽이 같아야 한다.
func TestCopySharePayload_EmptyPayload(t *testing.T) {
	pk := makePk(nil)
	require.Nil(t, pk.Copy(false).Payload)
	require.Nil(t, pk.CopySharePayload(false).Payload)
}

// 4) 기본값은 상류와 같은 복제여야 한다 — 옵션을 주지 않은 배치의 동작 불변.
func TestSharePublishPayload_DefaultOff(t *testing.T) {
	s := New(&Options{})
	require.False(t, s.Options.SharePublishPayload)
	require.NoError(t, s.Close())
}

// 5) 구독자에게 실제로 전달되는 바이트가 옵션 유무와 무관하게 같아야 한다.
func TestSharePublishPayload_DeliveredBytesIdentical(t *testing.T) {
	payload := []byte("LIVELIST-CBOR-BYTES-0123456789")
	got := map[bool][]byte{}
	for _, share := range []bool{false, true} {
		s := New(&Options{InlineClient: true, SharePublishPayload: share, SysTopicResendInterval: SysTopicsDisabled})
		done := make(chan []byte, 1)
		require.NoError(t, s.Subscribe("cdn/pubcbor/1", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
			done <- append([]byte{}, pk.Payload...)
		}))
		require.NoError(t, s.Publish("cdn/pubcbor/1", payload, false, 0))
		got[share] = <-done
		require.NoError(t, s.Close())
	}
	require.True(t, bytes.Equal(got[false], got[true]), "옵션 유무로 전달 바이트가 달라지면 안 된다")
	require.True(t, bytes.Equal(got[true], payload))
}

// 6) 🔴 구독자가 여럿일 때 — 전원이 같은 배열을 가리키게 되는데 실제로 안전한가.
//
// publishToClient가 구독자마다 CopySharePayload를 부르므로 N명이 같은 페이로드 배열을
// 공유한다. 안전 근거는 두 가지다. ①out.Payload를 수정하는 코드가 없다(WritePacket은
// net.Buffers/outbuf로 읽기만 한다) ②publishToSubscribers는 구독자를 순차 순회하고
// WritePacket은 cl.Lock() 아래 동기 쓰기다. 그래도 근거만으로 두지 않고 실제로 3명에게
// 보내 전원이 같은 바이트를 받는지, 옵션을 끈 경우와 동일한지 확인한다.
func TestSharePublishPayload_MultipleSubscribers(t *testing.T) {
	recvFor := func(share bool) [][]byte {
		// newServer()를 쓰는 이유: AllowHook이 붙어 있어야 OnACLCheck를 통과해 실제로
		// 전달된다. 옵션은 생성 후 직접 켠다.
		s := newServer()
		s.Options.SharePublishPayload = share
		defer func() { _ = s.Close() }()

		cl1, r1, w1 := newTestClient()
		cl1.ID = "sub1"
		cl2, r2, w2 := newTestClient()
		cl2.ID = "sub2"
		cl3, r3, w3 := newTestClient()
		cl3.ID = "sub3"
		s.Clients.Add(cl1)
		s.Clients.Add(cl2)
		s.Clients.Add(cl3)
		for _, id := range []string{"sub1", "sub2", "sub3"} {
			require.True(t, s.Topics.Subscribe(id, packets.Subscription{Filter: "cdn/pubnoti/SITEA"}))
		}

		out := make([]chan []byte, 3)
		for i, r := range []interface{ Read([]byte) (int, error) }{r1, r2, r3} {
			ch := make(chan []byte, 1)
			out[i] = ch
			go func(rd interface{ Read([]byte) (int, error) }, c chan []byte) {
				buf, _ := readAllFrom(rd)
				c <- buf
			}(r, ch)
		}

		payload := bytes.Repeat([]byte("SITE-BROADCAST-"), 64) // 960 bytes
		go func() {
			s.publishToSubscribers(packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
				TopicName:   "cdn/pubnoti/SITEA",
				Payload:     payload,
			})
			time.Sleep(20 * time.Millisecond)
			_ = w1.Close()
			_ = w2.Close()
			_ = w3.Close()
		}()

		got := make([][]byte, 3)
		for i := range out {
			got[i] = <-out[i]
		}
		return got
	}

	off := recvFor(false)
	on := recvFor(true)

	// 세 구독자가 서로 같은 바이트를 받아야 한다(공유로 섞이거나 깨지면 여기서 걸린다).
	require.True(t, bytes.Equal(on[0], on[1]), "구독자1·2가 받은 바이트가 다름")
	require.True(t, bytes.Equal(on[1], on[2]), "구독자2·3이 받은 바이트가 다름")
	require.NotEmpty(t, on[0])

	// 옵션을 끈 경우(상류 동작)와도 같아야 한다.
	for i := range off {
		require.True(t, bytes.Equal(off[i], on[i]), "구독자%d: 옵션 유무로 결과가 달라짐", i+1)
	}
}

func readAllFrom(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, nil
		}
	}
}
