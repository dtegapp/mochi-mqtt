/*
SysTopicsDisabled(D-TEG 20260824) 검증 — $SYS 발행을 끄면 실제로 발행이 일어나지 않고,
retained 저장소가 비어 있어 만료 청소기의 맵 복사 대상도 사라지는지 확인한다.

이 옵션을 넣은 이유는 접속 클라이언트가 0명인 서버에서도 eventLoop가 매초 $SYS 20개를
발행하고 그 값들이 retained로 남아 매초 복사되는 구조 때문이다(CDN 실측 2.42GB/일).
*/
package mqtt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSysTopicsDisabled_NoRetainedNoPublish:
// 끈 상태로 기동하면 $SYS가 한 건도 발행되지 않고 retained가 비어 있어야 한다.
func TestSysTopicsDisabled_NoRetainedNoPublish(t *testing.T) {
	s := New(&Options{SysTopicResendInterval: SysTopicsDisabled})
	defer s.Close()

	require.False(t, s.sysTopicsEnabled())
	require.NoError(t, s.Serve())

	// 티커가 멈춰 있으므로 아무리 기다려도 발행되지 않는다.
	time.Sleep(time.Millisecond * 30)
	require.Equal(t, 0, len(s.Topics.Retained.GetAll()),
		"$SYS를 끄면 retained가 비어 있어야 한다 — 비어야 만료 청소기의 복사 비용도 사라진다")
}

// TestSysTopicsEnabledByDefault:
// 옵션을 주지 않으면 상류와 동일하게 1초 주기로 켜져 있어야 한다(기본 동작 불변).
func TestSysTopicsEnabledByDefault(t *testing.T) {
	s := New(&Options{})
	defer s.Close()

	require.True(t, s.sysTopicsEnabled())
	require.Equal(t, defaultSysTopicInterval, s.Options.SysTopicResendInterval)
	require.NoError(t, s.Serve())

	// Serve()가 최초 1회 발행하므로 retained에 $SYS가 들어와 있어야 한다.
	require.NotEmpty(t, s.Topics.Retained.GetAll(),
		"기본값에서는 상류와 동일하게 $SYS가 retained로 남아야 한다")
}

// TestSysTopicsDisabled_EventLoopSafe:
// 꺼진 티커를 물고도 eventLoop와 Close()가 패닉 없이 동작해야 한다.
// (nil 티커를 쓰지 않고 Stop된 티커를 쓰는 이유가 이것 — select가 .C를 무조건 참조한다.)
func TestSysTopicsDisabled_EventLoopSafe(t *testing.T) {
	s := New(&Options{SysTopicResendInterval: SysTopicsDisabled})

	s.loop.clientExpiry = time.NewTicker(time.Millisecond)
	s.loop.retainedExpiry = time.NewTicker(time.Millisecond)
	s.loop.willDelaySend = time.NewTicker(time.Millisecond)
	s.loop.inflightExpiry = time.NewTicker(time.Millisecond)
	go s.eventLoop()

	time.Sleep(time.Millisecond * 5)
	require.NoError(t, s.Close()) // done 경로에서 Stop() 재호출 — 이미 멈춘 티커라도 안전
}
