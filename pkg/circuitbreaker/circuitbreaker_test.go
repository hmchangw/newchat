package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

func TestBreaker_ClosedPassesThroughAndResetsOnSuccess(t *testing.T) {
	b := New(3, time.Second)
	assert.Equal(t, StateClosed, b.State())
	// two failures below threshold stay closed
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateClosed, b.State())
	// a success resets the failure count
	require.NoError(t, b.Do(func() error { return nil }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateClosed, b.State(), "success should have reset the counter")
}

func TestBreaker_OpensAfterThresholdAndFastFails(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(3, time.Minute, WithClock(func() time.Time { return now }))
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	}
	assert.Equal(t, StateOpen, b.State())
	// While open, fn is NOT invoked and ErrOpen is returned immediately.
	called := false
	err := b.Do(func() error { called = true; return nil })
	require.ErrorIs(t, err, ErrOpen)
	assert.False(t, called, "fn must not run while open")
}

func TestBreaker_HalfOpenProbeSuccessCloses(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(1, time.Minute, WithClock(func() time.Time { return now }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.Equal(t, StateOpen, b.State())
	// advance past cooldown -> next Do is the half-open probe
	now = now.Add(2 * time.Minute)
	require.NoError(t, b.Do(func() error { return nil }))
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_HalfOpenProbeFailureReopens(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(1, time.Minute, WithClock(func() time.Time { return now }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	now = now.Add(2 * time.Minute)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State(), "failed probe must reopen")
	// still open immediately after (cooldown restarts)
	require.ErrorIs(t, b.Do(func() error { return nil }), ErrOpen)
}

func TestBreaker_OnTransitionFires(t *testing.T) {
	now := time.Unix(0, 0)
	var transitions []string
	b := New(1, time.Minute,
		WithClock(func() time.Time { return now }),
		WithOnTransition(func(from, to State) {
			transitions = append(transitions, from.String()+"->"+to.String())
		}),
	)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom) // closed->open
	now = now.Add(2 * time.Minute)
	require.NoError(t, b.Do(func() error { return nil })) // half-open probe -> closed
	assert.Equal(t, []string{"closed->open", "open->half-open", "half-open->closed"}, transitions)
}

func TestBreaker_OnTransitionNilCallbackIsSafe(t *testing.T) {
	b := New(1, time.Minute)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State())
}

func TestBreaker_OnTransitionSkipsNoOpStateChecks(t *testing.T) {
	var transitions []string
	b := New(3, time.Minute, WithOnTransition(func(from, to State) {
		transitions = append(transitions, from.String()+"->"+to.String())
	}))
	// Below threshold: state stays closed, no transition should fire.
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Empty(t, transitions)
	// A State() call that doesn't advance open->half-open must not fire either.
	assert.Equal(t, StateClosed, b.State())
	assert.Empty(t, transitions)
}

func TestState_String(t *testing.T) {
	tests := []struct {
		name string
		s    State
		want string
	}{
		{"closed", StateClosed, "closed"},
		{"open", StateOpen, "open"},
		{"half-open", StateHalfOpen, "half-open"},
		{"unknown", State(99), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.s.String())
		})
	}
}
