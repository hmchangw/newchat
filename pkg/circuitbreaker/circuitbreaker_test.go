package circuitbreaker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// testCooldown is short enough that a test can wait out a real cooldown.
//
// gobreaker measures its open->half-open deadline against time.Now() with no
// injection point, so the clock cannot be faked here as it could when this
// package owned the state machine. This is a wall-clock deadline, not goroutine
// synchronisation — there is no other goroutine to coordinate with and no sync
// primitive that can make time pass — so a sleep is the only way to cross it.
// The margin is deliberately 3x to keep it off a loaded CI runner's knife edge.
const testCooldown = 25 * time.Millisecond

func waitPastCooldown() { time.Sleep(3 * testCooldown) }

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
	b := New(3, time.Minute)
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
	b := New(1, testCooldown)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.Equal(t, StateOpen, b.State())
	// past the cooldown -> next Do is the half-open probe
	waitPastCooldown()
	require.NoError(t, b.Do(func() error { return nil }))
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_HalfOpenProbeFailureReopens(t *testing.T) {
	b := New(1, testCooldown)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	waitPastCooldown()
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State(), "failed probe must reopen")
	// still open immediately after (cooldown restarts)
	require.ErrorIs(t, b.Do(func() error { return nil }), ErrOpen)
}

func TestBreaker_OnTransitionFires(t *testing.T) {
	var transitions []string
	b := New(1, testCooldown,
		WithOnTransition(func(from, to State) {
			transitions = append(transitions, from.String()+"->"+to.String())
		}),
	)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom) // closed->open
	waitPastCooldown()
	require.NoError(t, b.Do(func() error { return nil })) // half-open probe -> closed
	assert.Equal(t, []string{"closed->open", "open->half-open", "half-open->closed"}, transitions)
}

func TestBreaker_OnTransitionFires_HalfOpenReopen(t *testing.T) {
	var transitions []string
	b := New(1, testCooldown,
		WithOnTransition(func(from, to State) {
			transitions = append(transitions, from.String()+"->"+to.String())
		}),
	)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom) // closed->open
	waitPastCooldown()
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom) // half-open probe fails -> open
	assert.Equal(t, []string{"closed->open", "open->half-open", "half-open->open"}, transitions)
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

// A call that entered while the breaker was closed can return long after other
// callers have tripped it open. Its success is evidence about a Mongo that was
// healthy *before* the outage, so it must not close the gate and let the herd
// back through — the breaker must stay open until a half-open probe says
// otherwise.
func TestBreaker_StaleSuccessDoesNotCloseOpenBreaker(t *testing.T) {
	b := New(2, time.Minute)

	release := make(chan struct{})
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.Do(func() error {
			close(entered)
			<-release
			return nil // succeeds, but only after the breaker has opened
		})
	}()
	<-entered

	// Meanwhile, two other callers fail and trip the breaker.
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.Equal(t, StateOpen, b.State())

	close(release)
	require.NoError(t, <-done)

	assert.Equal(t, StateOpen, b.State(),
		"a success that predates the failures must not reopen the gate")
	assert.ErrorIs(t, b.Do(func() error { return nil }), ErrOpen,
		"breaker must still be fast-failing")
}

// The probing flag belongs to the goroutine that set it. A pass-through call
// that entered before the outage and returns while a probe is in flight must
// not clear it — otherwise the next caller becomes a second concurrent probe
// and stampedes the very downstream the breaker is protecting.
func TestBreaker_StaleCallDoesNotClearAnotherProbe(t *testing.T) {
	b := New(1, testCooldown)

	// A slow caller enters while the breaker is still closed.
	staleRelease := make(chan struct{})
	staleEntered := make(chan struct{})
	staleDone := make(chan error, 1)
	go func() {
		staleDone <- b.Do(func() error {
			close(staleEntered)
			<-staleRelease
			return nil
		})
	}()
	<-staleEntered

	// The breaker trips, then its cooldown elapses.
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	waitPastCooldown()

	// A probe claims the single half-open slot and stays in flight.
	probeRelease := make(chan struct{})
	probeEntered := make(chan struct{})
	probeDone := make(chan error, 1)
	go func() {
		probeDone <- b.Do(func() error {
			close(probeEntered)
			<-probeRelease
			return nil
		})
	}()
	<-probeEntered

	// The stale caller now returns successfully, mid-probe.
	close(staleRelease)
	require.NoError(t, <-staleDone)

	// The probe still owns the slot, so the next caller must be turned away.
	require.ErrorIs(t, b.Do(func() error {
		t.Error("a second call ran while a probe was still in flight")
		return nil
	}), ErrOpen)

	close(probeRelease)
	require.NoError(t, <-probeDone)
	assert.Equal(t, StateClosed, b.State(), "a successful probe closes the breaker")
}

// A non-positive threshold means "never open" — the documented way to disable
// the breaker without deleting its wiring. It must not open on the first
// failure, which is what a naive failures >= threshold comparison does.
func TestBreaker_NonPositiveThresholdNeverOpens(t *testing.T) {
	for _, threshold := range []int{0, -1} {
		t.Run(fmt.Sprintf("threshold=%d", threshold), func(t *testing.T) {
			b := New(threshold, time.Minute)
			for range 5 {
				require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom,
					"the wrapped call must still run and its error propagate")
			}
			assert.Equal(t, StateClosed, b.State(), "a disabled breaker never opens")
		})
	}
}

// Some errors are healthy answers from a healthy downstream — "no such row" is
// the canonical one. Spending the failure budget on them lets a burst of
// requests for missing records open the breaker and degrade every other caller.
func TestBreaker_FailurePredicateExemptsHealthyErrors(t *testing.T) {
	notFound := errors.New("no documents in result")
	b := New(2, time.Minute, WithFailurePredicate(func(err error) bool {
		return !errors.Is(err, notFound)
	}))

	for range 5 {
		require.ErrorIs(t, b.Do(func() error { return notFound }),
			notFound, "the exempted error must still reach the caller")
	}
	assert.Equal(t, StateClosed, b.State(), "an exempted error must not spend the budget")

	// A real failure still counts, and an exempted one does not reset the count.
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return notFound }), notFound)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State(), "two real failures must still open it")
}

// Without the option every non-nil error counts, which is the default contract.
func TestBreaker_DefaultTreatsEveryErrorAsFailure(t *testing.T) {
	b := New(1, time.Minute)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State())
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

func TestDo_NilBreakerPassesThrough(t *testing.T) {
	var b *Breaker
	calls := 0
	for i := 0; i < 3; i++ {
		err := b.Do(func() error { calls++; return errors.New("boom") })
		require.Error(t, err)
	}
	assert.Equal(t, 3, calls, "a nil breaker must never fence a call")
}

func TestDo1_ReturnsValueAndZeroOnError(t *testing.T) {
	b := New(1, time.Minute)

	got, err := Do1(b, func() (string, error) { return "ok", nil })
	require.NoError(t, err)
	assert.Equal(t, "ok", got)

	// Trip it, then confirm an open breaker yields the zero value, not the last
	// successful one.
	_, err = Do1(b, func() (string, error) { return "stale", errors.New("boom") })
	require.Error(t, err)
	got, err = Do1(b, func() (string, error) { return "unreachable", nil })
	require.ErrorIs(t, err, ErrOpen)
	assert.Empty(t, got)
}

// FailureExcept is the shared form of the "a not-found is not a Mongo failure"
// predicate that five stores had each written by hand.
func TestFailureExcept(t *testing.T) {
	notFound := errors.New("not found")
	noSession := errors.New("no session")
	boom := errors.New("connection refused")

	tests := []struct {
		name      string
		sentinels []error
		err       error
		want      bool
	}{
		{"nil is never a failure", []error{notFound}, nil, false},
		{"nil is never a failure, no sentinels", nil, nil, false},
		{"an exempt sentinel is not a failure", []error{notFound}, notFound, false},
		{"a wrapped sentinel is still exempt", []error{notFound}, fmt.Errorf("find room: %w", notFound), false},
		{"any of several sentinels is exempt", []error{notFound, noSession}, noSession, false},
		{"an unlisted error is a failure", []error{notFound}, boom, true},
		{"with no sentinels every error is a failure", nil, boom, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FailureExcept(tt.sentinels...)(tt.err))
		})
	}
}

// The point of the exemption: a run of healthy not-founds must leave the
// breaker closed, while real infrastructure errors still trip it.
func TestFailureExcept_ExemptErrorsDoNotTripTheBreaker(t *testing.T) {
	notFound := errors.New("not found")
	b := New(2, time.Minute, WithFailurePredicate(FailureExcept(notFound)))

	for range 10 {
		require.ErrorIs(t, b.Do(func() error { return notFound }), notFound,
			"the sentinel must still reach the caller")
	}
	assert.Equal(t, StateClosed, b.State(), "healthy absences must not spend the budget")

	boom := errors.New("connection refused")
	for range 2 {
		require.ErrorIs(t, b.Do(func() error { return boom }), boom)
	}
	assert.Equal(t, StateOpen, b.State(), "real failures must still trip it")
}
