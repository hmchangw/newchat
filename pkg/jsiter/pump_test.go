package jsiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubMsg satisfies jetstream.Msg for identity assertions only; the tests never
// call through to the embedded nil interface.
type stubMsg struct{ jetstream.Msg }

// step is one scripted Next outcome.
type step struct {
	msg jetstream.Msg
	err error
}

// scriptedIter replays steps in order and reports how often it was stopped.
// Once the script is exhausted it blocks until Stop, standing in for an
// iterator parked on a quiet stream.
type scriptedIter struct {
	mu      sync.Mutex
	steps   []step
	i       int
	stops   int
	release chan struct{}
}

func newScriptedIter(steps ...step) *scriptedIter {
	return &scriptedIter{steps: steps, release: make(chan struct{})}
}

func (s *scriptedIter) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	s.mu.Lock()
	if s.i >= len(s.steps) {
		s.mu.Unlock()
		<-s.release
		return nil, nil, jetstream.ErrMsgIteratorClosed
	}
	st := s.steps[s.i]
	s.i++
	s.mu.Unlock()
	return context.Background(), st.msg, st.err
}

func (s *scriptedIter) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops++
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

func (s *scriptedIter) stopCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stops
}

// factory hands out pre-built iterators in order and records every call.
type factory struct {
	mu    sync.Mutex
	iters []Iterator
	errs  []error
	calls int
}

func (f *factory) new(context.Context) (Iterator, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i >= len(f.iters) {
		return nil, fmt.Errorf("factory exhausted after %d calls", i)
	}
	return f.iters[i], nil
}

func (f *factory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestPump builds a pump whose backoff never really sleeps, so rebuild paths
// run at test speed. Recorded delays let a test assert the schedule anyway.
func newTestPump(t *testing.T, f *factory) (*Pump, *[]time.Duration) {
	t.Helper()
	var mu sync.Mutex
	delays := make([]time.Duration, 0, 8)
	p, err := NewPump(context.Background(), "test", f.new)
	require.NoError(t, err)
	p.sleepFn = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return true
	}
	t.Cleanup(p.Stop)
	return p, &delays
}

func TestNewPump_FirstOpenFailurePropagates(t *testing.T) {
	f := &factory{errs: []error{errors.New("stream not found")}}

	p, err := NewPump(context.Background(), "test", f.new)

	require.Error(t, err)
	assert.Nil(t, p)
	assert.Contains(t, err.Error(), "stream not found")
}

// A missed idle heartbeat is the error nats.go has already re-pulled for. The
// pump must keep calling Next on the same iterator rather than surface it.
func TestPump_Next_RetriesTransientErrorWithoutRebuilding(t *testing.T) {
	msg := &stubMsg{}
	iter := newScriptedIter(
		step{err: jetstream.ErrNoHeartbeat},
		step{msg: msg},
	)
	f := &factory{iters: []Iterator{iter}}
	p, delays := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	assert.Equal(t, 1, f.callCount(), "transient errors must not rebuild the iterator")
	assert.Equal(t, 0, iter.stopCount())
	assert.Empty(t, *delays, "transient retries must not sleep")
	assert.True(t, p.IsUp())
}

func TestPump_Next_RebuildsOnFatalError(t *testing.T) {
	msg := &stubMsg{}
	dead := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	fresh := newScriptedIter(step{msg: msg})
	f := &factory{iters: []Iterator{dead, fresh}}
	p, _ := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	assert.Equal(t, 2, f.callCount())
	assert.Equal(t, 1, dead.stopCount(), "the dead iterator must be released")
	assert.True(t, p.IsUp())
}

// A stall that never produces a message only ever shows up as repeated
// heartbeat misses, so consecutive transient errors must escalate to a rebuild.
func TestPump_Next_EscalatesRepeatedTransientErrorsToRebuild(t *testing.T) {
	msg := &stubMsg{}
	steps := make([]step, TransientEscalation)
	for i := range steps {
		steps[i] = step{err: jetstream.ErrNoHeartbeat}
	}
	stalled := newScriptedIter(steps...)
	fresh := newScriptedIter(step{msg: msg})
	f := &factory{iters: []Iterator{stalled, fresh}}
	p, _ := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	assert.Equal(t, 2, f.callCount())
	assert.Equal(t, 1, stalled.stopCount())
}

func TestPump_Next_DeliveredMessageResetsTransientRun(t *testing.T) {
	first, second := &stubMsg{}, &stubMsg{}
	steps := make([]step, 0, 2*TransientEscalation)
	for i := 0; i < TransientEscalation-1; i++ {
		steps = append(steps, step{err: jetstream.ErrNoHeartbeat})
	}
	steps = append(steps, step{msg: first})
	for i := 0; i < TransientEscalation-1; i++ {
		steps = append(steps, step{err: jetstream.ErrNoHeartbeat})
	}
	steps = append(steps, step{msg: second})
	iter := newScriptedIter(steps...)
	f := &factory{iters: []Iterator{iter}}
	p, _ := newTestPump(t, f)

	_, got1, err := p.Next()
	require.NoError(t, err)
	assert.Same(t, first, got1)

	_, got2, err := p.Next()
	require.NoError(t, err)
	assert.Same(t, second, got2)
	assert.Equal(t, 1, f.callCount(), "a delivered message must reset the transient run")
}

func TestPump_Next_ConnectionClosedIsTerminal(t *testing.T) {
	iter := newScriptedIter(step{err: jetstream.ErrConnectionClosed})
	f := &factory{iters: []Iterator{iter}}
	p, _ := newTestPump(t, f)

	_, _, err := p.Next()

	require.Error(t, err)
	assert.ErrorIs(t, err, jetstream.ErrConnectionClosed)
	assert.Equal(t, 1, f.callCount(), "a closed connection must not be rebuilt against")
	assert.False(t, p.IsUp())
}

// A rebuild retries until it succeeds: a peer site that is down comes back, and
// exiting the loop would reproduce the stall this package exists to prevent.
func TestPump_Next_RebuildRetriesOnBackoffUntilItSucceeds(t *testing.T) {
	msg := &stubMsg{}
	dead := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	fresh := newScriptedIter(step{msg: msg})
	f := &factory{
		iters: []Iterator{dead, nil, nil, fresh},
		errs:  []error{nil, errors.New("no responders"), errors.New("no responders"), nil},
	}
	p, delays := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	assert.Equal(t, 4, f.callCount())
	require.Len(t, *delays, 3, "one backoff sleep per rebuild attempt")
	assert.Equal(t, RebuildBackoff[0], (*delays)[0])
	assert.Equal(t, RebuildBackoff[1], (*delays)[1])
	assert.Equal(t, RebuildBackoff[2], (*delays)[2])
}

func TestPump_Next_BackoffAdvancesAcrossFailingReplacements(t *testing.T) {
	msg := &stubMsg{}
	iters := []Iterator{
		newScriptedIter(step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{msg: msg}),
	}
	f := &factory{iters: iters}
	p, delays := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	require.Len(t, *delays, 3, "one backoff per rebuild")
	assert.Equal(t, RebuildBackoff[0], (*delays)[0])
	assert.Equal(t, RebuildBackoff[1], (*delays)[1], "a second failing replacement must back off further")
	assert.Equal(t, RebuildBackoff[2], (*delays)[2])
}

// One delivery is not proof of health: during a flap each replacement can hand
// over a single queued message and die. Resetting on it pins every later
// rebuild at the first step, which is the storm the backoff exists to avoid.
func TestPump_Next_OneDeliveryDoesNotResetRebuildBackoff(t *testing.T) {
	first, second := &stubMsg{}, &stubMsg{}
	f := &factory{iters: []Iterator{
		newScriptedIter(step{err: jetstream.ErrConsumerDeleted}),
		// Delivers once, then dies.
		newScriptedIter(step{msg: first}, step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{msg: second}),
	}}
	p, delays := newTestPump(t, f)

	_, got, err := p.Next()
	require.NoError(t, err)
	require.Same(t, first, got)
	require.Len(t, *delays, 1)
	assert.Equal(t, RebuildBackoff[0], (*delays)[0])

	_, got, err = p.Next()
	require.NoError(t, err)
	assert.Same(t, second, got)
	require.Len(t, *delays, 2)
	assert.Equal(t, RebuildBackoff[1], (*delays)[1],
		"a replacement that delivered once and died must keep backing off")
}

// Rebuilds far apart are unrelated failures, not a flap, so the schedule starts
// over — otherwise a long-lived pod would creep to the 30s cap and stay there.
func TestPump_RebuildBackoffResetsAfterAQuietWindow(t *testing.T) {
	first, second := &stubMsg{}, &stubMsg{}
	f := &factory{iters: []Iterator{
		newScriptedIter(step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{msg: first}, step{err: jetstream.ErrConsumerDeleted}),
		newScriptedIter(step{msg: second}),
	}}
	p, delays := newTestPump(t, f)
	now := time.Now()
	p.nowFn = func() time.Time { return now }

	_, _, err := p.Next()
	require.NoError(t, err)

	now = now.Add(EscalationWindow + time.Second)
	_, _, err = p.Next()
	require.NoError(t, err)

	require.Len(t, *delays, 2)
	assert.Equal(t, RebuildBackoff[0], (*delays)[1], "one long after the last starts over")
}

// Stop must unblock a Next parked on the real rebuild backoff, or shutdown
// hangs for a whole backoff step. newTestPump stubs sleepFn out, so nothing
// else in this package exercises SleepUntil's done channel for a Pump.
func TestPump_Stop_UnblocksRebuildBackoff(t *testing.T) {
	// A long step is what gives the assertion teeth: with the default 100ms,
	// a Stop that merely waited the sleep out would still return in time and
	// the test would pass while proving nothing. Tests here are sequential.
	restore := RebuildBackoff
	RebuildBackoff = []time.Duration{30 * time.Second}
	t.Cleanup(func() { RebuildBackoff = restore })

	f := &factory{iters: []Iterator{newScriptedIter(step{err: jetstream.ErrConsumerDeleted})}}
	p, err := NewPump(context.Background(), "test", f.new)
	require.NoError(t, err)

	// Keep the real SleepUntil; only add a signal for when Next reaches it, so
	// Stop cannot land before the rebuild is genuinely parked.
	parked := make(chan struct{})
	realSleep := p.sleepFn
	p.sleepFn = func(ctx context.Context, d time.Duration) bool {
		close(parked)
		return realSleep(ctx, d)
	}

	done := make(chan error, 1)
	go func() {
		_, _, nextErr := p.Next()
		done <- nextErr
	}()

	<-parked
	p.Stop()

	select {
	case nextErr := <-done:
		assert.ErrorIs(t, nextErr, ErrStopped)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not unblock a Next parked on rebuild backoff")
	}
}
