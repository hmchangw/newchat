package jsiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	p, err := New(context.Background(), "test", f.new)
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

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Disposition
	}{
		{"nil is transient", nil, Transient},
		{"missed heartbeat is recoverable", jetstream.ErrNoHeartbeat, Transient},
		{"wrapped missed heartbeat is recoverable", fmt.Errorf("pump: %w", jetstream.ErrNoHeartbeat), Transient},
		{"next timeout is recoverable", nats.ErrTimeout, Transient},
		{"consumer deleted needs a rebuild", jetstream.ErrConsumerDeleted, Fatal},
		{"consumer not found needs a rebuild", jetstream.ErrConsumerNotFound, Fatal},
		{"bad request needs a rebuild", jetstream.ErrBadRequest, Fatal},
		{"leadership change needs a rebuild", jetstream.ErrConsumerLeadershipChanged, Fatal},
		{"bare iterator closure needs a rebuild", jetstream.ErrMsgIteratorClosed, Fatal},
		{"unknown error needs a rebuild", errors.New("boom"), Fatal},
		{"jetstream connection closed ends consumption", jetstream.ErrConnectionClosed, Stopped},
		{"core connection closed ends consumption", nats.ErrConnectionClosed, Stopped},
		{"closed iterator on a closed connection ends consumption",
			fmt.Errorf("%w: %w", jetstream.ErrMsgIteratorClosed, jetstream.ErrConnectionClosed), Stopped},
		{"cancelled context ends consumption", context.Canceled, Stopped},
		{"deadline exceeded ends consumption", context.DeadlineExceeded, Stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classify(tt.err))
		})
	}
}

func TestDisposition_String(t *testing.T) {
	assert.Equal(t, "stopped", Stopped.String())
	assert.Equal(t, "transient", Transient.String())
	assert.Equal(t, "fatal", Fatal.String())
	assert.Equal(t, "unknown", Disposition(99).String())
}

func TestNew_FirstBuildFailurePropagates(t *testing.T) {
	f := &factory{errs: []error{errors.New("stream not found")}}

	p, err := New(context.Background(), "test", f.new)

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
	steps := make([]step, transientEscalation)
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
	steps := make([]step, 0, 2*transientEscalation)
	for i := 0; i < transientEscalation-1; i++ {
		steps = append(steps, step{err: jetstream.ErrNoHeartbeat})
	}
	steps = append(steps, step{msg: first})
	for i := 0; i < transientEscalation-1; i++ {
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

func TestPump_Next_RebuildBackoffReusesFinalStep(t *testing.T) {
	attempts := len(RebuildBackoff) + 2
	iters := make([]Iterator, attempts+2)
	errs := make([]error, attempts+2)
	iters[0] = newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	for i := 1; i <= attempts; i++ {
		errs[i] = errors.New("no responders")
	}
	msg := &stubMsg{}
	iters[attempts+1] = newScriptedIter(step{msg: msg})
	f := &factory{iters: iters, errs: errs}
	p, delays := newTestPump(t, f)

	_, got, err := p.Next()

	require.NoError(t, err)
	assert.Same(t, msg, got)
	require.Len(t, *delays, attempts+1)
	last := RebuildBackoff[len(RebuildBackoff)-1]
	assert.Equal(t, last, (*delays)[len(*delays)-1])
}

func TestPump_IsUp_FalseWhileRebuilding(t *testing.T) {
	dead := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	f := &factory{iters: []Iterator{dead, nil}, errs: []error{nil, errors.New("down")}}
	p, err := New(context.Background(), "test", f.new)
	require.NoError(t, err)
	t.Cleanup(p.Stop)
	assert.True(t, p.IsUp(), "a freshly built pump is up")

	observed := make(chan bool, 1)
	p.sleepFn = func(context.Context, time.Duration) bool {
		select {
		case observed <- p.IsUp():
		default:
		}
		p.Stop()
		return false
	}

	_, _, err = p.Next()

	require.ErrorIs(t, err, ErrStopped)
	assert.False(t, <-observed, "the pump reports down while its iterator is being rebuilt")
	assert.False(t, p.IsUp())
}

func TestPump_Stop_UnblocksRebuildBackoff(t *testing.T) {
	dead := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	f := &factory{iters: []Iterator{dead, nil}, errs: []error{nil, errors.New("down")}}
	p, err := New(context.Background(), "test", f.new)
	require.NoError(t, err)

	go func() {
		// Stop lands while Next is parked on the real rebuild backoff.
		time.Sleep(20 * time.Millisecond)
		p.Stop()
	}()

	done := make(chan error, 1)
	go func() {
		_, _, nextErr := p.Next()
		done <- nextErr
	}()

	select {
	case nextErr := <-done:
		assert.ErrorIs(t, nextErr, ErrStopped)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not interrupt the rebuild backoff")
	}
}

func TestPump_Stop_IsIdempotentAndStopsIterator(t *testing.T) {
	iter := newScriptedIter()
	f := &factory{iters: []Iterator{iter}}
	p, err := New(context.Background(), "test", f.new)
	require.NoError(t, err)

	p.Stop()
	p.Stop()

	assert.Equal(t, 1, iter.stopCount())
	_, _, err = p.Next()
	assert.ErrorIs(t, err, ErrStopped)
	assert.False(t, p.IsUp())
}

func TestPump_Next_StopDuringNextReturnsErrStopped(t *testing.T) {
	iter := newScriptedIter()
	f := &factory{iters: []Iterator{iter}}
	p, err := New(context.Background(), "test", f.new)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, _, nextErr := p.Next()
		done <- nextErr
	}()
	time.Sleep(20 * time.Millisecond)
	p.Stop()

	select {
	case nextErr := <-done:
		assert.ErrorIs(t, nextErr, ErrStopped)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not unblock a parked Next")
	}
}

func TestPump_Next_ContextCancellationEndsConsumption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	iter := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	f := &factory{iters: []Iterator{iter, nil}, errs: []error{nil, errors.New("down")}}
	p, err := New(ctx, "test", f.new)
	require.NoError(t, err)
	t.Cleanup(p.Stop)

	cancel()
	_, _, err = p.Next()

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPump_HealthCheck(t *testing.T) {
	iter := newScriptedIter()
	f := &factory{iters: []Iterator{iter}}
	p, err := New(context.Background(), "inbox", f.new)
	require.NoError(t, err)

	check := p.HealthCheck()
	assert.Equal(t, "jetstream-consumer:inbox", check.Name)
	require.NoError(t, check.Probe(context.Background()))

	p.Stop()
	probeErr := check.Probe(context.Background())
	require.Error(t, probeErr)
	assert.Contains(t, probeErr.Error(), "inbox")
}

func TestJitter(t *testing.T) {
	assert.Zero(t, jitter(0))
	assert.Zero(t, jitter(-time.Second))
	for range 50 {
		d := jitter(time.Second)
		assert.GreaterOrEqual(t, d, 500*time.Millisecond)
		assert.LessOrEqual(t, d, time.Second)
	}
}

func TestBackoffStep_ClampsPastTheSchedule(t *testing.T) {
	assert.Equal(t, RebuildBackoff[0], backoffStep(0))
	last := RebuildBackoff[len(RebuildBackoff)-1]
	assert.Equal(t, last, backoffStep(len(RebuildBackoff)))
	assert.Equal(t, last, backoffStep(1000))
}

// A Stop landing while newIter is in flight must not leave the freshly built
// iterator running: nothing would ever stop it, and its pull subscription keeps
// taking messages nobody reads until AckWait expires them.
func TestPump_Next_StopDuringRebuildStopsTheNewIterator(t *testing.T) {
	dead := newScriptedIter(step{err: jetstream.ErrConsumerDeleted})
	fresh := newScriptedIter(step{msg: &stubMsg{}})
	var p *Pump
	f := &factory{}
	f.iters = []Iterator{dead, fresh}
	built := make(chan struct{})

	var err error
	p, err = New(context.Background(), "test", func(ctx context.Context) (Iterator, error) {
		it, buildErr := f.new(ctx)
		if f.callCount() == 2 {
			// Stop lands after rebuild's stopped check, while the build runs.
			p.Stop()
			close(built)
		}
		return it, buildErr
	})
	require.NoError(t, err)
	p.sleepFn = func(context.Context, time.Duration) bool { return true }

	_, _, nextErr := p.Next()

	<-built
	require.ErrorIs(t, nextErr, ErrStopped)
	assert.Equal(t, 1, fresh.stopCount(), "the iterator built during Stop must be released")
	assert.False(t, p.IsUp())
}
