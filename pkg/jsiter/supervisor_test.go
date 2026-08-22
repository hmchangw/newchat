package jsiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubConsume is a running consumption handle that records its stops.
type stubConsume struct {
	mu    sync.Mutex
	stops int
}

func (c *stubConsume) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stops++
}

func (c *stubConsume) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stops
}

// consumeFactory hands out handles in order, captures each onError callback and
// signals every start so tests can wait instead of sleeping.
type consumeFactory struct {
	mu       sync.Mutex
	handles  []ConsumeContext
	errs     []error
	calls    int
	lastFail func(error)
	started  chan struct{}
}

func newConsumeFactory(handles []ConsumeContext, errs []error) *consumeFactory {
	return &consumeFactory{handles: handles, errs: errs, started: make(chan struct{}, 16)}
}

func (f *consumeFactory) start(_ context.Context, onError func(error)) (ConsumeContext, error) {
	f.mu.Lock()
	i := f.calls
	f.calls++
	f.lastFail = onError
	f.mu.Unlock()

	defer func() {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}()

	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.handles) && f.handles[i] != nil {
		return f.handles[i], nil
	}
	return &stubConsume{}, nil
}

func (f *consumeFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fail delivers err through the callback the latest start was given, the way
// jetstream.ConsumeErrHandler would.
func (f *consumeFactory) fail(err error) {
	f.mu.Lock()
	onError := f.lastFail
	f.mu.Unlock()
	onError(err)
}

// waitStarts blocks until the factory has been called n times since the last
// wait, so restart assertions never race the supervisor's goroutine.
func waitStarts(t *testing.T, f *consumeFactory, n int) {
	t.Helper()
	for range n {
		select {
		case <-f.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("consumption was not (re)started; calls so far: %d", f.callCount())
		}
	}
}

func newTestSupervisor(t *testing.T, f *consumeFactory) *Supervisor {
	t.Helper()
	s, err := Supervise(context.Background(), "ordered", f.start)
	require.NoError(t, err)
	waitStarts(t, f, 1)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	t.Cleanup(s.Stop)
	return s
}

func TestSupervise_FirstStartFailurePropagates(t *testing.T) {
	f := newConsumeFactory(nil, []error{errors.New("consumer not found")})

	s, err := Supervise(context.Background(), "ordered", f.start)

	require.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), "consumer not found")
}

// A missed heartbeat leaves consumption running — nats.go re-pulls on its own —
// so restarting on it would tear down a healthy subscription.
func TestSupervisor_TransientErrorDoesNotRestart(t *testing.T) {
	handle := &stubConsume{}
	f := newConsumeFactory([]ConsumeContext{handle}, nil)
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrNoHeartbeat)

	assert.Equal(t, 1, f.callCount())
	assert.Equal(t, 0, handle.stopCount())
	assert.True(t, s.IsUp())
}

// Consume stops itself on a deleted consumer without telling the caller: the
// handler simply never fires again. Restarting is the only way back.
func TestSupervisor_FatalErrorRestartsConsumption(t *testing.T) {
	dead, fresh := &stubConsume{}, &stubConsume{}
	f := newConsumeFactory([]ConsumeContext{dead, fresh}, nil)
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
	assert.Equal(t, 1, dead.stopCount(), "the dead handle must be released")
	assert.True(t, s.IsUp())
}

func TestSupervisor_RepeatedTransientErrorsRestartConsumption(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	for range transientEscalation {
		f.fail(jetstream.ErrNoHeartbeat)
	}
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
	assert.True(t, s.IsUp())
}

func TestSupervisor_RestartRetriesUntilItSucceeds(t *testing.T) {
	f := newConsumeFactory(nil, []error{nil, errors.New("down"), errors.New("down"), nil})
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 3)

	assert.Equal(t, 4, f.callCount())
	assert.True(t, s.IsUp())
}

func TestSupervisor_ConnectionClosedIsTerminal(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConnectionClosed)

	assert.Equal(t, 1, f.callCount(), "a closed connection must not be restarted against")
	assert.False(t, s.IsUp())
}

func TestSupervisor_StopIsIdempotentAndIgnoresLaterErrors(t *testing.T) {
	handle := &stubConsume{}
	f := newConsumeFactory([]ConsumeContext{handle}, nil)
	s, err := Supervise(context.Background(), "ordered", f.start)
	require.NoError(t, err)
	waitStarts(t, f, 1)

	s.Stop()
	s.Stop()
	f.fail(jetstream.ErrConsumerDeleted)

	assert.Equal(t, 1, handle.stopCount())
	assert.Equal(t, 1, f.callCount(), "a stopped supervisor must not restart")
	assert.False(t, s.IsUp())
}

// One restart at a time: a burst of errors from a single failure must not fan
// out into a pile of competing subscriptions.
func TestSupervisor_ConcurrentFatalErrorsRestartOnce(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)
	release := make(chan struct{})
	s.sleepFn = func(context.Context, time.Duration) bool {
		<-release
		return true
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.fail(jetstream.ErrConsumerDeleted)
		}()
	}
	wg.Wait()
	close(release)
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
}

func TestSupervisor_HealthCheck(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	check := s.HealthCheck()
	assert.Equal(t, "jetstream-consumer:ordered", check.Name)
	require.NoError(t, check.Probe(context.Background()))

	s.Stop()
	probeErr := check.Probe(context.Background())
	require.Error(t, probeErr)
	assert.Contains(t, probeErr.Error(), "ordered")
}

func TestSupervisor_Stop_UnblocksRestartBackoff(t *testing.T) {
	f := newConsumeFactory(nil, []error{nil, errors.New("down")})
	s, err := Supervise(context.Background(), "ordered", f.start)
	require.NoError(t, err)
	waitStarts(t, f, 1)

	// The real backoff runs here — Stop must cut it short rather than leave the
	// restart goroutine parked past shutdown.
	f.fail(jetstream.ErrConsumerDeleted)
	s.Stop()

	assert.False(t, s.IsUp())
	select {
	case <-f.started:
		t.Fatal("a stopped supervisor restarted consumption")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSupervisor_ContextCancellationEndsRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newConsumeFactory(nil, []error{nil, errors.New("down")})
	s, err := Supervise(ctx, "ordered", f.start)
	require.NoError(t, err)
	waitStarts(t, f, 1)
	t.Cleanup(s.Stop)

	cancel()
	f.fail(jetstream.ErrConsumerDeleted)

	select {
	case <-f.started:
		t.Fatal("a cancelled supervisor restarted consumption")
	case <-time.After(200 * time.Millisecond):
	}
	assert.False(t, s.IsUp())
}

// A status error can reach observe while start is still returning. Acting on a
// nil handle there would leave the just-created subscription running alongside
// its replacement — two consumers on a MaxAckPending=1 FIFO lane.
func TestSupervise_ErrorDuringStartDoesNotLeaveTwoSubscriptions(t *testing.T) {
	handle := &stubConsume{}
	var calls int
	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		calls++
		onError(jetstream.ErrConsumerDeleted)
		return handle, nil
	}

	s, err := Supervise(context.Background(), "ordered", start)

	require.Error(t, err, "a subscription that dies during start is a startup failure")
	assert.Nil(t, s)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, handle.stopCount(), "the doomed subscription must be released, not orphaned")
}

// An error still buffered on a superseded subscription's errs channel must not
// tear down the healthy replacement.
func TestSupervisor_StaleErrorFromSupersededRoundIsIgnored(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	f.mu.Lock()
	staleOnError := f.lastFail
	f.mu.Unlock()

	staleOnError(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)
	require.Equal(t, 2, f.callCount())

	// The first round is gone; its late error must be dropped.
	staleOnError(jetstream.ErrConsumerDeleted)

	select {
	case <-f.started:
		t.Fatal("a stale error from a superseded round restarted consumption")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Equal(t, 2, f.callCount())
	assert.True(t, s.IsUp())
}

// transientEscalation is meant to catch a back-to-back run, not a lifetime
// tally: unrelated heartbeat misses that nats.go already self-healed must not
// eventually tear down a healthy subscription and discard its buffered messages.
func TestSupervisor_TransientRunResetsAfterAQuietWindow(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)
	now := time.Unix(0, 0)
	s.nowFn = func() time.Time { return now }

	for range transientEscalation - 1 {
		f.fail(jetstream.ErrNoHeartbeat)
	}
	now = now.Add(transientWindow + time.Second)
	f.fail(jetstream.ErrNoHeartbeat)

	select {
	case <-f.started:
		t.Fatal("transient errors spread beyond the window escalated to a restart")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Equal(t, 1, f.callCount())
}

func TestSupervisor_TransientRunWithinTheWindowStillEscalates(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)
	now := time.Unix(0, 0)
	s.nowFn = func() time.Time { return now }

	for range transientEscalation {
		now = now.Add(time.Second)
		f.fail(jetstream.ErrNoHeartbeat)
	}
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
}

// Stop landing while begin is still starting the replacement must leave the
// supervisor reported down, not up on a subscription it just released.
func TestSupervisor_StopDuringRestartLeavesItDown(t *testing.T) {
	handles := []ConsumeContext{&stubConsume{}, &stubConsume{}}
	f := newConsumeFactory(handles, nil)

	var s *Supervisor
	var stopOnce sync.Once
	// Wired before Supervise so the closure is never reassigned under a reader.
	start := func(ctx context.Context, onError func(error)) (ConsumeContext, error) {
		cc, err := f.start(ctx, onError)
		if f.callCount() > 1 {
			// Stop lands after the restart's stopped check, while start runs.
			stopOnce.Do(s.Stop)
		}
		return cc, err
	}

	var err error
	s, err = Supervise(context.Background(), "ordered", start)
	require.NoError(t, err)
	waitStarts(t, f, 1)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }

	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)

	assert.Eventually(t, func() bool { return !s.IsUp() }, 2*time.Second, 10*time.Millisecond,
		"a supervisor stopped mid-restart must report down")
	assert.Equal(t, 1, handles[1].(*stubConsume).stopCount(),
		"the replacement started during Stop must be released")
}
