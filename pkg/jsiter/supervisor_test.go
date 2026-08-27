package jsiter

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubConsume is a running consumption handle that records its stops. Closed
// reports when its work is finished, the way nats.go's does; hold keeps it open
// past Stop so a test can model a handler that is still running.
type stubConsume struct {
	mu     sync.Mutex
	stops  int
	hold   bool
	closed chan struct{}
}

func (c *stubConsume) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stops++
	if !c.hold {
		c.markClosed()
	}
}

func (c *stubConsume) Closed() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completion()
}

// finish completes the work a held handle was still doing.
func (c *stubConsume) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markClosed()
}

// completion returns the channel Closed hands out, made on first use. Caller
// holds mu.
func (c *stubConsume) completion() chan struct{} {
	if c.closed == nil {
		c.closed = make(chan struct{})
	}
	return c.closed
}

// markClosed signals completion once. Caller holds mu.
func (c *stubConsume) markClosed() {
	ch := c.completion()
	select {
	case <-ch:
	default:
		close(ch)
	}
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
// wait. It fires from inside start, before run has the handle or has published
// up, so it orders call-count assertions only — read state with Eventually.
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

// fakeClock hands run a controllable time and signals every read. run consults
// it once per failure it classifies, so a test can wait for one failure to be
// taken in before moving the clock for the next — nothing about the actor loop
// is synchronous with the callback that feeds it.
type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	read chan struct{}
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0), read: make(chan struct{}, 64)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.read <- struct{}{}:
	default:
	}
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// awaitRead blocks until run has consulted the clock.
func (c *fakeClock) awaitRead(t *testing.T) {
	t.Helper()
	select {
	case <-c.read:
	case <-time.After(5 * time.Second):
		t.Fatal("run never consulted the clock")
	}
}

// newTestSupervisor starts a supervisor whose backoff never really sleeps.
// configure runs before the loop does, so the seams it sets are not written
// concurrently with run reading them.
func newTestSupervisor(t *testing.T, f *consumeFactory, configure ...func(*Supervisor)) *Supervisor {
	t.Helper()
	s := buildSupervisor(context.Background(), "ordered", f.start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	for _, c := range configure {
		c(s)
	}
	require.NoError(t, s.launch())
	waitStarts(t, f, 1)
	t.Cleanup(s.Stop)
	return s
}

// recordDelays captures the backoff schedule run actually asks for.
func recordDelays(delays *[]time.Duration, mu *sync.Mutex) func(*Supervisor) {
	return func(s *Supervisor) {
		s.sleepFn = func(_ context.Context, d time.Duration) bool {
			mu.Lock()
			*delays = append(*delays, d)
			mu.Unlock()
			return true
		}
	}
}

func TestSupervise_FirstStartFailurePropagates(t *testing.T) {
	f := newConsumeFactory(nil, []error{errors.New("consumer not found")})

	s, err := NewSupervisor(context.Background(), "ordered", f.start)

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
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)
}

func TestSupervisor_RepeatedTransientErrorsRestartConsumption(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	for range TransientEscalation {
		f.fail(jetstream.ErrNoHeartbeat)
	}
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)
}

func TestSupervisor_RestartRetriesUntilItSucceeds(t *testing.T) {
	f := newConsumeFactory(nil, []error{nil, errors.New("down"), errors.New("down"), nil})
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 3)

	assert.Equal(t, 4, f.callCount())
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)
}

func TestSupervisor_ConnectionClosedIsTerminal(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConnectionClosed)

	require.Eventually(t, func() bool { return !s.IsUp() }, 2*time.Second, 10*time.Millisecond)
	select {
	case <-f.started:
		t.Fatal("a closed connection must not be restarted against")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Equal(t, 1, f.callCount())
}

func TestSupervisor_StopIsIdempotentAndIgnoresLaterErrors(t *testing.T) {
	handle := &stubConsume{}
	f := newConsumeFactory([]ConsumeContext{handle}, nil)
	s, err := NewSupervisor(context.Background(), "ordered", f.start)
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
	release := make(chan struct{})
	newTestSupervisor(t, f, func(s *Supervisor) {
		s.sleepFn = func(context.Context, time.Duration) bool {
			<-release
			return true
		}
	})

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
	s, err := NewSupervisor(context.Background(), "ordered", f.start)
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
	s, err := NewSupervisor(ctx, "ordered", f.start)
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
	var mu sync.Mutex
	var calls int
	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		onError(jetstream.ErrConsumerDeleted)
		return handle, nil
	}

	s, err := NewSupervisor(context.Background(), "ordered", start)

	require.Error(t, err, "a subscription that dies during start is a startup failure")
	assert.Nil(t, s)
	assert.Equal(t, 1, handle.stopCount(), "the doomed subscription must be released, not orphaned")

	// A failed NewSupervisor must not leave a restart goroutine retrying behind the
	// caller's back — it has nothing to hand the retried subscription to. The
	// first backoff step is 100ms, so a retry would land inside this window.
	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls > 1
	}, time.Second, 20*time.Millisecond, "a failed NewSupervisor must not keep restarting")
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
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)
}

// TransientEscalation is meant to catch a back-to-back run, not a lifetime
// tally: unrelated heartbeat misses that nats.go already self-healed must not
// eventually tear down a healthy subscription and discard its buffered messages.
func TestSupervisor_TransientRunResetsAfterAQuietWindow(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	c := newFakeClock()
	_ = newTestSupervisor(t, f, func(s *Supervisor) { s.nowFn = c.Now })

	for range TransientEscalation - 1 {
		f.fail(jetstream.ErrNoHeartbeat)
		c.awaitRead(t)
	}
	c.advance(EscalationWindow + time.Second)
	f.fail(jetstream.ErrNoHeartbeat)
	c.awaitRead(t)

	select {
	case <-f.started:
		t.Fatal("transient errors spread beyond the window escalated to a restart")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Equal(t, 1, f.callCount())
}

func TestSupervisor_TransientRunWithinTheWindowStillEscalates(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	c := newFakeClock()
	_ = newTestSupervisor(t, f, func(s *Supervisor) { s.nowFn = c.Now })

	for range TransientEscalation {
		c.advance(time.Second)
		f.fail(jetstream.ErrNoHeartbeat)
		c.awaitRead(t)
	}
	waitStarts(t, f, 1)

	assert.Equal(t, 2, f.callCount())
}

// Stop landing while begin is still starting the replacement must leave the
// supervisor reported down, not up on a subscription it just released.
func TestSupervisor_StopDuringRestartLeavesItDown(t *testing.T) {
	handles := []ConsumeContext{&stubConsume{}, &stubConsume{}}
	f := newConsumeFactory(handles, nil)

	opening := make(chan struct{})
	finishOpen := make(chan struct{})
	start := func(ctx context.Context, onError func(error)) (ConsumeContext, error) {
		cc, err := f.start(ctx, onError)
		if f.callCount() > 1 {
			close(opening)
			<-finishOpen // Stop lands while this start is still running
		}
		return cc, err
	}

	s := buildSupervisor(context.Background(), "ordered", start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, s.launch())
	waitStarts(t, f, 1)

	f.fail(jetstream.ErrConsumerDeleted)
	<-opening

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.Stop()
	}()
	require.Eventually(t, s.stopping, 2*time.Second, 10*time.Millisecond,
		"Stop should have taken effect while the replacement was still opening")
	close(finishOpen)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never returned")
	}
	// Asserted directly rather than eventually: once Stop has returned, nothing
	// of ours may still be running.
	assert.False(t, s.IsUp(), "a supervisor stopped mid-restart must report down")
	assert.Equal(t, 1, handles[1].(*stubConsume).stopCount(),
		"the replacement started during Stop must be released before Stop returns")
}

// The replacement can die during its own start. Treating that as a duplicate
// would leave an already-stopped subscription installed and marked up: Consume
// reports a terminal error exactly once, so nothing repairs it and the lane
// stays frozen behind a green health check.
func TestSupervisor_ReplacementFailingDuringStartKeepsRestarting(t *testing.T) {
	var mu sync.Mutex
	failRounds := map[int]bool{1: true, 2: true, 3: true}
	handles := make([]*stubConsume, 0, 8)
	started := make(chan int, 16)
	var lastFail func(error)

	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		mu.Lock()
		round := len(handles) + 1
		h := &stubConsume{}
		handles = append(handles, h)
		fail := failRounds[round]
		lastFail = onError
		mu.Unlock()

		if fail {
			// Terminal status lands while start is still returning.
			onError(jetstream.ErrConsumerDeleted)
		}
		started <- round
		return h, nil
	}

	failed, err := NewSupervisor(context.Background(), "ordered", start)
	require.Error(t, err, "round 1 dying during start is a startup failure")
	assert.Nil(t, failed)

	// Round 1 failed at startup; drive the same shape through a live supervisor.
	mu.Lock()
	failRounds = map[int]bool{2: true, 3: true}
	handles = handles[:0]
	mu.Unlock()
	for len(started) > 0 {
		<-started
	}

	s := buildSupervisor(context.Background(), "ordered", start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, s.launch())
	t.Cleanup(s.Stop)
	require.Equal(t, 1, <-started)

	// Round 1 is live. Kill it; rounds 2 and 3 then die inside start, and only
	// round 4 survives. The supervisor must keep going until one sticks.
	mu.Lock()
	killRound1 := lastFail
	mu.Unlock()
	killRound1(jetstream.ErrConsumerDeleted)

	// run takes the failure in on its own goroutine, so wait for each round
	// rather than probing IsUp, which is still true from the round just killed.
	for want := 2; want <= 4; want++ {
		select {
		case got := <-started:
			require.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d never started", want)
		}
	}
	require.Eventually(t, s.IsUp, 5*time.Second, 10*time.Millisecond,
		"the supervisor must keep restarting until a replacement survives")

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(handles), 4, "rounds that died during start must be retried")
	for i, h := range handles[:len(handles)-1] {
		assert.Equal(t, 1, h.stopCount(), "dead round %d must be released", i+1)
	}
	assert.Equal(t, 0, handles[len(handles)-1].stopCount(), "the surviving round stays live")
}

// A terminal error from a superseded round must not clear the health of the
// replacement that already took over.
func TestSupervisor_StaleStoppedErrorLeavesHealthyRoundUp(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	f.mu.Lock()
	staleOnError := f.lastFail
	f.mu.Unlock()

	staleOnError(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)

	// The dead round's connection-closed error arrives late.
	staleOnError(jetstream.ErrConnectionClosed)

	assert.Never(t, func() bool { return !s.IsUp() }, 500*time.Millisecond, 20*time.Millisecond,
		"a stale terminal error must not mark the live round down")
}

// A terminal error during start must not leave begin publishing a subscription
// nats.go has already given up on as healthy.
func TestSupervise_StoppedErrorDuringStartIsNotPublishedHealthy(t *testing.T) {
	handle := &stubConsume{}
	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		onError(jetstream.ErrConnectionClosed)
		return handle, nil
	}

	s, err := NewSupervisor(context.Background(), "ordered", start)

	require.Error(t, err, "a subscription whose connection closed during start is a startup failure")
	assert.Nil(t, s)
	assert.Equal(t, 1, handle.stopCount(), "the dead subscription must be released, not published")
}

// A replacement that starts cleanly and then dies must keep backing off.
// Restarting the schedule each round would hammer the first step forever.
func TestSupervisor_RestartBackoffAdvancesAcrossRounds(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	c := newFakeClock()
	var mu sync.Mutex
	var delays []time.Duration
	newTestSupervisor(t, f,
		func(s *Supervisor) { s.nowFn = c.Now },
		recordDelays(&delays, &mu))

	// Three rounds that each start fine, then die shortly after. fail goes
	// through the callback the newest round was given, as nats.go would, and
	// waitStarts confirms the backoff ran before the clock moves again.
	for range 3 {
		c.advance(time.Second)
		f.fail(jetstream.ErrConsumerDeleted)
		waitStarts(t, f, 1)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, delays, 3)
	assert.Equal(t, RebuildBackoff[0], delays[0])
	assert.Equal(t, RebuildBackoff[1], delays[1], "a second failing round must back off further")
	assert.Equal(t, RebuildBackoff[2], delays[2])
}

func TestSupervisor_RestartBackoffResetsAfterAQuietSpell(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	c := newFakeClock()
	var mu sync.Mutex
	var delays []time.Duration
	newTestSupervisor(t, f,
		func(s *Supervisor) { s.nowFn = c.Now },
		recordDelays(&delays, &mu))

	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)

	// A round that survived well past the window starts the schedule over.
	c.advance(EscalationWindow + time.Minute)
	f.fail(jetstream.ErrConsumerDeleted)
	waitStarts(t, f, 1)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, delays, 2)
	assert.Equal(t, RebuildBackoff[0], delays[0])
	assert.Equal(t, RebuildBackoff[0], delays[1], "a long-lived round resets the schedule")
}

// The loop runs one round at a time, so a replacement can never begin while the
// previous start is still returning. That window is what the old design had to
// guard against with a generation check and still got wrong; here it does not
// exist, and a round that fails inside start is simply released.
func TestSupervise_RunsOneRoundAtATime(t *testing.T) {
	handle := &stubConsume{}
	entered := make(chan struct{}, 4)
	reported := make(chan struct{})
	release := make(chan struct{})

	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		entered <- struct{}{}
		// Fail, then stall past any backoff a replacement would have waited.
		onError(jetstream.ErrConsumerDeleted)
		close(reported)
		<-release
		return handle, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := NewSupervisor(context.Background(), "ordered", start)
		done <- err
	}()

	<-entered
	<-reported
	select {
	case <-entered:
		t.Fatal("a second round started while the first was still inside start")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	select {
	case err := <-done:
		require.Error(t, err, "a round that failed inside start is a startup failure")
	case <-time.After(5 * time.Second):
		t.Fatal("NewSupervisor never returned")
	}
	assert.Equal(t, 1, handle.stopCount(), "the failed round's handle must be released")
}

// observe runs on a nats.go goroutine, so it must never block there, and a
// terminal failure must still reach run: Consume reports one exactly once, so
// losing it is the silent stall this package exists to prevent.
//
// It must also not answer a flood by starting a goroutine per report. The queue
// only backs up while run is parked on rebuild backoff, which is exactly when an
// outage is producing errors, so "one goroutine each" grows for as long as the
// outage lasts.
func TestSupervisor_ObserveNeitherBlocksNorSpawns(t *testing.T) {
	s := buildSupervisor(context.Background(), "ordered", nil)

	const n = 512 // far past the queue's capacity
	settle(t)
	before := runtime.NumGoroutine()

	reported := make(chan struct{})
	go func() {
		defer close(reported)
		for range n {
			s.observe(1, jetstream.ErrConsumerDeleted)
		}
	}()

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("observe blocked once the queue filled")
	}
	assert.Less(t, runtime.NumGoroutine()-before, 8,
		"observe started a goroutine per report once the queue was full")

	// 16 fit in the queue and one waits in the slot, so the failure is still
	// there to be found rather than dropped on the floor.
	assert.Len(t, s.failures, 16)
	assert.Len(t, s.overflow, 1, "a terminal failure must survive a full queue")
}

// A recoverable error is safe to drop when the queue is full — a consumer that
// is still failing reports again — but it must never cost a goroutine either.
func TestSupervisor_ObserveDropsOverflowingTransients(t *testing.T) {
	s := buildSupervisor(context.Background(), "ordered", nil)

	settle(t)
	before := runtime.NumGoroutine()
	for range 512 {
		s.observe(1, jetstream.ErrNoHeartbeat)
	}

	assert.Less(t, runtime.NumGoroutine()-before, 8,
		"a transient flood must not spawn relays")
	assert.Len(t, s.overflow, 0, "a transient must not take the terminal slot")
}

// settle gives goroutines from earlier tests a chance to exit, so a count taken
// next is not measuring their teardown.
func settle(t *testing.T) {
	t.Helper()
	assert.Eventually(t, func() bool {
		n := runtime.NumGoroutine()
		time.Sleep(10 * time.Millisecond)
		return runtime.NumGoroutine() <= n
	}, 2*time.Second, 10*time.Millisecond)
}

// A recoverable error reported while the round is still starting must not abort
// it: nats.go re-pulls on its own and the handle is still on its way.
func TestSupervise_TransientDuringStartStillInstallsTheRound(t *testing.T) {
	handle := &stubConsume{}
	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		onError(jetstream.ErrNoHeartbeat)
		return handle, nil
	}

	s, err := NewSupervisor(context.Background(), "ordered", start)
	require.NoError(t, err)
	t.Cleanup(s.Stop)

	assert.True(t, s.IsUp())
	assert.Zero(t, handle.stopCount(), "a heartbeat miss must not release the round")
}

// A terminal error reported while open is still running must fail the start,
// not be installed and reported healthy. open runs on run's own goroutine now,
// so the failure is deterministically in the queue by the time the handle is
// considered — this needed a test seam back when a select decided the order.
func TestSupervisor_FailureDuringStartOutranksTheHandle(t *testing.T) {
	handle := &stubConsume{}
	start := func(_ context.Context, onError func(error)) (ConsumeContext, error) {
		onError(jetstream.ErrConnectionClosed)
		return handle, nil
	}

	s, err := NewSupervisor(context.Background(), "ordered", start)

	require.Error(t, err, "a round with a terminal failure against it must not start")
	assert.Nil(t, s)
	assert.ErrorIs(t, err, jetstream.ErrConnectionClosed, "the original error must reach the caller")
	assert.Equal(t, 1, handle.stopCount(), "the dead round must be released")
}

// reportedFailure decides which queued failure, if any, ends the round.
func TestSupervisor_ReportedFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		queued []roundFailure
		want   error
	}{
		{"nothing queued", nil, nil},
		{
			"another round's failure is not ours",
			[]roundFailure{{gen: 7, err: jetstream.ErrConsumerDeleted}},
			nil,
		},
		{
			"a recoverable error does not end the round",
			[]roundFailure{{gen: 1, err: jetstream.ErrNoHeartbeat}},
			nil,
		},
		{
			"a terminal error ends it",
			[]roundFailure{{gen: 1, err: jetstream.ErrConsumerDeleted}},
			jetstream.ErrConsumerDeleted,
		},
		{
			"it looks past noise for one that counts",
			[]roundFailure{
				{gen: 9, err: jetstream.ErrConsumerDeleted},
				{gen: 1, err: jetstream.ErrNoHeartbeat},
				{gen: 1, err: jetstream.ErrConnectionClosed},
			},
			jetstream.ErrConnectionClosed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := buildSupervisor(context.Background(), "ordered", nil)
			for _, f := range tc.queued {
				s.failures <- f
			}

			got := s.reportedFailure(1)

			if tc.want == nil {
				assert.NoError(t, got)
				return
			}
			assert.ErrorIs(t, got, tc.want)
		})
	}
}

// A closed connection while a replacement is starting means the same as one
// reported against a live round: nothing can be built against it, so retrying
// is a busy loop against a connection that is gone.
func TestSupervisor_ConnectionClosedWhileRestartingEndsSupervision(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	s := newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted) // round 1 dies, round 2 starts
	waitStarts(t, f, 1)
	require.Eventually(t, s.IsUp, 2*time.Second, 10*time.Millisecond)

	f.fail(jetstream.ErrConnectionClosed)

	require.Eventually(t, func() bool { return !s.IsUp() }, 2*time.Second, 10*time.Millisecond)
	select {
	case <-f.started:
		t.Fatal("a closed connection must not be restarted against")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Equal(t, 2, f.callCount())
}

// Stop is a shutdown hook: when it returns, the caller drains NATS and closes
// databases. A replacement still inside open would otherwise start consuming
// after that point, and its handler's writes would land against closed
// dependencies — un-acked, and redelivered on the next boot.
func TestSupervisor_StopWaitsForAnOpenStillRunning(t *testing.T) {
	handles := []ConsumeContext{&stubConsume{}, &stubConsume{}}
	f := newConsumeFactory(handles, nil)
	opening := make(chan struct{})
	release := make(chan struct{})

	start := func(ctx context.Context, onError func(error)) (ConsumeContext, error) {
		cc, err := f.start(ctx, onError)
		if f.callCount() > 1 {
			close(opening)
			<-release // the replacement is mid-open when Stop lands
		}
		return cc, err
	}

	s := buildSupervisor(context.Background(), "ordered", start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, s.launch())
	waitStarts(t, f, 1)

	f.fail(jetstream.ErrConsumerDeleted)
	<-opening

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.Stop()
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a replacement was still being opened")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never returned")
	}
	assert.Equal(t, 1, handles[1].(*stubConsume).stopCount(),
		"the replacement must be released before Stop returns")
}

// Stop cancels the context an open is running under, so a call to a peer that
// is gone is abandoned rather than waited out inside the shutdown budget.
func TestSupervisor_StopCancelsAnOpenInProgress(t *testing.T) {
	f := newConsumeFactory(nil, nil)
	opening := make(chan struct{})
	cancelled := make(chan struct{})

	start := func(ctx context.Context, onError func(error)) (ConsumeContext, error) {
		cc, err := f.start(ctx, onError)
		if f.callCount() > 1 {
			close(opening)
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		}
		return cc, err
	}

	s := buildSupervisor(context.Background(), "ordered", start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, s.launch())
	waitStarts(t, f, 1)

	f.fail(jetstream.ErrConsumerDeleted)
	<-opening
	s.Stop()

	select {
	case <-cancelled:
	default:
		t.Fatal("Stop did not cancel the open it was waiting on")
	}
	assert.False(t, s.IsUp())
}

// Stop ends delivery but does not wait for a handler already executing. Opening
// the replacement before that handler returns puts two consumers on a durable
// that must stay sequential — and since the stopped round never acked, the
// replacement is handed the very message still being processed.
func TestSupervisor_RestartWaitsForInFlightWork(t *testing.T) {
	busy := &stubConsume{hold: true}
	f := newConsumeFactory([]ConsumeContext{busy, &stubConsume{}}, nil)
	newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted)

	select {
	case <-f.started:
		t.Fatal("a replacement opened while the stopped round still had work in flight")
	case <-time.After(200 * time.Millisecond):
	}

	busy.finish()
	waitStarts(t, f, 1)
}

// The wait is a bound, not a promise: a handler that never returns delays the
// restart rather than blocking it forever, because a lane that stays down is
// the failure this package exists to prevent.
func TestSupervisor_RestartProceedsWhenAHandlerWedges(t *testing.T) {
	restore := releaseWait
	releaseWait = 50 * time.Millisecond
	t.Cleanup(func() { releaseWait = restore })

	wedged := &stubConsume{hold: true}
	f := newConsumeFactory([]ConsumeContext{wedged, &stubConsume{}}, nil)
	newTestSupervisor(t, f)

	f.fail(jetstream.ErrConsumerDeleted)

	waitStarts(t, f, 1)
	assert.Equal(t, 1, wedged.stopCount())
}

// Stop can land while a round is still opening. Without the stopped check that
// follows open, run installs the handle and publishes up before serve notices
// the shutdown — readiness flickers green on a pod that is already terminating.
func TestSupervisor_StopDuringAStartNeverReportsUp(t *testing.T) {
	opening := make(chan struct{})
	finishOpen := make(chan struct{})
	handle := &stubConsume{}
	start := func(_ context.Context, _ func(error)) (ConsumeContext, error) {
		close(opening)
		<-finishOpen
		return handle, nil
	}

	s := buildSupervisor(context.Background(), "ordered", start)
	s.sleepFn = func(context.Context, time.Duration) bool { return true }
	launched := make(chan error, 1)
	go func() { launched <- s.launch() }()
	<-opening

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.Stop()
	}()
	require.Eventually(t, s.stopping, 2*time.Second, 10*time.Millisecond)
	close(finishOpen)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never returned")
	}
	require.ErrorIs(t, <-launched, ErrStopped, "a round opened into a stopped supervisor must not be installed")
	assert.False(t, s.IsUp())
	assert.Equal(t, 1, handle.stopCount(), "the handle must still be released")
}

// A terminal error that found no room in the queue waits in the overflow slot.
// serve is the only reader while a round is live, so if it does not drain that
// slot the round's one and only death notice is never seen and the lane hangs.
func TestSupervisor_ServeReadsTheOverflowSlot(t *testing.T) {
	s := buildSupervisor(context.Background(), "ordered", nil)
	// Fill the queue with a superseded round's noise, the way a storm would.
	for range cap(s.failures) {
		s.failures <- roundFailure{gen: 99, err: jetstream.ErrNoHeartbeat}
	}
	s.overflow <- roundFailure{gen: 1, err: jetstream.ErrConsumerDeleted}

	cc := &stubConsume{}
	keepGoing := s.serve(1, cc)

	assert.True(t, keepGoing, "a terminal in the slot must restart the round")
	assert.Equal(t, 1, cc.stopCount(), "the dead round must be released")
}
