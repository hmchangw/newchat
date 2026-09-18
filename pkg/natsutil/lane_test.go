package natsutil_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// fakeIter feeds a fixed number of messages and then reports the iterator-closed
// error the real MessagesContext returns after Stop. exhausted closes when the
// loop has been told to exit, which is the point after which no further wg.Add
// can happen — the only safe moment for a test to Wait.
type fakeIter struct {
	mu        sync.Mutex
	n         int
	stopped   bool
	exhausted chan struct{}
	once      sync.Once
}

func newFakeIter(n int) *fakeIter {
	return &fakeIter{n: n, exhausted: make(chan struct{})}
}

func (f *fakeIter) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped || f.n == 0 {
		f.once.Do(func() { close(f.exhausted) })
		return nil, nil, errors.New("iterator closed")
	}
	f.n--
	return context.Background(), nil, nil
}

func (f *fakeIter) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = true
}

func TestRunPool_HandlesEveryMessageThenExitsOnIteratorError(t *testing.T) {
	iter := newFakeIter(5)
	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup

	var mu sync.Mutex
	seen := 0
	natsutil.RunPool(iter, sem, &wg, func(context.Context, jetstream.Msg) {
		mu.Lock()
		seen++
		mu.Unlock()
	}, nil)

	wgDone(t, iter, &wg)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 5, seen)
}

// The semaphore is the only thing bounding in-flight handlers; if RunPool
// released it before the handler returned, a burst would run unbounded.
func TestRunPool_BoundsConcurrencyBySemaphore(t *testing.T) {
	iter := newFakeIter(20)
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup

	var mu sync.Mutex
	inFlight, peak := 0, 0
	natsutil.RunPool(iter, sem, &wg, func(context.Context, jetstream.Msg) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
	}, nil)

	wgDone(t, iter, &wg)
	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, peak, 3, "in-flight handlers must never exceed the semaphore")
}

// A lane that never bound is still a value the shutdown list can call, so no
// service needs a nil guard around it.
func TestLane_StopIsNilSafe(t *testing.T) {
	var l *natsutil.Lane
	assert.NotPanics(t, func() { l.Stop() })
	assert.False(t, l.Bound())
}

func TestLane_StopStopsTheIterator(t *testing.T) {
	iter := newFakeIter(0)
	lane := natsutil.NewLane(iter)
	lane.Stop()

	iter.mu.Lock()
	defer iter.mu.Unlock()
	assert.True(t, iter.stopped)
}

// wgDone waits for the pool loop to exit before waiting on the WaitGroup —
// Wait must not race the loop's own Add calls.
func wgDone(t *testing.T, iter *fakeIter, wg *sync.WaitGroup) {
	t.Helper()
	select {
	case <-iter.exhausted:
	case <-time.After(2 * time.Second):
		t.Fatal("pull loop did not exit")
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not drain")
	}
}

func TestWaitPool_ReturnsWhenEveryWorkerIsDone(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { time.Sleep(5 * time.Millisecond); wg.Done() }()

	require.NoError(t, natsutil.WaitPool(context.Background(), &wg))
}

func TestWaitPool_TimesOutOnAWedgedWorker(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { <-release; wg.Done() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := natsutil.WaitPool(ctx, &wg)
	close(release)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker drain timed out")
}

// Two lanes sharing one pool is the contract that keeps a buddy lane from
// doubling a service's concurrency budget against its databases.
func TestRunPool_TwoLanesShareOneBudget(t *testing.T) {
	iterA, iterB := newFakeIter(20), newFakeIter(20)
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup

	var mu sync.Mutex
	inFlight, peak := 0, 0
	handle := func(context.Context, jetstream.Msg) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	natsutil.RunPool(iterA, sem, &wg, handle, nil)
	natsutil.RunPool(iterB, sem, &wg, handle, nil)

	wgDone(t, iterA, &wg)
	wgDone(t, iterB, &wg)
	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, peak, 3,
		"a second lane must draw from the same budget, not add its own")
}

// A pooled loop shares its WaitGroup with every other lane, so the group keeps
// looking busy after one of them dies. The stopped hook is the only signal a
// caller has, and it must carry the terminal error, not just the fact of it.
func TestRunPool_ReportsTheTerminalErrorToStopped(t *testing.T) {
	iter := newFakeIter(2)
	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup

	stopped := make(chan error, 1)
	natsutil.RunPool(iter, sem, &wg, func(context.Context, jetstream.Msg) {},
		func(err error) { stopped <- err })

	wgDone(t, iter, &wg)
	select {
	case err := <-stopped:
		require.Error(t, err, "a loop that exited must report why")
		assert.Contains(t, err.Error(), "iterator closed")
	case <-time.After(2 * time.Second):
		t.Fatal("stopped was never called; a dead lane would sit unobserved")
	}
}

// A nil hook is the ordinary case for a single-lane caller, and must not panic
// the loop goroutine on the way out.
func TestRunPool_NilStoppedIsSafe(t *testing.T) {
	iter := newFakeIter(1)
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup

	natsutil.RunPool(iter, sem, &wg, func(context.Context, jetstream.Msg) {}, nil)
	wgDone(t, iter, &wg)
}
