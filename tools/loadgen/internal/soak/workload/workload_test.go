package workload

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
)

func TestSoakNew_DefaultsAndRunDependencies(t *testing.T) {
	workload := New(&Config{HeartbeatInterval: 90 * time.Second}, nil, nil, nil, nil, nil)
	assert.Equal(t, 256, workload.cfg.MaxInFlight)
	assert.Equal(t, 185*time.Second, workload.cfg.HeartbeatStaleAfter)
	_, err := workload.Run(context.Background())
	require.ErrorContains(t, err, "lifecycle")

	lifecycle := newFakeLifecycle(time.Now())
	workload = New(nil, lifecycle, nil, nil, nil, nil)
	_, err = workload.Run(context.Background())
	require.ErrorContains(t, err, "dispatcher")
}

func TestSoakWorkload_RunsEveryConfiguredLaneAndRecordsPacing(t *testing.T) {
	start := time.Now()
	lifecycle := newFakeLifecycle(start)
	dispatcher := &recordingDispatcher{}
	pacing := &capturePacing{}
	var calls sync.Map
	count := func(name string) Action {
		return func(context.Context, bool) error {
			value, _ := calls.LoadOrStore(name, &atomic.Int64{})
			value.(*atomic.Int64).Add(1)
			return nil
		}
	}
	actions := Actions{
		Send: count("send"), Read: count("read"), Mutation: count("mutation"),
		Reaction: count("reaction"), PinnedList: count("pinned_list"), Verify: count("verify"),
		MemberMutation: count("member_mutation"), RoomMutation: count("room_mutation"),
		RoomRead: count("room_read"), UserRead: count("user_read"),
		SearchRead: count("search_read"), RoomCreate: count("room_create"),
		ReadReceipt: count("read_receipt"), Presence: count("presence"),
	}
	cfg := allRatesConfig()
	workload := New(&cfg, lifecycle, &actions, dispatcher.Dispatch, time.Now, nil,
		WithPacingRecorder(pacing))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := workload.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, CompletionCanceled, result.Completion)
	assert.Len(t, dispatcher.rates(), 14)
	assert.Len(t, pacing.configured, 14)
	for name := range dispatcher.rates() {
		value, ok := calls.Load(name)
		require.True(t, ok, name)
		assert.Equal(t, int64(1), value.(*atomic.Int64).Load(), name)
		assert.Equal(t, 1, pacing.count(name, PacingDispatched), name)
	}
}

func TestSoakWorkload_MarksWarmupAndMeasured(t *testing.T) {
	start := time.Unix(100, 0)
	now := sequenceNow(start, start, start, start.Add(5*time.Second), start.Add(11*time.Second))
	dispatcher := &recordingDispatcher{callsPerLane: 2}
	var warmup, measured atomic.Int64
	action := func(_ context.Context, isMeasured bool) error {
		if isMeasured {
			measured.Add(1)
		} else {
			warmup.Add(1)
		}
		return nil
	}
	cfg := Config{RunID: "run", Duration: time.Hour, Warmup: 10 * time.Second, SendRate: 1}
	workload := New(&cfg, newFakeLifecycle(start), &Actions{Send: action}, dispatcher.Dispatch, now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = workload.Run(ctx)
	assert.Positive(t, warmup.Load())
	assert.Positive(t, measured.Load())
}

func TestSoakWorkload_GlobalBudgetAndSaturation(t *testing.T) {
	const limit = 3
	dispatcher := &burstDispatcher{burst: 20}
	release := make(chan struct{})
	var active, maximum, saturated atomic.Int64
	action := func(ctx context.Context, _ bool) error {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		defer active.Add(-1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	cfg := Config{
		RunID: "run", Duration: 50 * time.Millisecond, SendRate: 1,
		ReadRate: 1, ReactionRate: 1, MaxInFlight: limit,
		HeartbeatInterval: 10 * time.Millisecond, HeartbeatStaleAfter: 6 * time.Second,
	}
	lifecycle := newFakeLifecycle(time.Now())
	lifecycle.window.Deadline = time.Now().Add(50 * time.Millisecond)
	workload := New(&cfg, lifecycle,
		&Actions{Send: action, Read: action, Reaction: action}, dispatcher.Dispatch,
		time.Now, func() { saturated.Add(1) })
	done := make(chan error, 1)
	go func() {
		_, err := workload.Run(context.Background())
		done <- err
	}()
	require.Eventually(t, func() bool { return maximum.Load() == limit }, time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-done)
	assert.LessOrEqual(t, maximum.Load(), int64(limit))
	assert.Positive(t, saturated.Load())
}

func TestSoakWorkload_CancellationDrainsAndContinuousModeStops(t *testing.T) {
	start := time.Now()
	lifecycle := newFakeLifecycle(start)
	started := make(chan struct{})
	release := make(chan struct{})
	dispatcher := &singleDispatcher{}
	cfg := Config{RunID: "run", Continuous: true, SendRate: 1}
	workload := New(&cfg, lifecycle, &Actions{Send: func(context.Context, bool) error {
		close(started)
		<-release
		return nil
	}}, dispatcher.Dispatch, time.Now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := workload.Run(ctx)
		done <- err
	}()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned before the action drained")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, 1, lifecycle.stopCalls)
}

func TestSoakWorkload_CompletionAndFailurePaths(t *testing.T) {
	t.Run("elapsed deadline completes without dispatch", func(t *testing.T) {
		now := time.Now()
		lifecycle := newFakeLifecycle(now)
		lifecycle.window.Deadline = now.Add(-time.Second)
		dispatcher := &recordingDispatcher{}
		cfg := Config{RunID: "run", Duration: time.Hour, SendRate: 1}
		result, err := New(&cfg, lifecycle, &Actions{Send: func(context.Context, bool) error {
			t.Fatal("must not dispatch")
			return nil
		}}, dispatcher.Dispatch, func() time.Time { return now }, nil).Run(context.Background())
		require.NoError(t, err)
		assert.Equal(t, CompletionConfiguredDuration, result.Completion)
		assert.Equal(t, 1, lifecycle.completeCalls)
		assert.Empty(t, dispatcher.rates())
	})

	t.Run("action error stops a restartable run", func(t *testing.T) {
		wantErr := errors.New("dependency unavailable")
		lifecycle := newFakeLifecycle(time.Now())
		dispatcher := &singleDispatcher{}
		cfg := Config{RunID: "run", Duration: time.Hour, SendRate: 1, StopOnActionError: true}
		result, err := New(&cfg, lifecycle, &Actions{Send: func(context.Context, bool) error {
			return wantErr
		}}, dispatcher.Dispatch, time.Now, nil).Run(context.Background())
		assert.ErrorIs(t, err, wantErr)
		assert.Equal(t, CompletionDependencyFailure, result.Completion)
		assert.Zero(t, lifecycle.completeCalls)
	})

	for name, test := range map[string]struct {
		configure func(*fakeLifecycle)
		want      string
	}{
		"prepare":  {func(l *fakeLifecycle) { l.prepareErr = errors.New("prepare") }, "prepare"},
		"complete": {func(l *fakeLifecycle) { l.completeErr = errors.New("complete") }, "complete"},
	} {
		t.Run(name+" error", func(t *testing.T) {
			now := time.Now()
			lifecycle := newFakeLifecycle(now)
			test.configure(lifecycle)
			if name == "complete" {
				lifecycle.window.Deadline = now.Add(-time.Second)
			}
			cfg := Config{RunID: "run", Duration: time.Hour}
			_, err := New(&cfg, lifecycle, nil, (&recordingDispatcher{}).Dispatch,
				func() time.Time { return now }, nil).Run(context.Background())
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestSoakWorkload_LeaseRiskBoundsDrainAndInvalidates(t *testing.T) {
	const interval = 20 * time.Millisecond
	start := time.Now()
	lifecycle := newFakeLifecycle(start)
	lifecycle.touchErr = errors.New("mongo unavailable")
	started := make(chan struct{})
	release := make(chan struct{})
	var atRisk atomic.Bool
	now := func() time.Time {
		if atRisk.Load() {
			return start.Add(5020 * time.Millisecond)
		}
		return start
	}
	var invalidated atomic.Bool
	cfg := Config{
		RunID: "run", Duration: time.Hour, SendRate: 1,
		HeartbeatInterval: interval, HeartbeatStaleAfter: 5040 * time.Millisecond,
	}
	workload := New(&cfg, lifecycle, &Actions{Send: func(context.Context, bool) error {
		atRisk.Store(true)
		close(started)
		<-release
		return nil
	}}, (&singleDispatcher{}).Dispatch, now, nil,
		WithFailureInvalidation(func() { invalidated.Store(true) }))
	done := make(chan struct{})
	var result Result
	var runErr error
	go func() {
		result, runErr = workload.Run(context.Background())
		close(done)
	}()
	<-started
	select {
	case <-done:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("lease-risk shutdown did not bound the lane drain")
	}
	close(release)
	assert.ErrorIs(t, runErr, ErrHeartbeatLeaseAtRisk)
	assert.Equal(t, CompletionDependencyFailure, result.Completion)
	assert.True(t, result.LeaseAbort)
	assert.True(t, invalidated.Load())
}

func TestSoakRunHeartbeat_SuccessRecoveryInactiveAndCancellation(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	t.Run("success", func(t *testing.T) {
		store := &heartbeatStore{}
		ticks := make(chan time.Time, 2)
		ticks <- start
		ticks <- start.Add(time.Second)
		close(ticks)
		observer := &captureHeartbeat{}
		require.NoError(t, RunHeartbeat(context.Background(), store, "run", ticks,
			time.Second, time.Second, 5*time.Second, time.Second,
			start.Add(-time.Second), nil, func() time.Time { return start }, observer))
		assert.Len(t, store.times, 2)
		assert.Equal(t, []HeartbeatOutcome{HeartbeatSuccess, HeartbeatSuccess}, observer.outcomes)
	})

	t.Run("failure retries and recovers", func(t *testing.T) {
		store := &heartbeatStore{errors: []error{errors.New("down"), nil}}
		ticks := make(chan time.Time, 1)
		ticks <- start
		close(ticks)
		current := start
		observer := &captureHeartbeat{}
		require.NoError(t, RunHeartbeat(context.Background(), store, "run", ticks,
			time.Second, time.Second, 5*time.Second, time.Second, start.Add(-time.Second),
			func(context.Context, time.Duration) error {
				current = current.Add(time.Second)
				return nil
			}, func() time.Time { return current }, observer))
		assert.Equal(t, []HeartbeatOutcome{HeartbeatError, HeartbeatSuccess}, observer.outcomes)
		assert.Equal(t, []bool{true, false}, observer.degraded)
	})

	t.Run("inactive", func(t *testing.T) {
		store := &heartbeatStore{errors: []error{ErrRunNotActive}}
		ticks := make(chan time.Time, 1)
		ticks <- start
		observer := &captureHeartbeat{}
		err := RunHeartbeat(context.Background(), store, "run", ticks,
			time.Second, time.Second, 5*time.Second, time.Second,
			start.Add(-time.Second), nil, func() time.Time { return start }, observer)
		assert.ErrorIs(t, err, ErrRunNotActive)
		assert.Equal(t, []HeartbeatOutcome{HeartbeatNotActive}, observer.outcomes)
	})

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := RunHeartbeat(ctx, &heartbeatStore{}, "run", make(chan time.Time),
			time.Second, time.Second, 5*time.Second, time.Second,
			start, nil, time.Now, nil)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestSoakRunHeartbeat_RejectsUnsafeOrExpiredLease(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	for name, test := range map[string]struct {
		stale  time.Duration
		margin time.Duration
		last   time.Time
		now    time.Time
		want   error
	}{
		"unsafe durations":     {time.Second, time.Second, start, start, ErrHeartbeatLeaseInvalid},
		"missing last success": {5 * time.Second, time.Second, time.Time{}, start, nil},
		"expired":              {5 * time.Second, time.Second, start, start.Add(4 * time.Second), ErrHeartbeatLeaseAtRisk},
	} {
		t.Run(name, func(t *testing.T) {
			ticks := make(chan time.Time, 1)
			ticks <- test.now
			err := RunHeartbeat(context.Background(), &heartbeatStore{}, "run", ticks,
				time.Second, time.Second, test.stale, test.margin, test.last, nil,
				func() time.Time { return test.now }, nil)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			} else {
				require.ErrorContains(t, err, "last persisted")
			}
		})
	}
	_, ok := MinimumHeartbeatStaleAfter(time.Duration(1<<62), time.Second)
	assert.False(t, ok)
	_, ok = MinimumHeartbeatStaleAfter(0, time.Second)
	assert.False(t, ok)
}

func TestSoakWaitHelpersAndLaneActivity(t *testing.T) {
	activity := newLaneActivity()
	finishA := activity.Start("send")
	finishB := activity.Start("send")
	active, total := activity.Snapshot()
	assert.Equal(t, map[string]int{"send": 2}, active)
	assert.Equal(t, 2, total)
	finishA()
	finishB()
	active, total = activity.Snapshot()
	assert.Empty(t, active)
	assert.Zero(t, total)

	var wg sync.WaitGroup
	assert.True(t, waitLaneDrain(&wg, time.Second))
	wg.Add(1)
	assert.False(t, waitLaneDrain(&wg, time.Millisecond))
	wg.Done()
	assert.True(t, waitFailureInvalidation(nil, time.Second))
	assert.True(t, waitFailureInvalidation(func() {}, time.Second))
	release := make(chan struct{})
	assert.False(t, waitFailureInvalidation(func() { <-release }, time.Millisecond))
	close(release)
}

func allRatesConfig() Config {
	return Config{
		RunID: "run", Duration: time.Hour,
		SendRate: 1, ReadRate: 2, MutationRate: 3, ReactionRate: 4,
		PinnedListRate: 5, VerifyRate: 6, MemberMutationRate: 7,
		RoomMutationRate: 8, RoomReadRate: 9, UserReadRate: 10,
		SearchReadRate: 11, RoomCreateRate: 12, ReadReceiptRate: 13,
		PresenceRate: 14,
	}
}

type fakeLifecycle struct {
	window        run.Window
	prepareErr    error
	completeErr   error
	stopErr       error
	touchErr      error
	completeCalls int
	stopCalls     int
}

func newFakeLifecycle(now time.Time) *fakeLifecycle {
	return &fakeLifecycle{window: run.Window{
		Deadline: now.Add(time.Hour), LastHeartbeatAt: now,
	}}
}

func (l *fakeLifecycle) Prepare(
	context.Context, string, time.Duration, bool, time.Time,
) (run.Window, error) {
	return l.window, l.prepareErr
}

func (l *fakeLifecycle) Complete(context.Context, string, time.Time) error {
	l.completeCalls++
	return l.completeErr
}

func (l *fakeLifecycle) Stop(context.Context, string, time.Time) error {
	l.stopCalls++
	return l.stopErr
}

func (l *fakeLifecycle) TouchHeartbeat(context.Context, string, time.Time) error {
	return l.touchErr
}

type recordingDispatcher struct {
	mu           sync.Mutex
	seen         map[string]float64
	callsPerLane int
}

func (d *recordingDispatcher) Dispatch(
	ctx context.Context, lane string, rate float64, _ int,
	recordUnderrun func(int), recordSaturation func(), action func(context.Context),
) {
	d.mu.Lock()
	if d.seen == nil {
		d.seen = make(map[string]float64)
	}
	d.seen[lane] = rate
	calls := max(1, d.callsPerLane)
	d.mu.Unlock()
	for range calls {
		action(ctx)
	}
	recordUnderrun(0)
	recordSaturation()
}

func (d *recordingDispatcher) rates() map[string]float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(map[string]float64, len(d.seen))
	for name, rate := range d.seen {
		result[name] = rate
	}
	return result
}

type singleDispatcher struct{}

func (d *singleDispatcher) Dispatch(
	ctx context.Context, _ string, _ float64, _ int,
	_ func(int), _ func(), action func(context.Context),
) {
	action(ctx)
	<-ctx.Done()
}

type burstDispatcher struct{ burst int }

func (d *burstDispatcher) Dispatch(
	ctx context.Context, _ string, _ float64, _ int,
	_ func(int), _ func(), action func(context.Context),
) {
	var wg sync.WaitGroup
	for range d.burst {
		wg.Add(1)
		go func() {
			defer wg.Done()
			action(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

type capturePacing struct {
	mu         sync.Mutex
	configured map[string]float64
	recorded   map[string]map[PacingOutcome]int
}

func (p *capturePacing) Configure(lane string, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.configured == nil {
		p.configured = make(map[string]float64)
	}
	p.configured[lane] = rate
}

func (p *capturePacing) Record(lane string, outcome PacingOutcome, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.recorded == nil {
		p.recorded = make(map[string]map[PacingOutcome]int)
	}
	if p.recorded[lane] == nil {
		p.recorded[lane] = make(map[PacingOutcome]int)
	}
	p.recorded[lane][outcome] += count
}

func (p *capturePacing) count(lane string, outcome PacingOutcome) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.recorded[lane][outcome]
}

type heartbeatStore struct {
	errors []error
	times  []time.Time
}

func (s *heartbeatStore) TouchHeartbeat(_ context.Context, _ string, at time.Time) error {
	if len(s.errors) > 0 {
		err := s.errors[0]
		s.errors = s.errors[1:]
		if err != nil {
			return err
		}
	}
	s.times = append(s.times, at)
	return nil
}

type captureHeartbeat struct {
	outcomes []HeartbeatOutcome
	degraded []bool
}

func (o *captureHeartbeat) RecordHeartbeatAttempt(
	outcome HeartbeatOutcome, degraded bool, _ time.Time,
) {
	o.outcomes = append(o.outcomes, outcome)
	o.degraded = append(o.degraded, degraded)
}

func sequenceNow(values ...time.Time) func() time.Time {
	var mu sync.Mutex
	index := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(values) {
			return values[len(values)-1]
		}
		value := values[index]
		index++
		return value
	}
}
