package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakWorkload_RunsIndependentConfiguredLanes(t *testing.T) {
	dispatcher := &recordingSoakDispatcher{}
	var calls sync.Map
	actions := soakWorkloadActions{
		Send:       countSoakAction(&calls, "send"),
		Read:       countSoakAction(&calls, "read"),
		Mutation:   countSoakAction(&calls, "mutation"),
		Reaction:   countSoakAction(&calls, "reaction"),
		PinnedList: countSoakAction(&calls, "pinned_list"),
		Verify:     countSoakAction(&calls, "verify"),
	}
	store := seededLifecycleStore("run-1")
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: time.Hour, Warmup: 0,
		SendRate: 100, ReadRate: 700, MutationRate: 5,
		ReactionRate: 87, PinnedListRate: 2, VerifyRate: 1,
		MaxInFlight: 16,
	}, store, actions, dispatcher.Dispatch, time.Now, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := workload.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, soakCompletionCanceled, result.Completion)

	assert.Equal(t, map[string]int{
		"send": 100, "read": 700, "mutation": 5,
		"reaction": 87, "pinned_list": 2, "verify": 1,
	}, dispatcher.rates())
	for _, name := range []string{
		"send", "read", "mutation", "reaction", "pinned_list", "verify",
	} {
		value, ok := calls.Load(name)
		require.True(t, ok)
		assert.Equal(t, int64(1), value.(*atomic.Int64).Load())
	}
}

func TestSoakWorkload_MarksWarmupAndMeasuredActions(t *testing.T) {
	dispatcher := &recordingSoakDispatcher{callsPerLane: 2}
	var warmup, measured atomic.Int64
	action := func(_ context.Context, isMeasured bool) error {
		if isMeasured {
			measured.Add(1)
		} else {
			warmup.Add(1)
		}
		return nil
	}
	start := time.Unix(100, 0)
	now := sequenceSoakNow(
		start,
		start,
		start,
		start.Add(5*time.Second),
		start.Add(11*time.Second),
	)
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: time.Hour, Warmup: 10 * time.Second,
		SendRate: 1, MaxInFlight: 2,
	}, seededLifecycleStoreAt("run-1", start), soakWorkloadActions{
		Send: action,
	}, dispatcher.Dispatch, now, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = workload.Run(ctx)

	assert.Greater(t, warmup.Load(), int64(0))
	assert.Greater(t, measured.Load(), int64(0))
}

func TestSoakWorkload_GlobalInFlightBudgetBoundsAllLanes(t *testing.T) {
	const limit = 3
	dispatcher := &burstSoakDispatcher{burst: 20}
	release := make(chan struct{})
	var active, maximum atomic.Int64
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
	var saturated atomic.Int64
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: 50 * time.Millisecond, Warmup: 0,
		SendRate: 100, ReadRate: 100, ReactionRate: 100,
		MaxInFlight: limit,
	}, seededLifecycleStore("run-1"), soakWorkloadActions{
		Send: action, Read: action, Reaction: action,
	}, dispatcher.Dispatch, time.Now, func() { saturated.Add(1) })

	done := make(chan struct{})
	var result soakWorkloadResult
	var runErr error
	go func() {
		result, runErr = workload.Run(context.Background())
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	<-done

	require.NoError(t, runErr)
	assert.Equal(t, soakCompletionConfiguredDuration, result.Completion)
	assert.LessOrEqual(t, maximum.Load(), int64(limit))
	assert.Greater(t, saturated.Load(), int64(0))
}

func TestSoakWorkload_CancellationStopsNewWorkAndDrainsInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	dispatcher := &singleSoakDispatcher{}
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: time.Hour, SendRate: 1, MaxInFlight: 1,
	}, seededLifecycleStore("run-1"), soakWorkloadActions{
		Send: func(context.Context, bool) error {
			close(started)
			<-release
			finished.Store(true)
			return nil
		},
	}, dispatcher.Dispatch, time.Now, nil)

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
		t.Fatal("Run returned before its in-flight action drained")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.True(t, finished.Load())
	assert.Equal(t, int64(1), dispatcher.started.Load())
}

func TestPrepareSoakRun_ReusesManifestDeadlineAfterRestart(t *testing.T) {
	firstStart := time.Unix(100, 0).UTC()
	store := seededLifecycleStoreAt("run-1", firstStart.Add(-time.Minute))

	first, err := prepareSoakRun(
		context.Background(),
		store,
		"run-1",
		time.Hour,
		firstStart,
	)
	require.NoError(t, err)
	assert.Equal(t, firstStart.Add(time.Hour), first.Deadline)
	assert.Equal(t, 0, first.RestartCount)

	restartAt := firstStart.Add(20 * time.Minute)
	second, err := prepareSoakRun(
		context.Background(),
		store,
		"run-1",
		time.Hour,
		restartAt,
	)
	require.NoError(t, err)
	assert.Equal(t, first.Deadline, second.Deadline)
	assert.Equal(t, 1, second.RestartCount)
	assert.Equal(t, 40*time.Minute, second.Deadline.Sub(restartAt))

	manifest := store.snapshot()
	require.NotNil(t, manifest.FirstStartedAt)
	require.NotNil(t, manifest.Deadline)
	assert.Equal(t, firstStart, *manifest.FirstStartedAt)
	assert.Equal(t, first.Deadline, *manifest.Deadline)
	assert.Equal(t, 1, manifest.RestartCount)
	assert.Equal(t, soakManifestRunning, manifest.State)
}

func TestSoakWorkload_ContinuousModeRunsUntilCancellationAndMarksStopped(
	t *testing.T,
) {
	store := seededLifecycleStore("run-1")
	store.manifest.RunMode = "continuous"
	dispatcher := &singleSoakDispatcher{}
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Continuous: true, Warmup: 0,
		SendRate: 1, MaxInFlight: 1,
	}, store, soakWorkloadActions{
		Send: func(context.Context, bool) error { return nil },
	}, dispatcher.Dispatch, time.Now, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var result soakWorkloadResult
	var runErr error
	go func() {
		result, runErr = workload.Run(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool {
		return dispatcher.started.Load() == 1
	}, time.Second, time.Millisecond)
	cancel()
	<-done

	assert.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, soakCompletionCanceled, result.Completion)
	assert.True(t, result.Deadline.IsZero())
	manifest := store.snapshot()
	assert.Equal(t, soakManifestStopped, manifest.State)
	assert.Nil(t, manifest.Deadline)
	require.NotNil(t, manifest.LastStoppedAt)
}

func TestPrepareContinuousSoakRun_ResumesStoppedManifestWithoutDeadline(
	t *testing.T,
) {
	firstStart := time.Unix(100, 0).UTC()
	store := seededLifecycleStoreAt("run-1", firstStart.Add(-time.Minute))
	store.manifest.RunMode = "continuous"

	first, err := prepareContinuousSoakRun(
		context.Background(),
		store,
		"run-1",
		firstStart,
	)
	require.NoError(t, err)
	assert.True(t, first.Deadline.IsZero())
	assert.Equal(t, 0, first.RestartCount)

	require.NoError(t, stopSoakRun(
		context.Background(),
		store,
		"run-1",
		firstStart.Add(time.Minute),
	))
	second, err := prepareContinuousSoakRun(
		context.Background(),
		store,
		"run-1",
		firstStart.Add(2*time.Minute),
	)
	require.NoError(t, err)
	assert.True(t, second.Deadline.IsZero())
	assert.Equal(t, 1, second.RestartCount)

	manifest := store.snapshot()
	assert.Equal(t, soakManifestRunning, manifest.State)
	assert.Nil(t, manifest.Deadline)
	assert.Zero(t, manifest.ConfiguredDuration)
	assert.Equal(t, 1, manifest.RestartCount)
}

func TestSoakManifestProjection_IncludesLifecycleAndOwnershipFields(t *testing.T) {
	projection := soakManifestProjection()
	fields := make(map[string]any, len(projection))
	for _, element := range projection {
		fields[element.Key] = element.Value
	}
	for _, field := range []string{
		"_id", "state", "siteId", "mongoDatabase", "cassandraKeyspace",
		"configDigest", "borrowedUserCount", "activeUserCount", "roomCount",
		"subscriptionCount", "startedAt", "updatedAt", "seededAt",
		"cleanedAt", "firstStartedAt", "deadline", "completedAt",
		"configuredDuration", "restartCount", "runMode", "lastStoppedAt",
		"lastHeartbeatAt",
	} {
		assert.Equal(t, 1, fields[field], field)
	}
	assert.Len(t, fields, 22)
}

func TestSoakWorkload_AlreadyElapsedDeadlineCompletesWithoutDispatch(t *testing.T) {
	now := time.Now().UTC()
	store := seededLifecycleStoreAt("run-1", now.Add(-time.Hour))
	deadline := now.Add(-time.Second)
	store.manifest.State = soakManifestRunning
	store.manifest.FirstStartedAt = ptrTime(now.Add(-time.Hour))
	store.manifest.Deadline = &deadline
	store.manifest.RestartCount = 1
	dispatcher := &recordingSoakDispatcher{}
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: time.Hour, SendRate: 1, MaxInFlight: 1,
	}, store, soakWorkloadActions{
		Send: func(context.Context, bool) error {
			t.Fatal("elapsed run must not dispatch")
			return nil
		},
	}, dispatcher.Dispatch, func() time.Time { return now }, nil)

	result, err := workload.Run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, soakCompletionConfiguredDuration, result.Completion)
	assert.Empty(t, dispatcher.rates())
	manifest := store.snapshot()
	assert.Equal(t, soakManifestCompleted, manifest.State)
	require.NotNil(t, manifest.CompletedAt)
}

func TestSoakWorkload_DependencyFailureRemainsRestartable(t *testing.T) {
	dependencyErr := errors.New("dependency unavailable")
	dispatcher := &singleSoakDispatcher{}
	store := seededLifecycleStore("run-1")
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Duration: time.Hour, SendRate: 1,
		MaxInFlight: 1, StopOnActionError: true,
	}, store, soakWorkloadActions{
		Send: func(context.Context, bool) error { return dependencyErr },
	}, dispatcher.Dispatch, time.Now, nil)

	result, err := workload.Run(context.Background())
	assert.ErrorIs(t, err, dependencyErr)
	assert.Equal(t, soakCompletionDependencyFailure, result.Completion)
	assert.Equal(t, soakManifestRunning, store.snapshot().State)
}

func TestSoakWorkload_RestartBeginsWithEmptyCatalog(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	firstCatalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	assert.Equal(t, 1, firstCatalog.Size())

	restartedCatalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	assert.Zero(t, restartedCatalog.Size())
	_, ok := restartedCatalog.Get("room-1", "message-1")
	assert.False(t, ok, "manifest lifecycle never checkpoints recent messages")
}

func TestRunSoakHeartbeat_UpdatesEveryTick(t *testing.T) {
	store := seededLifecycleStore("run-1")
	store.manifest.State = soakManifestRunning
	ticks := make(chan time.Time, 2)
	first := time.Unix(100, 0).UTC()
	second := first.Add(30 * time.Second)
	ticks <- first
	ticks <- second
	close(ticks)

	require.NoError(t, runSoakHeartbeat(
		context.Background(),
		store,
		"run-1",
		ticks,
	))

	assert.Equal(t, []time.Time{first, second}, store.heartbeats)
	assert.Equal(t, second, *store.snapshot().LastHeartbeatAt)
}

func TestRunSoakHeartbeat_StopsOnCancellationAndStoreFailure(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := runSoakHeartbeat(
			ctx,
			seededLifecycleStore("run-1"),
			"run-1",
			make(chan time.Time),
		)

		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("store failure", func(t *testing.T) {
		wantErr := errors.New("mongo unavailable")
		store := seededLifecycleStore("run-1")
		store.manifest.State = soakManifestRunning
		store.heartbeatErr = wantErr
		ticks := make(chan time.Time, 1)
		ticks <- time.Now()

		err := runSoakHeartbeat(
			context.Background(),
			store,
			"run-1",
			ticks,
		)

		require.Error(t, err)
		assert.ErrorIs(t, err, wantErr)
	})
}

type recordingSoakDispatcher struct {
	mu           sync.Mutex
	seenRates    map[string]int
	callsPerLane int
}

func (d *recordingSoakDispatcher) Dispatch(
	ctx context.Context,
	lane string,
	rate int,
	_ int,
	_ func(int),
	_ func(),
	action func(context.Context),
) {
	d.mu.Lock()
	if d.seenRates == nil {
		d.seenRates = make(map[string]int)
	}
	d.seenRates[lane] = rate
	calls := d.callsPerLane
	if calls == 0 {
		calls = 1
	}
	d.mu.Unlock()
	for range calls {
		action(ctx)
	}
}

func (d *recordingSoakDispatcher) rates() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	cloned := make(map[string]int, len(d.seenRates))
	for lane, rate := range d.seenRates {
		cloned[lane] = rate
	}
	return cloned
}

type burstSoakDispatcher struct {
	burst int
}

func (d *burstSoakDispatcher) Dispatch(
	ctx context.Context,
	_ string,
	_ int,
	_ int,
	_ func(int),
	_ func(),
	action func(context.Context),
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

type singleSoakDispatcher struct {
	started atomic.Int64
}

func (d *singleSoakDispatcher) Dispatch(
	ctx context.Context,
	_ string,
	_ int,
	_ int,
	_ func(int),
	_ func(),
	action func(context.Context),
) {
	d.started.Add(1)
	action(ctx)
	<-ctx.Done()
}

type fakeSoakLifecycleStore struct {
	mu           sync.Mutex
	manifest     soakManifest
	heartbeats   []time.Time
	heartbeatErr error
}

func (s *fakeSoakLifecycleStore) TouchHeartbeat(
	_ context.Context,
	runID string,
	at time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.heartbeatErr != nil {
		return s.heartbeatErr
	}
	if s.manifest.ID != runID || s.manifest.State != soakManifestRunning {
		return errors.New("manifest is not running")
	}
	heartbeat := at.UTC()
	s.manifest.LastHeartbeatAt = &heartbeat
	s.manifest.UpdatedAt = heartbeat
	s.heartbeats = append(s.heartbeats, heartbeat)
	return nil
}

func (s *fakeSoakLifecycleStore) GetManifest(
	_ context.Context,
	runID string,
) (*soakManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manifest.ID != runID {
		return nil, errSoakManifestNotFound
	}
	cloned := s.manifest
	return &cloned, nil
}

func (s *fakeSoakLifecycleStore) PutManifest(
	_ context.Context,
	manifest *soakManifest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifest = *manifest
	return nil
}

func (s *fakeSoakLifecycleStore) snapshot() soakManifest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifest
}

func seededLifecycleStore(runID string) *fakeSoakLifecycleStore {
	return seededLifecycleStoreAt(runID, time.Now().UTC())
}

func seededLifecycleStoreAt(
	runID string,
	seededAt time.Time,
) *fakeSoakLifecycleStore {
	return &fakeSoakLifecycleStore{manifest: soakManifest{
		ID: runID, State: soakManifestSeeded,
		StartedAt: seededAt, UpdatedAt: seededAt,
	}}
}

func countSoakAction(
	calls *sync.Map,
	name string,
) soakWorkloadAction {
	return func(context.Context, bool) error {
		value, _ := calls.LoadOrStore(name, &atomic.Int64{})
		value.(*atomic.Int64).Add(1)
		return nil
	}
}

func sequenceSoakNow(values ...time.Time) func() time.Time {
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

func ptrTime(value time.Time) *time.Time {
	return &value
}
