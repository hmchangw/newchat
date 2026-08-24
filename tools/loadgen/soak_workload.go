package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	soakHeartbeatAttemptTimeout = 5 * time.Second
	soakHeartbeatRetryInterval  = 5 * time.Second
)

type soakWorkloadAction func(context.Context, bool) error

type soakWorkloadActions struct {
	Send           soakWorkloadAction
	Read           soakWorkloadAction
	Mutation       soakWorkloadAction
	Reaction       soakWorkloadAction
	PinnedList     soakWorkloadAction
	Verify         soakWorkloadAction
	MemberMutation soakWorkloadAction
	RoomMutation   soakWorkloadAction
	RoomRead       soakWorkloadAction
	UserRead       soakWorkloadAction
	SearchRead     soakWorkloadAction
	RoomCreate     soakWorkloadAction
	ReadReceipt    soakWorkloadAction
	Presence       soakWorkloadAction
}

type soakWorkloadConfig struct {
	RunID               string
	Duration            time.Duration
	Continuous          bool
	Warmup              time.Duration
	HeartbeatInterval   time.Duration
	HeartbeatStaleAfter time.Duration
	SendRate            float64
	ReadRate            float64
	MutationRate        float64
	ReactionRate        float64
	PinnedListRate      float64
	VerifyRate          float64
	MemberMutationRate  float64
	RoomMutationRate    float64
	RoomReadRate        float64
	UserReadRate        float64
	SearchReadRate      float64
	RoomCreateRate      float64
	ReadReceiptRate     float64
	PresenceRate        float64
	MaxInFlight         int
	StopOnActionError   bool
}

// soakWorkloadConfigFrom maps the parsed environment onto the workload's lane
// rates. It exists as a named function rather than an inline literal because a
// lane whose rate is never mapped through is skipped by lanes() and sends
// nothing, while still reporting a configured target — a silent hole that only
// a test over this mapping can catch.
func soakWorkloadConfigFrom(cfg *soakConfig, maxInFlight int) *soakWorkloadConfig {
	return &soakWorkloadConfig{
		RunID:               cfg.RunID,
		Duration:            cfg.RunDuration,
		Continuous:          cfg.RunMode == soakRunModeContinuous,
		Warmup:              cfg.Warmup,
		HeartbeatInterval:   cfg.HeartbeatInterval,
		HeartbeatStaleAfter: cfg.HeartbeatStaleAfter,
		SendRate:            cfg.SendRate,
		ReadRate:            cfg.ReadRate,
		MutationRate:        cfg.MutationRate,
		ReactionRate:        cfg.ReactionRate,
		PinnedListRate:      cfg.PinnedListRate,
		VerifyRate:          cfg.VerifyRate,
		MemberMutationRate:  cfg.MemberMutationRate,
		RoomMutationRate:    cfg.RoomMutationRate,
		RoomReadRate:        cfg.RoomReadRate,
		UserReadRate:        cfg.UserReadRate,
		SearchReadRate:      cfg.SearchReadRate,
		RoomCreateRate:      cfg.RoomCreateRate,
		ReadReceiptRate:     cfg.ReadReceiptRate,
		PresenceRate:        cfg.PresenceRate,
		MaxInFlight:         maxInFlight,
	}
}

type soakCompletion string

const (
	soakCompletionConfiguredDuration soakCompletion = "configured_duration"
	soakCompletionCanceled           soakCompletion = "canceled"
	soakCompletionDependencyFailure  soakCompletion = "dependency_failure"
)

type soakWorkloadResult struct {
	Completion   soakCompletion
	Deadline     time.Time
	RestartCount int
	LeaseAbort   bool
}

type soakRunWindow struct {
	Deadline        time.Time
	LastHeartbeatAt time.Time
	RestartCount    int
}

type soakLifecycleStore interface {
	GetManifest(context.Context, string) (*soakManifest, error)
	PutManifest(context.Context, *soakManifest) error
	TouchHeartbeat(context.Context, string, time.Time) error
}

var (
	errSoakManifestNotFound      = errors.New("soak run manifest not found")
	errSoakRunNotActive          = errors.New("soak run is not active")
	errSoakHeartbeatLeaseInvalid = errors.New("soak heartbeat lease configuration is invalid")
	errSoakHeartbeatLeaseAtRisk  = errors.New("soak heartbeat lease can no longer be renewed safely")
)

type soakHeartbeatOutcome string

const (
	soakHeartbeatSuccess   soakHeartbeatOutcome = "success"
	soakHeartbeatError     soakHeartbeatOutcome = "error"
	soakHeartbeatNotActive soakHeartbeatOutcome = "not_active"
)

type soakHeartbeatObserver interface {
	RecordHeartbeatAttempt(soakHeartbeatOutcome, bool, time.Time)
}

type soakLaneDispatcher func(
	ctx context.Context,
	lane string,
	rate float64,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	action func(context.Context),
)

type soakPacingOutcome string

const (
	soakPacingDispatched        soakPacingOutcome = "dispatched"
	soakPacingSchedulerUnderrun soakPacingOutcome = "scheduler_underrun"
	soakPacingLaneSaturation    soakPacingOutcome = "lane_saturation"
	soakPacingGlobalSaturation  soakPacingOutcome = "global_saturation"
)

type soakPacingMetrics struct {
	metrics *Metrics
}

func newSoakPacingMetrics(metrics *Metrics) *soakPacingMetrics {
	return &soakPacingMetrics{metrics: metrics}
}

func (r *soakPacingMetrics) Configure(lane string, rate float64) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.SoakConfiguredRate.WithLabelValues(lane).Set(rate)
}

func (r *soakPacingMetrics) Record(lane string, outcome soakPacingOutcome, count int) {
	if r == nil || r.metrics == nil || count <= 0 {
		return
	}
	value := float64(count)
	var recordOutcome func()
	switch outcome {
	case soakPacingDispatched:
		recordOutcome = func() { r.metrics.SoakDispatched.WithLabelValues(lane).Add(value) }
	case soakPacingSchedulerUnderrun:
		recordOutcome = func() { r.metrics.SoakSchedulerUnderrun.WithLabelValues(lane).Add(value) }
	case soakPacingLaneSaturation:
		recordOutcome = func() { r.metrics.SoakLaneSaturation.WithLabelValues(lane).Add(value) }
	case soakPacingGlobalSaturation:
		recordOutcome = func() { r.metrics.SoakGlobalSaturation.WithLabelValues(lane).Add(value) }
	default:
		return
	}
	r.metrics.SoakIntended.WithLabelValues(lane).Add(value)
	recordOutcome()
}

type soakWorkloadOption func(*soakWorkload)

func withSoakPacingMetrics(recorder *soakPacingMetrics) soakWorkloadOption {
	return func(workload *soakWorkload) { workload.pacing = recorder }
}

func withSoakHeartbeatObserver(observer soakHeartbeatObserver) soakWorkloadOption {
	return func(workload *soakWorkload) { workload.heartbeatObserver = observer }
}

func withSoakFailureInvalidation(invalidate func(string)) soakWorkloadOption {
	return func(workload *soakWorkload) { workload.invalidateFailure = invalidate }
}

type soakWorkload struct {
	cfg               soakWorkloadConfig
	store             soakLifecycleStore
	actions           soakWorkloadActions
	dispatch          soakLaneDispatcher
	now               func() time.Time
	onSaturation      func()
	pacing            *soakPacingMetrics
	heartbeatObserver soakHeartbeatObserver
	invalidateFailure func(string)
}

func newSoakWorkload(
	cfg *soakWorkloadConfig,
	store soakLifecycleStore,
	actions *soakWorkloadActions,
	dispatch soakLaneDispatcher,
	now func() time.Time,
	onSaturation func(),
	options ...soakWorkloadOption,
) *soakWorkload {
	if cfg == nil {
		cfg = &soakWorkloadConfig{}
	}
	config := *cfg
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = 256
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.HeartbeatStaleAfter <= 0 {
		minimumStaleAfter, _ := minimumSoakHeartbeatStaleAfter(
			config.HeartbeatInterval,
			soakHeartbeatAttemptTimeout,
		)
		config.HeartbeatStaleAfter = max(2*time.Minute, minimumStaleAfter)
	}
	if dispatch == nil {
		dispatch = dispatchSoakLane
	}
	if now == nil {
		now = time.Now
	}
	if onSaturation == nil {
		onSaturation = func() {}
	}
	if actions == nil {
		actions = &soakWorkloadActions{}
	}
	workload := &soakWorkload{
		cfg: config, store: store, actions: *actions, dispatch: dispatch,
		now: now, onSaturation: onSaturation,
	}
	for _, option := range options {
		option(workload)
	}
	return workload
}

func (w *soakWorkload) Run(
	ctx context.Context,
) (soakWorkloadResult, error) {
	var (
		window soakRunWindow
		err    error
	)
	if w.cfg.Continuous {
		window, err = prepareContinuousSoakRun(
			ctx,
			w.store,
			w.cfg.RunID,
			w.now().UTC(),
		)
	} else {
		window, err = prepareSoakRun(
			ctx,
			w.store,
			w.cfg.RunID,
			w.cfg.Duration,
			w.now().UTC(),
		)
	}
	if err != nil {
		return soakWorkloadResult{}, err
	}
	result := soakWorkloadResult{
		Deadline:     window.Deadline,
		RestartCount: window.RestartCount,
	}
	if !w.cfg.Continuous && !w.now().Before(window.Deadline) {
		if err := completeSoakRun(
			ctx,
			w.store,
			w.cfg.RunID,
			w.now().UTC(),
		); err != nil {
			return result, err
		}
		result.Completion = soakCompletionConfiguredDuration
		return result, nil
	}

	var (
		runCtx context.Context
		cancel context.CancelFunc
	)
	if w.cfg.Continuous {
		runCtx, cancel = context.WithCancel(ctx)
	} else {
		runCtx, cancel = context.WithDeadline(ctx, window.Deadline)
	}
	defer cancel()
	processStartedAt := w.now()
	warmupDeadline := processStartedAt.Add(w.cfg.Warmup)
	globalBudget := make(chan struct{}, w.cfg.MaxInFlight)

	var (
		fatalMu  sync.Mutex
		fatalErr error
		laneWG   sync.WaitGroup
	)
	laneActivity := newSoakLaneActivity()
	setFatal := func(err error) {
		if err == nil || !w.cfg.StopOnActionError {
			return
		}
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
			cancel()
		}
		fatalMu.Unlock()
	}
	heartbeatDone := make(chan error, 1)
	heartbeatTicker := time.NewTicker(w.cfg.HeartbeatInterval)
	go func() {
		defer heartbeatTicker.Stop()
		heartbeatErr := runSoakHeartbeat(
			runCtx,
			w.store,
			w.cfg.RunID,
			heartbeatTicker.C,
			soakHeartbeatAttemptTimeout,
			soakHeartbeatRetryInterval,
			w.cfg.HeartbeatStaleAfter,
			w.cfg.HeartbeatInterval,
			window.LastHeartbeatAt,
			nil,
			w.now,
			w.heartbeatObserver,
		)
		if heartbeatErr != nil &&
			!errors.Is(heartbeatErr, context.Canceled) &&
			!errors.Is(heartbeatErr, context.DeadlineExceeded) {
			fatalMu.Lock()
			if fatalErr == nil {
				fatalErr = heartbeatErr
				cancel()
			}
			fatalMu.Unlock()
		}
		heartbeatDone <- heartbeatErr
	}()
	lanes := w.lanes()
	for _, lane := range lanes {
		if lane.action == nil || lane.rate <= 0 {
			continue
		}
		lane := lane
		w.pacing.Configure(lane.name, lane.rate)
		laneWG.Add(1)
		go func() {
			defer laneWG.Done()
			w.dispatch(
				runCtx,
				lane.name,
				lane.rate,
				w.cfg.MaxInFlight,
				func(count int) {
					w.pacing.Record(lane.name, soakPacingSchedulerUnderrun, count)
				},
				func() {
					w.pacing.Record(lane.name, soakPacingLaneSaturation, 1)
					w.onSaturation()
				},
				func(actionCtx context.Context) {
					select {
					case globalBudget <- struct{}{}:
					default:
						w.pacing.Record(lane.name, soakPacingGlobalSaturation, 1)
						w.onSaturation()
						return
					}
					w.pacing.Record(lane.name, soakPacingDispatched, 1)
					defer func() { <-globalBudget }()
					finishActivity := laneActivity.Start(lane.name)
					defer finishActivity()
					measured := !w.now().Before(warmupDeadline)
					setFatal(lane.action(actionCtx, measured))
				},
			)
		}()
	}

	<-runCtx.Done()
	<-heartbeatDone
	fatalMu.Lock()
	dependencyErr := fatalErr
	fatalMu.Unlock()
	if errors.Is(dependencyErr, errSoakHeartbeatLeaseAtRisk) {
		drainBudget := w.cfg.HeartbeatInterval / 2
		if !waitSoakLaneDrain(&laneWG, drainBudget) {
			activeLanes, inFlight := laneActivity.Snapshot()
			slog.Error(
				"soak lane drain exceeded lease safety budget",
				"drainBudget", drainBudget,
				"inFlight", inFlight,
				"lanes", activeLanes,
			)
			invalidationBudget := drainBudget / 2
			if !waitSoakFailureInvalidation(
				w.invalidateFailure,
				invalidReasonLeaseAbort,
				invalidationBudget,
			) {
				slog.Error(
					"soak lease-abort invalidation exceeded safety budget",
					"invalidationBudget", invalidationBudget,
					"consequence", "the process will exit even if the invalidation did not reach the WAL",
				)
			}
			result.Completion = soakCompletionDependencyFailure
			result.LeaseAbort = true
			return result, dependencyErr
		}
	} else {
		laneWG.Wait()
	}
	switch {
	case dependencyErr != nil:
		result.Completion = soakCompletionDependencyFailure
		return result, dependencyErr
	case ctx.Err() != nil:
		result.Completion = soakCompletionCanceled
		if w.cfg.Continuous {
			stopCtx, cancelStop := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancelStop()
			if err := stopSoakRun(
				stopCtx,
				w.store,
				w.cfg.RunID,
				w.now().UTC(),
			); err != nil {
				return result, err
			}
		}
		return result, ctx.Err()
	default:
		completeCtx, cancelComplete := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancelComplete()
		if err := completeSoakRun(
			completeCtx,
			w.store,
			w.cfg.RunID,
			w.now().UTC(),
		); err != nil {
			return result, err
		}
		result.Completion = soakCompletionConfiguredDuration
		return result, nil
	}
}

type soakLane struct {
	name   string
	rate   float64
	action soakWorkloadAction
}

type soakLaneActivity struct {
	mu     sync.Mutex
	active map[string]int
}

func newSoakLaneActivity() *soakLaneActivity {
	return &soakLaneActivity{active: make(map[string]int)}
}

func (a *soakLaneActivity) Start(lane string) func() {
	a.mu.Lock()
	a.active[lane]++
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.active[lane]--
		if a.active[lane] == 0 {
			delete(a.active, lane)
		}
		a.mu.Unlock()
	}
}

func (a *soakLaneActivity) Snapshot() (map[string]int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	active := make(map[string]int, len(a.active))
	total := 0
	for lane, count := range a.active {
		active[lane] = count
		total += count
	}
	return active, total
}

func waitSoakLaneDrain(laneWG *sync.WaitGroup, budget time.Duration) bool {
	done := make(chan struct{})
	go func() {
		laneWG.Wait()
		close(done)
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func waitSoakFailureInvalidation(
	invalidate func(string),
	reason string,
	budget time.Duration,
) bool {
	if invalidate == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		invalidate(reason)
		close(done)
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (w *soakWorkload) lanes() []soakLane {
	return []soakLane{
		{name: "send", rate: w.cfg.SendRate, action: w.actions.Send},
		{name: "read", rate: w.cfg.ReadRate, action: w.actions.Read},
		{name: "mutation", rate: w.cfg.MutationRate, action: w.actions.Mutation},
		{name: "reaction", rate: w.cfg.ReactionRate, action: w.actions.Reaction},
		{name: "pinned_list", rate: w.cfg.PinnedListRate, action: w.actions.PinnedList},
		{name: "verify", rate: w.cfg.VerifyRate, action: w.actions.Verify},
		{
			name: soakFailureLaneMemberMutation, rate: w.cfg.MemberMutationRate,
			action: w.actions.MemberMutation,
		},
		{
			name: soakFailureLaneRoomMutation, rate: w.cfg.RoomMutationRate,
			action: w.actions.RoomMutation,
		},
		{name: "room_read", rate: w.cfg.RoomReadRate, action: w.actions.RoomRead},
		{name: "user_read", rate: w.cfg.UserReadRate, action: w.actions.UserRead},
		{name: "search_read", rate: w.cfg.SearchReadRate, action: w.actions.SearchRead},
		{
			name: soakFailureLaneRoomCreate, rate: w.cfg.RoomCreateRate,
			action: w.actions.RoomCreate,
		},
		{
			name: soakFailureLaneReadReceipt, rate: w.cfg.ReadReceiptRate,
			action: w.actions.ReadReceipt,
		},
		{name: "presence", rate: w.cfg.PresenceRate, action: w.actions.Presence},
	}
}

func dispatchSoakLane(
	ctx context.Context,
	_ string,
	rate float64,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	action func(context.Context),
) {
	pacedDispatchRate(
		ctx,
		rate,
		maxInFlight,
		recordUnderrun,
		recordSaturation,
		action,
	)
}

func prepareSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	duration time.Duration,
	now time.Time,
) (soakRunWindow, error) {
	if store == nil {
		return soakRunWindow{}, fmt.Errorf("soak lifecycle store is required")
	}
	if runID == "" {
		return soakRunWindow{}, fmt.Errorf("soak run ID is required")
	}
	if duration <= 0 {
		return soakRunWindow{}, fmt.Errorf("soak duration must be greater than zero")
	}
	manifest, err := store.GetManifest(ctx, runID)
	if err != nil {
		return soakRunWindow{}, fmt.Errorf("load soak lifecycle: %w", err)
	}
	if manifest == nil {
		return soakRunWindow{}, errSoakManifestNotFound
	}
	if manifest.State != soakManifestSeeded &&
		manifest.State != soakManifestRunning &&
		manifest.State != soakManifestCompleted {
		return soakRunWindow{}, fmt.Errorf(
			"soak manifest state %q cannot run",
			manifest.State,
		)
	}
	if manifest.RunMode == soakRunModeContinuous {
		return soakRunWindow{}, fmt.Errorf(
			"soak manifest run mode %q cannot use a duration deadline",
			manifest.RunMode,
		)
	}

	if manifest.Deadline == nil {
		firstStartedAt := now.UTC()
		deadline := firstStartedAt.Add(duration)
		manifest.FirstStartedAt = &firstStartedAt
		manifest.Deadline = &deadline
		manifest.ConfiguredDuration = duration
		manifest.RestartCount = 0
	} else if manifest.State == soakManifestRunning {
		manifest.RestartCount++
	}
	manifest.State = soakManifestRunning
	heartbeat := now.UTC()
	manifest.LastHeartbeatAt = &heartbeat
	manifest.UpdatedAt = heartbeat
	if err := store.PutManifest(ctx, manifest); err != nil {
		return soakRunWindow{}, fmt.Errorf("mark soak run running: %w", err)
	}
	return soakRunWindow{
		Deadline:        *manifest.Deadline,
		LastHeartbeatAt: heartbeat,
		RestartCount:    manifest.RestartCount,
	}, nil
}

func prepareContinuousSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) (soakRunWindow, error) {
	if store == nil {
		return soakRunWindow{}, fmt.Errorf("soak lifecycle store is required")
	}
	if runID == "" {
		return soakRunWindow{}, fmt.Errorf("soak run ID is required")
	}
	manifest, err := store.GetManifest(ctx, runID)
	if err != nil {
		return soakRunWindow{}, fmt.Errorf("load continuous soak lifecycle: %w", err)
	}
	if manifest == nil {
		return soakRunWindow{}, errSoakManifestNotFound
	}
	if manifest.RunMode != soakRunModeContinuous {
		return soakRunWindow{}, fmt.Errorf(
			"soak manifest run mode %q is not continuous",
			manifest.RunMode,
		)
	}
	if manifest.State != soakManifestSeeded &&
		manifest.State != soakManifestRunning &&
		manifest.State != soakManifestStopped {
		return soakRunWindow{}, fmt.Errorf(
			"continuous soak manifest state %q cannot run",
			manifest.State,
		)
	}

	startedAt := now.UTC()
	if manifest.FirstStartedAt == nil {
		manifest.FirstStartedAt = &startedAt
		manifest.RestartCount = 0
	} else {
		manifest.RestartCount++
	}
	manifest.State = soakManifestRunning
	manifest.Deadline = nil
	manifest.ConfiguredDuration = 0
	manifest.LastStoppedAt = nil
	manifest.LastHeartbeatAt = &startedAt
	manifest.UpdatedAt = startedAt
	if err := store.PutManifest(ctx, manifest); err != nil {
		return soakRunWindow{}, fmt.Errorf(
			"mark continuous soak run running: %w",
			err,
		)
	}
	return soakRunWindow{
		LastHeartbeatAt: startedAt,
		RestartCount:    manifest.RestartCount,
	}, nil
}

func stopSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) error {
	if store == nil {
		return fmt.Errorf("soak lifecycle store is required")
	}
	if runID == "" {
		return fmt.Errorf("soak run ID is required")
	}
	manifest, err := store.GetManifest(ctx, runID)
	if err != nil {
		return fmt.Errorf("load soak manifest for stop: %w", err)
	}
	if manifest == nil {
		return errSoakManifestNotFound
	}
	if manifest.State == soakManifestStopped {
		return nil
	}
	if manifest.State != soakManifestRunning {
		return fmt.Errorf("soak manifest state %q cannot stop", manifest.State)
	}
	stoppedAt := now.UTC()
	manifest.State = soakManifestStopped
	manifest.LastStoppedAt = &stoppedAt
	manifest.UpdatedAt = stoppedAt
	if err := store.PutManifest(ctx, manifest); err != nil {
		return fmt.Errorf("mark soak run stopped: %w", err)
	}
	return nil
}

type soakHeartbeatRetryWait func(context.Context, time.Duration) error

func minimumSoakHeartbeatStaleAfter(
	heartbeatInterval time.Duration,
	attemptTimeout time.Duration,
) (time.Duration, bool) {
	const maxDuration = time.Duration(1<<63 - 1)
	if heartbeatInterval <= 0 || attemptTimeout <= 0 ||
		heartbeatInterval > (maxDuration-attemptTimeout)/2 {
		return 0, false
	}
	return 2*heartbeatInterval + attemptTimeout, true
}

func waitSoakHeartbeatRetry(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runSoakHeartbeat(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	healthyTicks <-chan time.Time,
	attemptTimeout time.Duration,
	retryInterval time.Duration,
	staleAfter time.Duration,
	shutdownMargin time.Duration,
	lastSuccessAt time.Time,
	waitRetry soakHeartbeatRetryWait,
	now func() time.Time,
	observer soakHeartbeatObserver,
) error {
	if attemptTimeout <= 0 {
		attemptTimeout = soakHeartbeatAttemptTimeout
	}
	if now == nil {
		now = time.Now
	}
	if retryInterval <= 0 {
		retryInterval = soakHeartbeatRetryInterval
	}
	if waitRetry == nil {
		waitRetry = waitSoakHeartbeatRetry
	}
	minimumStaleAfter, validLeaseDurations := minimumSoakHeartbeatStaleAfter(
		shutdownMargin,
		attemptTimeout,
	)
	if !validLeaseDurations || staleAfter < minimumStaleAfter {
		return fmt.Errorf(
			"%w: stale threshold must be at least twice the shutdown margin plus the attempt timeout",
			errSoakHeartbeatLeaseInvalid,
		)
	}
	if lastSuccessAt.IsZero() {
		return fmt.Errorf("soak heartbeat lease requires the last persisted heartbeat time")
	}
	lastSuccessAt = lastSuccessAt.UTC()
	degraded := false
	var degradedAt time.Time
	for healthyTicks != nil || degraded {
		var at time.Time
		if degraded {
			stopAt := lastSuccessAt.Add(staleAfter - shutdownMargin)
			current := now().UTC()
			if !current.Before(stopAt) {
				return soakHeartbeatLeaseError(lastSuccessAt, stopAt)
			}
			wait := min(retryInterval, stopAt.Sub(current))
			if err := waitRetry(ctx, wait); err != nil {
				return err
			}
			at = now().UTC()
			if !at.Before(stopAt) {
				return soakHeartbeatLeaseError(lastSuccessAt, stopAt)
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case tick, ok := <-healthyTicks:
				if !ok {
					healthyTicks = nil
					continue
				}
				at = tick
			}
		}

		stopAt := lastSuccessAt.Add(staleAfter - shutdownMargin)
		remaining := stopAt.Sub(now().UTC())
		if remaining <= 0 {
			return soakHeartbeatLeaseError(lastSuccessAt, stopAt)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, min(attemptTimeout, remaining))
		err := store.TouchHeartbeat(attemptCtx, runID, at.UTC())
		cancel()
		completedAt := now().UTC()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, errSoakRunNotActive):
			recordSoakHeartbeatAttempt(
				observer, soakHeartbeatNotActive, false, completedAt,
			)
			return fmt.Errorf("update soak heartbeat: %w", err)
		case err != nil:
			recordSoakHeartbeatAttempt(observer, soakHeartbeatError, true, completedAt)
			if !degraded {
				degraded = true
				degradedAt = completedAt
				slog.Warn("soak heartbeat entered degraded state",
					"runId", runID, "error", err)
			}
			continue
		default:
			recordSoakHeartbeatAttempt(observer, soakHeartbeatSuccess, false, completedAt)
			lastSuccessAt = at.UTC()
			if degraded {
				slog.Info("soak heartbeat recovered",
					"runId", runID, "degradedDuration", completedAt.Sub(degradedAt))
				if !drainSoakHeartbeatTicks(healthyTicks) {
					healthyTicks = nil
				}
			}
			degraded = false
		}
	}
	return nil
}

func soakHeartbeatLeaseError(lastSuccessAt, stopAt time.Time) error {
	return fmt.Errorf(
		"%w: last persisted heartbeat %s; workload stop boundary %s",
		errSoakHeartbeatLeaseAtRisk,
		lastSuccessAt.Format(time.RFC3339Nano),
		stopAt.Format(time.RFC3339Nano),
	)
}

// drainSoakHeartbeatTicks discards the stale healthy tick a time.Ticker may
// buffer while retries own the schedule, preventing an immediate second
// heartbeat after recovery.
func drainSoakHeartbeatTicks(ticks <-chan time.Time) bool {
	for {
		select {
		case _, ok := <-ticks:
			if !ok {
				return false
			}
		default:
			return true
		}
	}
}

func recordSoakHeartbeatAttempt(
	observer soakHeartbeatObserver,
	outcome soakHeartbeatOutcome,
	degraded bool,
	completedAt time.Time,
) {
	if observer != nil {
		observer.RecordHeartbeatAttempt(outcome, degraded, completedAt)
	}
}

func completeSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) error {
	manifest, err := store.GetManifest(ctx, runID)
	if err != nil {
		return fmt.Errorf("load soak manifest for completion: %w", err)
	}
	if manifest == nil {
		return errSoakManifestNotFound
	}
	completedAt := now.UTC()
	manifest.State = soakManifestCompleted
	manifest.CompletedAt = &completedAt
	manifest.UpdatedAt = completedAt
	if err := store.PutManifest(ctx, manifest); err != nil {
		return fmt.Errorf("mark soak run completed: %w", err)
	}
	return nil
}
