package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	soakrun "github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
	soakworkload "github.com/hmchangw/chat/tools/loadgen/internal/soak/workload"
)

const (
	soakHeartbeatAttemptTimeout = soakworkload.HeartbeatAttemptTimeout
	soakHeartbeatRetryInterval  = 5 * time.Second
)

type soakWorkloadAction = soakworkload.Action
type soakWorkloadActions = soakworkload.Actions
type soakWorkloadConfig = soakworkload.Config

const (
	soakCompletionConfiguredDuration = soakworkload.CompletionConfiguredDuration
	soakCompletionCanceled           = soakworkload.CompletionCanceled
	soakCompletionDependencyFailure  = soakworkload.CompletionDependencyFailure
)

type soakWorkloadResult = soakworkload.Result
type soakRunWindow = soakworkload.RunWindow

//go:generate mockgen -destination=mock_soak_store_test.go -package=main . soakLifecycleStore

type soakLifecycleStore interface {
	GetManifest(context.Context, string) (*soakManifest, error)
	PutManifest(context.Context, *soakManifest) error
	TouchHeartbeat(context.Context, string, time.Time) error
}

var (
	errSoakManifestNotFound      = soakrun.ErrManifestNotFound
	errSoakRunNotActive          = soakrun.ErrRunNotActive
	errSoakHeartbeatLeaseInvalid = soakworkload.ErrHeartbeatLeaseInvalid
	errSoakHeartbeatLeaseAtRisk  = soakworkload.ErrHeartbeatLeaseAtRisk
)

type soakHeartbeatOutcome = soakworkload.HeartbeatOutcome

const (
	soakHeartbeatSuccess   = soakworkload.HeartbeatSuccess
	soakHeartbeatError     = soakworkload.HeartbeatError
	soakHeartbeatNotActive = soakworkload.HeartbeatNotActive
)

type soakHeartbeatObserver = soakworkload.HeartbeatObserver
type soakLaneDispatcher = soakworkload.Dispatcher
type soakPacingOutcome = soakworkload.PacingOutcome

const (
	soakPacingDispatched        = soakworkload.PacingDispatched
	soakPacingSchedulerUnderrun = soakworkload.PacingSchedulerUnderrun
	soakPacingLaneSaturation    = soakworkload.PacingLaneSaturation
	soakPacingGlobalSaturation  = soakworkload.PacingGlobalSaturation
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

type soakWorkloadOption = soakworkload.Option

func withSoakPacingMetrics(recorder *soakPacingMetrics) soakWorkloadOption {
	return soakworkload.WithPacingRecorder(recorder)
}

func withSoakHeartbeatObserver(observer soakHeartbeatObserver) soakWorkloadOption {
	return soakworkload.WithHeartbeatObserver(observer)
}

func withSoakFailureInvalidation(invalidate func(string)) soakWorkloadOption {
	if invalidate == nil {
		return soakworkload.WithFailureInvalidation(nil)
	}
	return soakworkload.WithFailureInvalidation(func() {
		invalidate(invalidReasonLeaseAbort)
	})
}

type soakWorkload struct {
	inner        *soakworkload.Workload
	cfg          soakWorkloadConfig
	actions      soakWorkloadActions
	dispatch     soakLaneDispatcher
	now          func() time.Time
	onSaturation func()
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
		minimum, _ := minimumSoakHeartbeatStaleAfter(
			config.HeartbeatInterval, soakHeartbeatAttemptTimeout,
		)
		config.HeartbeatStaleAfter = max(2*time.Minute, minimum)
	}
	if actions == nil {
		actions = &soakWorkloadActions{}
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
	lifecycle := &soakLifecycleAdapter{store: store}
	return &soakWorkload{
		inner: soakworkload.New(
			&config, lifecycle, actions, dispatch, now, onSaturation, options...,
		),
		cfg: config, actions: *actions, dispatch: dispatch,
		now: now, onSaturation: onSaturation,
	}
}

func (w *soakWorkload) Run(ctx context.Context) (soakWorkloadResult, error) {
	return w.inner.Run(ctx)
}

type soakLane struct {
	name   string
	rate   float64
	action soakWorkloadAction
}

func (w *soakWorkload) lanes() []soakLane {
	return []soakLane{
		{name: "send", rate: w.cfg.SendRate, action: w.actions.Send},
		{name: "read", rate: w.cfg.ReadRate, action: w.actions.Read},
		{name: "mutation", rate: w.cfg.MutationRate, action: w.actions.Mutation},
		{name: "reaction", rate: w.cfg.ReactionRate, action: w.actions.Reaction},
		{name: "pinned_list", rate: w.cfg.PinnedListRate, action: w.actions.PinnedList},
		{name: "verify", rate: w.cfg.VerifyRate, action: w.actions.Verify},
		{name: soakFailureLaneMemberMutation, rate: w.cfg.MemberMutationRate, action: w.actions.MemberMutation},
		{name: soakFailureLaneRoomMutation, rate: w.cfg.RoomMutationRate, action: w.actions.RoomMutation},
		{name: "room_read", rate: w.cfg.RoomReadRate, action: w.actions.RoomRead},
		{name: "user_read", rate: w.cfg.UserReadRate, action: w.actions.UserRead},
		{name: "search_read", rate: w.cfg.SearchReadRate, action: w.actions.SearchRead},
		{name: soakFailureLaneRoomCreate, rate: w.cfg.RoomCreateRate, action: w.actions.RoomCreate},
		{name: soakFailureLaneReadReceipt, rate: w.cfg.ReadReceiptRate, action: w.actions.ReadReceipt},
		{name: "presence", rate: w.cfg.PresenceRate, action: w.actions.Presence},
	}
}

// soakWorkloadConfigFrom maps the parsed environment onto every scheduler lane.
func soakWorkloadConfigFrom(cfg *soakConfig, maxInFlight int) *soakWorkloadConfig {
	return &soakWorkloadConfig{
		RunID: cfg.RunID, Duration: cfg.RunDuration,
		Continuous: cfg.RunMode == soakRunModeContinuous, Warmup: cfg.Warmup,
		HeartbeatInterval:   cfg.HeartbeatInterval,
		HeartbeatStaleAfter: cfg.HeartbeatStaleAfter,
		SendRate:            cfg.SendRate, ReadRate: cfg.ReadRate,
		MutationRate: cfg.MutationRate, ReactionRate: cfg.ReactionRate,
		PinnedListRate: cfg.PinnedListRate, VerifyRate: cfg.VerifyRate,
		MemberMutationRate: cfg.MemberMutationRate,
		RoomMutationRate:   cfg.RoomMutationRate, RoomReadRate: cfg.RoomReadRate,
		UserReadRate: cfg.UserReadRate, SearchReadRate: cfg.SearchReadRate,
		RoomCreateRate: cfg.RoomCreateRate, ReadReceiptRate: cfg.ReadReceiptRate,
		PresenceRate: cfg.PresenceRate, MaxInFlight: maxInFlight,
	}
}

type soakLifecycleAdapter struct {
	store soakLifecycleStore
}

func (a *soakLifecycleAdapter) Prepare(
	ctx context.Context,
	runID string,
	duration time.Duration,
	continuous bool,
	now time.Time,
) (soakworkload.RunWindow, error) {
	if continuous {
		return prepareContinuousSoakRun(ctx, a.store, runID, now)
	}
	return prepareSoakRun(ctx, a.store, runID, duration, now)
}

func (a *soakLifecycleAdapter) Complete(
	ctx context.Context, runID string, now time.Time,
) error {
	return completeSoakRun(ctx, a.store, runID, now)
}

func (a *soakLifecycleAdapter) Stop(
	ctx context.Context, runID string, now time.Time,
) error {
	return stopSoakRun(ctx, a.store, runID, now)
}

func (a *soakLifecycleAdapter) TouchHeartbeat(
	ctx context.Context, runID string, at time.Time,
) error {
	if a.store == nil {
		return fmt.Errorf("soak lifecycle store is required")
	}
	err := a.store.TouchHeartbeat(ctx, runID, at)
	if errors.Is(err, errSoakRunNotActive) {
		return fmt.Errorf("%w: %w", soakworkload.ErrRunNotActive, err)
	}
	if err != nil {
		return fmt.Errorf("touch soak heartbeat: %w", err)
	}
	return nil
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
		ctx, rate, maxInFlight, recordUnderrun, recordSaturation, action,
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
		return soakRunWindow{}, fmt.Errorf("soak manifest state %q cannot run", manifest.State)
	}
	if manifest.RunMode == soakRunModeContinuous {
		return soakRunWindow{}, fmt.Errorf(
			"soak manifest run mode %q cannot use a duration deadline", manifest.RunMode,
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
		Deadline: *manifest.Deadline, LastHeartbeatAt: heartbeat,
		RestartCount: manifest.RestartCount,
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
		return soakRunWindow{}, fmt.Errorf("soak manifest run mode %q is not continuous", manifest.RunMode)
	}
	if manifest.State != soakManifestSeeded && manifest.State != soakManifestRunning &&
		manifest.State != soakManifestStopped {
		return soakRunWindow{}, fmt.Errorf("continuous soak manifest state %q cannot run", manifest.State)
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
		return soakRunWindow{}, fmt.Errorf("mark continuous soak run running: %w", err)
	}
	return soakRunWindow{
		LastHeartbeatAt: startedAt, RestartCount: manifest.RestartCount,
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

func minimumSoakHeartbeatStaleAfter(
	heartbeatInterval time.Duration,
	attemptTimeout time.Duration,
) (time.Duration, bool) {
	return soakworkload.MinimumHeartbeatStaleAfter(heartbeatInterval, attemptTimeout)
}

type soakHeartbeatRetryWait func(context.Context, time.Duration) error

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
	var wait func(context.Context, time.Duration) error
	if waitRetry != nil {
		wait = waitRetry
	}
	return soakworkload.RunHeartbeat(
		ctx, &soakLifecycleAdapter{store: store}, runID, healthyTicks,
		attemptTimeout, retryInterval, staleAfter, shutdownMargin,
		lastSuccessAt, wait, now, observer,
	)
}

func waitSoakFailureInvalidation(
	invalidate func(string), reason string, budget time.Duration,
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

// waitSoakDrain remains at the root because the failure NATS runtime shares it.
func waitSoakDrain(done <-chan struct{}, budget time.Duration) bool {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
