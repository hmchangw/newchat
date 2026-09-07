package main

import (
	"context"
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
type soakRunWindow = soakrun.Window

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
	return &soakWorkload{
		inner: soakworkload.New(
			&config, soakrun.NewLifecycle(store), actions, dispatch, now, onSaturation, options...,
		),
		cfg: config, actions: *actions, dispatch: dispatch,
		now: now, onSaturation: onSaturation,
	}
}

func (w *soakWorkload) Run(ctx context.Context) (soakWorkloadResult, error) {
	return w.inner.Run(ctx)
}

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
	return soakrun.NewLifecycle(store).Prepare(ctx, runID, duration, false, now)
}

func prepareContinuousSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) (soakRunWindow, error) {
	return soakrun.NewLifecycle(store).Prepare(ctx, runID, 0, true, now)
}

func stopSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) error {
	return soakrun.NewLifecycle(store).Stop(ctx, runID, now)
}

func completeSoakRun(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	now time.Time,
) error {
	return soakrun.NewLifecycle(store).Complete(ctx, runID, now)
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
		ctx, soakrun.NewLifecycle(store), runID, healthyTicks,
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
