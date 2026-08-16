package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type soakWorkloadAction func(context.Context, bool) error

type soakWorkloadActions struct {
	Send       soakWorkloadAction
	Read       soakWorkloadAction
	Mutation   soakWorkloadAction
	Reaction   soakWorkloadAction
	PinnedList soakWorkloadAction
	Verify     soakWorkloadAction
}

type soakWorkloadConfig struct {
	RunID             string
	Duration          time.Duration
	Continuous        bool
	Warmup            time.Duration
	HeartbeatInterval time.Duration
	SendRate          float64
	ReadRate          float64
	MutationRate      float64
	ReactionRate      float64
	PinnedListRate    float64
	VerifyRate        float64
	MaxInFlight       int
	StopOnActionError bool
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
}

type soakRunWindow struct {
	Deadline     time.Time
	RestartCount int
}

type soakLifecycleStore interface {
	GetManifest(context.Context, string) (*soakManifest, error)
	PutManifest(context.Context, *soakManifest) error
	TouchHeartbeat(context.Context, string, time.Time) error
}

var errSoakManifestNotFound = errors.New("soak run manifest not found")

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

type soakWorkload struct {
	cfg          soakWorkloadConfig
	store        soakLifecycleStore
	actions      soakWorkloadActions
	dispatch     soakLaneDispatcher
	now          func() time.Time
	onSaturation func()
	pacing       *soakPacingMetrics
}

func newSoakWorkload(
	cfg *soakWorkloadConfig,
	store soakLifecycleStore,
	actions soakWorkloadActions,
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
	if dispatch == nil {
		dispatch = dispatchSoakLane
	}
	if now == nil {
		now = time.Now
	}
	if onSaturation == nil {
		onSaturation = func() {}
	}
	workload := &soakWorkload{
		cfg: config, store: store, actions: actions, dispatch: dispatch,
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
					measured := !w.now().Before(warmupDeadline)
					setFatal(lane.action(actionCtx, measured))
				},
			)
		}()
	}

	<-runCtx.Done()
	laneWG.Wait()
	<-heartbeatDone
	fatalMu.Lock()
	dependencyErr := fatalErr
	fatalMu.Unlock()
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

func (w *soakWorkload) lanes() []soakLane {
	return []soakLane{
		{name: "send", rate: w.cfg.SendRate, action: w.actions.Send},
		{name: "read", rate: w.cfg.ReadRate, action: w.actions.Read},
		{name: "mutation", rate: w.cfg.MutationRate, action: w.actions.Mutation},
		{name: "reaction", rate: w.cfg.ReactionRate, action: w.actions.Reaction},
		{name: "pinned_list", rate: w.cfg.PinnedListRate, action: w.actions.PinnedList},
		{name: "verify", rate: w.cfg.VerifyRate, action: w.actions.Verify},
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
		Deadline:     *manifest.Deadline,
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
	return soakRunWindow{RestartCount: manifest.RestartCount}, nil
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

func runSoakHeartbeat(
	ctx context.Context,
	store soakLifecycleStore,
	runID string,
	ticks <-chan time.Time,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case at, ok := <-ticks:
			if !ok {
				return nil
			}
			if err := store.TouchHeartbeat(ctx, runID, at.UTC()); err != nil {
				return fmt.Errorf("update soak heartbeat: %w", err)
			}
		}
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
