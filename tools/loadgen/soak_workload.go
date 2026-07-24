package main

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	Warmup            time.Duration
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
}

var errSoakManifestNotFound = errors.New("soak run manifest not found")

type soakLaneDispatcher func(
	ctx context.Context,
	lane string,
	rate int,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	action func(context.Context),
)

type soakWorkload struct {
	cfg          soakWorkloadConfig
	store        soakLifecycleStore
	actions      soakWorkloadActions
	dispatch     soakLaneDispatcher
	now          func() time.Time
	onSaturation func()
}

func newSoakWorkload(
	cfg *soakWorkloadConfig,
	store soakLifecycleStore,
	actions soakWorkloadActions,
	dispatch soakLaneDispatcher,
	now func() time.Time,
	onSaturation func(),
) *soakWorkload {
	if cfg == nil {
		cfg = &soakWorkloadConfig{}
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
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
		cfg: *cfg, store: store, actions: actions, dispatch: dispatch,
		now: now, onSaturation: onSaturation,
	}
}

func (w *soakWorkload) Run(
	ctx context.Context,
) (soakWorkloadResult, error) {
	window, err := prepareSoakRun(
		ctx,
		w.store,
		w.cfg.RunID,
		w.cfg.Duration,
		w.now().UTC(),
	)
	if err != nil {
		return soakWorkloadResult{}, err
	}
	result := soakWorkloadResult{
		Deadline:     window.Deadline,
		RestartCount: window.RestartCount,
	}
	if !w.now().Before(window.Deadline) {
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

	runCtx, cancel := context.WithDeadline(ctx, window.Deadline)
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
	lanes := w.lanes()
	for _, lane := range lanes {
		if lane.action == nil || lane.rate <= 0 {
			continue
		}
		lane := lane
		laneWG.Add(1)
		go func() {
			defer laneWG.Done()
			w.dispatch(
				runCtx,
				lane.name,
				soakIntegerRate(lane.rate),
				w.cfg.MaxInFlight,
				func(int) {},
				w.onSaturation,
				func(actionCtx context.Context) {
					select {
					case globalBudget <- struct{}{}:
					default:
						w.onSaturation()
						return
					}
					defer func() { <-globalBudget }()
					measured := !w.now().Before(warmupDeadline)
					setFatal(lane.action(actionCtx, measured))
				},
			)
		}()
	}

	<-runCtx.Done()
	laneWG.Wait()
	fatalMu.Lock()
	dependencyErr := fatalErr
	fatalMu.Unlock()
	switch {
	case dependencyErr != nil:
		result.Completion = soakCompletionDependencyFailure
		return result, dependencyErr
	case ctx.Err() != nil:
		result.Completion = soakCompletionCanceled
		return result, ctx.Err()
	default:
		if err := completeSoakRun(
			context.Background(),
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
	rate int,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	action func(context.Context),
) {
	pacedDispatch(
		ctx,
		rate,
		maxInFlight,
		recordUnderrun,
		recordSaturation,
		action,
	)
}

func soakIntegerRate(rate float64) int {
	return max(1, int(math.Round(rate)))
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
	if manifest.State != soakManifestSeeded &&
		manifest.State != soakManifestRunning &&
		manifest.State != soakManifestCompleted {
		return soakRunWindow{}, fmt.Errorf(
			"soak manifest state %q cannot run",
			manifest.State,
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
	manifest.UpdatedAt = now.UTC()
	if err := store.PutManifest(ctx, manifest); err != nil {
		return soakRunWindow{}, fmt.Errorf("mark soak run running: %w", err)
	}
	return soakRunWindow{
		Deadline:     *manifest.Deadline,
		RestartCount: manifest.RestartCount,
	}, nil
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
	completedAt := now.UTC()
	manifest.State = soakManifestCompleted
	manifest.CompletedAt = &completedAt
	manifest.UpdatedAt = completedAt
	if err := store.PutManifest(ctx, manifest); err != nil {
		return fmt.Errorf("mark soak run completed: %w", err)
	}
	return nil
}
