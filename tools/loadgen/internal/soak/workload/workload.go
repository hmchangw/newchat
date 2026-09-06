package workload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
)

const (
	HeartbeatAttemptTimeout = 5 * time.Second
	heartbeatRetryInterval  = 5 * time.Second
)

type Action func(context.Context, bool) error

type Actions struct {
	Send           Action
	Read           Action
	Mutation       Action
	Reaction       Action
	PinnedList     Action
	Verify         Action
	MemberMutation Action
	RoomMutation   Action
	RoomRead       Action
	UserRead       Action
	SearchRead     Action
	RoomCreate     Action
	ReadReceipt    Action
	Presence       Action
}

type Config struct {
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

type Completion string

const (
	CompletionConfiguredDuration Completion = "configured_duration"
	CompletionCanceled           Completion = "canceled"
	CompletionDependencyFailure  Completion = "dependency_failure"
)

type Result struct {
	Completion   Completion
	Deadline     time.Time
	RestartCount int
	LeaseAbort   bool
}

// Lifecycle owns durable run-state transitions. Workload only orchestrates
// them, so it does not depend on the Mongo manifest representation.
type Lifecycle interface {
	Prepare(context.Context, string, time.Duration, bool, time.Time) (run.Window, error)
	Complete(context.Context, string, time.Time) error
	Stop(context.Context, string, time.Time) error
	TouchHeartbeat(context.Context, string, time.Time) error
}

var (
	ErrRunNotActive          = errors.New("soak run is not active")
	ErrHeartbeatLeaseInvalid = errors.New("soak heartbeat lease configuration is invalid")
	ErrHeartbeatLeaseAtRisk  = errors.New("soak heartbeat lease can no longer be renewed safely")
)

type HeartbeatOutcome string

const (
	HeartbeatSuccess   HeartbeatOutcome = "success"
	HeartbeatError     HeartbeatOutcome = "error"
	HeartbeatNotActive HeartbeatOutcome = "not_active"
)

type HeartbeatObserver interface {
	RecordHeartbeatAttempt(HeartbeatOutcome, bool, time.Time)
}

type Dispatcher func(
	ctx context.Context,
	lane string,
	rate float64,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	action func(context.Context),
)

type PacingOutcome string

const (
	PacingDispatched        PacingOutcome = "dispatched"
	PacingSchedulerUnderrun PacingOutcome = "scheduler_underrun"
	PacingLaneSaturation    PacingOutcome = "lane_saturation"
	PacingGlobalSaturation  PacingOutcome = "global_saturation"
)

type PacingRecorder interface {
	Configure(string, float64)
	Record(string, PacingOutcome, int)
}

type Option func(*Workload)

func WithPacingRecorder(recorder PacingRecorder) Option {
	return func(workload *Workload) { workload.pacing = recorder }
}

func WithHeartbeatObserver(observer HeartbeatObserver) Option {
	return func(workload *Workload) { workload.heartbeatObserver = observer }
}

func WithFailureInvalidation(invalidate func()) Option {
	return func(workload *Workload) { workload.invalidateFailure = invalidate }
}

type Workload struct {
	cfg               Config
	lifecycle         Lifecycle
	actions           Actions
	dispatch          Dispatcher
	now               func() time.Time
	onSaturation      func()
	pacing            PacingRecorder
	heartbeatObserver HeartbeatObserver
	invalidateFailure func()
}

func New(
	cfg *Config,
	lifecycle Lifecycle,
	actions *Actions,
	dispatch Dispatcher,
	now func() time.Time,
	onSaturation func(),
	options ...Option,
) *Workload {
	if cfg == nil {
		cfg = &Config{}
	}
	config := *cfg
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = 256
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.HeartbeatStaleAfter <= 0 {
		minimumStaleAfter, _ := MinimumHeartbeatStaleAfter(
			config.HeartbeatInterval,
			HeartbeatAttemptTimeout,
		)
		config.HeartbeatStaleAfter = max(2*time.Minute, minimumStaleAfter)
	}
	if now == nil {
		now = time.Now
	}
	if onSaturation == nil {
		onSaturation = func() {}
	}
	if actions == nil {
		actions = &Actions{}
	}
	workload := &Workload{
		cfg: config, lifecycle: lifecycle, actions: *actions, dispatch: dispatch,
		now: now, onSaturation: onSaturation,
	}
	for _, option := range options {
		option(workload)
	}
	return workload
}

func (w *Workload) Run(ctx context.Context) (Result, error) {
	if w.lifecycle == nil {
		return Result{}, fmt.Errorf("lifecycle is required")
	}
	if w.dispatch == nil {
		return Result{}, fmt.Errorf("dispatcher is required")
	}
	window, err := w.lifecycle.Prepare(
		ctx, w.cfg.RunID, w.cfg.Duration, w.cfg.Continuous, w.now().UTC(),
	)
	if err != nil {
		return Result{}, fmt.Errorf("prepare workload lifecycle: %w", err)
	}
	result := Result{Deadline: window.Deadline, RestartCount: window.RestartCount}
	if !w.cfg.Continuous && !w.now().Before(window.Deadline) {
		if err := w.lifecycle.Complete(ctx, w.cfg.RunID, w.now().UTC()); err != nil {
			return result, fmt.Errorf("complete elapsed workload: %w", err)
		}
		result.Completion = CompletionConfiguredDuration
		return result, nil
	}

	var runCtx context.Context
	var cancel context.CancelFunc
	if w.cfg.Continuous {
		runCtx, cancel = context.WithCancel(ctx)
	} else {
		runCtx, cancel = context.WithDeadline(ctx, window.Deadline)
	}
	defer cancel()
	warmupDeadline := w.now().Add(w.cfg.Warmup)
	globalBudget := make(chan struct{}, w.cfg.MaxInFlight)

	var fatalMu sync.Mutex
	var fatalErr error
	var laneWG sync.WaitGroup
	laneActivity := newLaneActivity()
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
		heartbeatErr := RunHeartbeat(
			runCtx, w.lifecycle, w.cfg.RunID, heartbeatTicker.C,
			HeartbeatAttemptTimeout, heartbeatRetryInterval,
			w.cfg.HeartbeatStaleAfter, w.cfg.HeartbeatInterval,
			window.LastHeartbeatAt, nil, w.now, w.heartbeatObserver,
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

	for _, lane := range w.lanes() {
		if lane.action == nil || lane.rate <= 0 {
			continue
		}
		lane := lane
		w.configurePacing(lane.name, lane.rate)
		laneWG.Add(1)
		go func() {
			defer laneWG.Done()
			w.dispatch(
				runCtx, lane.name, lane.rate, w.cfg.MaxInFlight,
				func(count int) { w.recordPacing(lane.name, PacingSchedulerUnderrun, count) },
				func() {
					w.recordPacing(lane.name, PacingLaneSaturation, 1)
					w.onSaturation()
				},
				func(actionCtx context.Context) {
					select {
					case globalBudget <- struct{}{}:
					default:
						w.recordPacing(lane.name, PacingGlobalSaturation, 1)
						w.onSaturation()
						return
					}
					w.recordPacing(lane.name, PacingDispatched, 1)
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
	if errors.Is(dependencyErr, ErrHeartbeatLeaseAtRisk) {
		drainBudget := w.cfg.HeartbeatInterval / 2
		if !waitLaneDrain(&laneWG, drainBudget) {
			activeLanes, inFlight := laneActivity.Snapshot()
			slog.Error("soak lane drain exceeded lease safety budget",
				"drainBudget", drainBudget, "inFlight", inFlight, "lanes", activeLanes)
			invalidationBudget := drainBudget / 2
			if !waitFailureInvalidation(w.invalidateFailure, invalidationBudget) {
				slog.Error("soak lease-abort invalidation exceeded safety budget",
					"invalidationBudget", invalidationBudget,
					"consequence", "the process will exit even if invalidation did not reach durable storage")
			}
			result.Completion = CompletionDependencyFailure
			result.LeaseAbort = true
			return result, dependencyErr
		}
	} else {
		laneWG.Wait()
	}

	switch {
	case dependencyErr != nil:
		result.Completion = CompletionDependencyFailure
		return result, fmt.Errorf("workload dependency failed: %w", dependencyErr)
	case ctx.Err() != nil:
		result.Completion = CompletionCanceled
		if w.cfg.Continuous {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStop()
			if err := w.lifecycle.Stop(stopCtx, w.cfg.RunID, w.now().UTC()); err != nil {
				return result, fmt.Errorf("stop continuous workload: %w", err)
			}
		}
		return result, ctx.Err()
	default:
		completeCtx, cancelComplete := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelComplete()
		if err := w.lifecycle.Complete(completeCtx, w.cfg.RunID, w.now().UTC()); err != nil {
			return result, fmt.Errorf("complete workload: %w", err)
		}
		result.Completion = CompletionConfiguredDuration
		return result, nil
	}
}

func (w *Workload) configurePacing(lane string, rate float64) {
	if w.pacing != nil {
		w.pacing.Configure(lane, rate)
	}
}

func (w *Workload) recordPacing(lane string, outcome PacingOutcome, count int) {
	if w.pacing != nil {
		w.pacing.Record(lane, outcome, count)
	}
}

type lane struct {
	name   string
	rate   float64
	action Action
}

func (w *Workload) lanes() []lane {
	return []lane{
		{name: "send", rate: w.cfg.SendRate, action: w.actions.Send},
		{name: "read", rate: w.cfg.ReadRate, action: w.actions.Read},
		{name: "mutation", rate: w.cfg.MutationRate, action: w.actions.Mutation},
		{name: "reaction", rate: w.cfg.ReactionRate, action: w.actions.Reaction},
		{name: "pinned_list", rate: w.cfg.PinnedListRate, action: w.actions.PinnedList},
		{name: "verify", rate: w.cfg.VerifyRate, action: w.actions.Verify},
		{name: "member_mutation", rate: w.cfg.MemberMutationRate, action: w.actions.MemberMutation},
		{name: "room_mutation", rate: w.cfg.RoomMutationRate, action: w.actions.RoomMutation},
		{name: "room_read", rate: w.cfg.RoomReadRate, action: w.actions.RoomRead},
		{name: "user_read", rate: w.cfg.UserReadRate, action: w.actions.UserRead},
		{name: "search_read", rate: w.cfg.SearchReadRate, action: w.actions.SearchRead},
		{name: "room_create", rate: w.cfg.RoomCreateRate, action: w.actions.RoomCreate},
		{name: "read_receipt", rate: w.cfg.ReadReceiptRate, action: w.actions.ReadReceipt},
		{name: "presence", rate: w.cfg.PresenceRate, action: w.actions.Presence},
	}
}

type laneActivity struct {
	mu     sync.Mutex
	active map[string]int
}

func newLaneActivity() *laneActivity {
	return &laneActivity{active: make(map[string]int)}
}

func (a *laneActivity) Start(lane string) func() {
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

func (a *laneActivity) Snapshot() (map[string]int, int) {
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

func waitLaneDrain(laneWG *sync.WaitGroup, budget time.Duration) bool {
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

func waitFailureInvalidation(invalidate func(), budget time.Duration) bool {
	if invalidate == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		invalidate()
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

type HeartbeatRetryWait func(context.Context, time.Duration) error

func MinimumHeartbeatStaleAfter(
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

func waitHeartbeatRetry(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func RunHeartbeat(
	ctx context.Context,
	store interface {
		TouchHeartbeat(context.Context, string, time.Time) error
	},
	runID string,
	healthyTicks <-chan time.Time,
	attemptTimeout time.Duration,
	retryInterval time.Duration,
	staleAfter time.Duration,
	shutdownMargin time.Duration,
	lastSuccessAt time.Time,
	waitRetry HeartbeatRetryWait,
	now func() time.Time,
	observer HeartbeatObserver,
) error {
	if attemptTimeout <= 0 {
		attemptTimeout = HeartbeatAttemptTimeout
	}
	if now == nil {
		now = time.Now
	}
	if retryInterval <= 0 {
		retryInterval = heartbeatRetryInterval
	}
	if waitRetry == nil {
		waitRetry = waitHeartbeatRetry
	}
	minimumStaleAfter, validDurations := MinimumHeartbeatStaleAfter(shutdownMargin, attemptTimeout)
	if !validDurations || staleAfter < minimumStaleAfter {
		return fmt.Errorf("%w: stale threshold must be at least twice the shutdown margin plus the attempt timeout", ErrHeartbeatLeaseInvalid)
	}
	if lastSuccessAt.IsZero() {
		return fmt.Errorf("heartbeat lease requires the last persisted heartbeat time")
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
				return heartbeatLeaseError(lastSuccessAt, stopAt)
			}
			wait := min(retryInterval, stopAt.Sub(current))
			if err := waitRetry(ctx, wait); err != nil {
				return fmt.Errorf("wait heartbeat retry: %w", err)
			}
			at = now().UTC()
			if !at.Before(stopAt) {
				return heartbeatLeaseError(lastSuccessAt, stopAt)
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
			return heartbeatLeaseError(lastSuccessAt, stopAt)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, min(attemptTimeout, remaining))
		err := store.TouchHeartbeat(attemptCtx, runID, at.UTC())
		cancel()
		completedAt := now().UTC()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, ErrRunNotActive), errors.Is(err, run.ErrRunNotActive):
			recordHeartbeatAttempt(observer, HeartbeatNotActive, false, completedAt)
			return fmt.Errorf("update heartbeat: %w", err)
		case err != nil:
			recordHeartbeatAttempt(observer, HeartbeatError, true, completedAt)
			if !degraded {
				degraded = true
				degradedAt = completedAt
				slog.Warn("heartbeat entered degraded state", "runId", runID, "error", err)
			}
			continue
		default:
			recordHeartbeatAttempt(observer, HeartbeatSuccess, false, completedAt)
			lastSuccessAt = at.UTC()
			if degraded {
				slog.Info("heartbeat recovered", "runId", runID,
					"degradedDuration", completedAt.Sub(degradedAt))
				if !drainHeartbeatTicks(healthyTicks) {
					healthyTicks = nil
				}
			}
			degraded = false
		}
	}
	return nil
}

func heartbeatLeaseError(lastSuccessAt, stopAt time.Time) error {
	return fmt.Errorf("%w: last persisted heartbeat %s; workload stop boundary %s",
		ErrHeartbeatLeaseAtRisk, lastSuccessAt.Format(time.RFC3339Nano), stopAt.Format(time.RFC3339Nano))
}

func drainHeartbeatTicks(ticks <-chan time.Time) bool {
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

func recordHeartbeatAttempt(
	observer HeartbeatObserver,
	outcome HeartbeatOutcome,
	degraded bool,
	completedAt time.Time,
) {
	if observer != nil {
		observer.RecordHeartbeatAttempt(outcome, degraded, completedAt)
	}
}
