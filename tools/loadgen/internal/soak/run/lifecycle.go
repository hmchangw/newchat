package run

import (
	"context"
	"fmt"
	"time"
)

const continuousMode = "continuous"

type Window struct {
	Deadline        time.Time
	LastHeartbeatAt time.Time
	RestartCount    int
}

type LifecycleStore interface {
	GetManifest(context.Context, string) (*Manifest, error)
	PutManifest(context.Context, *Manifest) error
	TouchHeartbeat(context.Context, string, time.Time) error
}

// Lifecycle owns the durable state transitions around one workload process.
// The scheduler only receives the resulting window, so manifest persistence
// and restart rules remain in the run package with seed and teardown.
type Lifecycle struct {
	store LifecycleStore
}

func NewLifecycle(store LifecycleStore) *Lifecycle {
	return &Lifecycle{store: store}
}

func (l *Lifecycle) Prepare(
	ctx context.Context,
	runID string,
	duration time.Duration,
	continuous bool,
	now time.Time,
) (Window, error) {
	if continuous {
		return l.prepareContinuous(ctx, runID, now)
	}
	return l.prepareDuration(ctx, runID, duration, now)
}

func (l *Lifecycle) prepareDuration(
	ctx context.Context,
	runID string,
	duration time.Duration,
	now time.Time,
) (Window, error) {
	if err := l.validate(runID); err != nil {
		return Window{}, err
	}
	if duration <= 0 {
		return Window{}, fmt.Errorf("soak duration must be greater than zero")
	}
	manifest, err := l.store.GetManifest(ctx, runID)
	if err != nil {
		return Window{}, fmt.Errorf("load soak lifecycle: %w", err)
	}
	if manifest == nil {
		return Window{}, ErrManifestNotFound
	}
	if manifest.State != StateSeeded &&
		manifest.State != StateRunning &&
		manifest.State != StateCompleted {
		return Window{}, fmt.Errorf("soak manifest state %q cannot run", manifest.State)
	}
	if manifest.RunMode == continuousMode {
		return Window{}, fmt.Errorf(
			"soak manifest run mode %q cannot use a duration deadline", manifest.RunMode,
		)
	}
	startedAt := now.UTC()
	if manifest.Deadline == nil {
		deadline := startedAt.Add(duration)
		manifest.FirstStartedAt = &startedAt
		manifest.Deadline = &deadline
		manifest.ConfiguredDuration = duration
		manifest.RestartCount = 0
	} else if manifest.State == StateRunning {
		manifest.RestartCount++
	}
	manifest.State = StateRunning
	manifest.LastHeartbeatAt = &startedAt
	manifest.UpdatedAt = startedAt
	if err := l.store.PutManifest(ctx, manifest); err != nil {
		return Window{}, fmt.Errorf("mark soak run running: %w", err)
	}
	return Window{
		Deadline:        *manifest.Deadline,
		LastHeartbeatAt: startedAt,
		RestartCount:    manifest.RestartCount,
	}, nil
}

func (l *Lifecycle) prepareContinuous(
	ctx context.Context,
	runID string,
	now time.Time,
) (Window, error) {
	if err := l.validate(runID); err != nil {
		return Window{}, err
	}
	manifest, err := l.store.GetManifest(ctx, runID)
	if err != nil {
		return Window{}, fmt.Errorf("load continuous soak lifecycle: %w", err)
	}
	if manifest == nil {
		return Window{}, ErrManifestNotFound
	}
	if manifest.RunMode != continuousMode {
		return Window{}, fmt.Errorf(
			"soak manifest run mode %q is not continuous", manifest.RunMode,
		)
	}
	if manifest.State != StateSeeded &&
		manifest.State != StateRunning &&
		manifest.State != StateStopped {
		return Window{}, fmt.Errorf(
			"continuous soak manifest state %q cannot run", manifest.State,
		)
	}
	startedAt := now.UTC()
	if manifest.FirstStartedAt == nil {
		manifest.FirstStartedAt = &startedAt
		manifest.RestartCount = 0
	} else {
		manifest.RestartCount++
	}
	manifest.State = StateRunning
	manifest.Deadline = nil
	manifest.ConfiguredDuration = 0
	manifest.LastStoppedAt = nil
	manifest.LastHeartbeatAt = &startedAt
	manifest.UpdatedAt = startedAt
	if err := l.store.PutManifest(ctx, manifest); err != nil {
		return Window{}, fmt.Errorf("mark continuous soak run running: %w", err)
	}
	return Window{
		LastHeartbeatAt: startedAt,
		RestartCount:    manifest.RestartCount,
	}, nil
}

func (l *Lifecycle) Complete(
	ctx context.Context,
	runID string,
	now time.Time,
) error {
	if err := l.validate(runID); err != nil {
		return err
	}
	manifest, err := l.store.GetManifest(ctx, runID)
	if err != nil {
		return fmt.Errorf("load soak manifest for completion: %w", err)
	}
	if manifest == nil {
		return ErrManifestNotFound
	}
	completedAt := now.UTC()
	manifest.State = StateCompleted
	manifest.CompletedAt = &completedAt
	manifest.UpdatedAt = completedAt
	if err := l.store.PutManifest(ctx, manifest); err != nil {
		return fmt.Errorf("mark soak run completed: %w", err)
	}
	return nil
}

func (l *Lifecycle) Stop(
	ctx context.Context,
	runID string,
	now time.Time,
) error {
	if err := l.validate(runID); err != nil {
		return err
	}
	manifest, err := l.store.GetManifest(ctx, runID)
	if err != nil {
		return fmt.Errorf("load soak manifest for stop: %w", err)
	}
	if manifest == nil {
		return ErrManifestNotFound
	}
	if manifest.State == StateStopped {
		return nil
	}
	if manifest.State != StateRunning {
		return fmt.Errorf("soak manifest state %q cannot stop", manifest.State)
	}
	stoppedAt := now.UTC()
	manifest.State = StateStopped
	manifest.LastStoppedAt = &stoppedAt
	manifest.UpdatedAt = stoppedAt
	if err := l.store.PutManifest(ctx, manifest); err != nil {
		return fmt.Errorf("mark soak run stopped: %w", err)
	}
	return nil
}

func (l *Lifecycle) TouchHeartbeat(
	ctx context.Context,
	runID string,
	at time.Time,
) error {
	if err := l.validate(runID); err != nil {
		return err
	}
	if err := l.store.TouchHeartbeat(ctx, runID, at.UTC()); err != nil {
		return fmt.Errorf("touch soak heartbeat: %w", err)
	}
	return nil
}

func (l *Lifecycle) validate(runID string) error {
	if l == nil || l.store == nil {
		return fmt.Errorf("soak lifecycle store is required")
	}
	if runID == "" {
		return fmt.Errorf("soak run ID is required")
	}
	return nil
}
