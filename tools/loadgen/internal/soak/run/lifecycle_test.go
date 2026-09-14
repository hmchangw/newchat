package run

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakLifecycle_PrepareDurationPreservesOriginalDeadline(t *testing.T) {
	firstStart := time.Unix(100, 0).UTC()
	store := &lifecycleStore{manifest: &Manifest{
		ID: "run-a", State: StateSeeded, RunMode: "duration",
	}}
	lifecycle := NewLifecycle(store)

	first, err := lifecycle.Prepare(
		context.Background(), "run-a", time.Hour, false, firstStart,
	)
	require.NoError(t, err)
	assert.Equal(t, firstStart.Add(time.Hour), first.Deadline)
	assert.Equal(t, firstStart, first.LastHeartbeatAt)
	assert.Zero(t, first.RestartCount)

	restartAt := firstStart.Add(20 * time.Minute)
	second, err := lifecycle.Prepare(
		context.Background(), "run-a", time.Hour, false, restartAt,
	)
	require.NoError(t, err)
	assert.Equal(t, first.Deadline, second.Deadline)
	assert.Equal(t, restartAt, second.LastHeartbeatAt)
	assert.Equal(t, 1, second.RestartCount)

	manifest := store.snapshot()
	require.NotNil(t, manifest.FirstStartedAt)
	require.NotNil(t, manifest.Deadline)
	assert.Equal(t, firstStart, *manifest.FirstStartedAt)
	assert.Equal(t, first.Deadline, *manifest.Deadline)
	assert.Equal(t, StateRunning, manifest.State)
	assert.Equal(t, time.Hour, manifest.ConfiguredDuration)
}

func TestSoakLifecycle_ContinuousRunStopsAndResumesWithoutDeadline(t *testing.T) {
	firstStart := time.Unix(100, 0).UTC()
	store := &lifecycleStore{manifest: &Manifest{
		ID: "run-a", State: StateSeeded, RunMode: "continuous",
	}}
	lifecycle := NewLifecycle(store)

	first, err := lifecycle.Prepare(
		context.Background(), "run-a", 0, true, firstStart,
	)
	require.NoError(t, err)
	assert.True(t, first.Deadline.IsZero())
	assert.Zero(t, first.RestartCount)

	stoppedAt := firstStart.Add(time.Minute)
	require.NoError(t, lifecycle.Stop(context.Background(), "run-a", stoppedAt))
	manifest := store.snapshot()
	assert.Equal(t, StateStopped, manifest.State)
	require.NotNil(t, manifest.LastStoppedAt)
	assert.Equal(t, stoppedAt, *manifest.LastStoppedAt)

	restartAt := firstStart.Add(2 * time.Minute)
	second, err := lifecycle.Prepare(
		context.Background(), "run-a", 0, true, restartAt,
	)
	require.NoError(t, err)
	assert.True(t, second.Deadline.IsZero())
	assert.Equal(t, 1, second.RestartCount)
	manifest = store.snapshot()
	assert.Equal(t, StateRunning, manifest.State)
	assert.Nil(t, manifest.Deadline)
	assert.Nil(t, manifest.LastStoppedAt)
	assert.Zero(t, manifest.ConfiguredDuration)
}

func TestSoakLifecycle_CompleteAndTouchHeartbeat(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	store := &lifecycleStore{manifest: &Manifest{
		ID: "run-a", State: StateRunning, RunMode: "duration",
	}}
	lifecycle := NewLifecycle(store)

	require.NoError(t, lifecycle.TouchHeartbeat(context.Background(), "run-a", now))
	assert.Equal(t, now, store.heartbeat)
	require.NoError(t, lifecycle.Complete(context.Background(), "run-a", now))
	manifest := store.snapshot()
	assert.Equal(t, StateCompleted, manifest.State)
	require.NotNil(t, manifest.CompletedAt)
	assert.Equal(t, now, *manifest.CompletedAt)
}

func TestSoakLifecycle_RejectsInvalidInputsStatesAndStoreFailures(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	wantErr := errors.New("store unavailable")

	tests := []struct {
		name       string
		lifecycle  *Lifecycle
		runID      string
		duration   time.Duration
		continuous bool
		want       string
		is         error
	}{
		{name: "nil store", lifecycle: NewLifecycle(nil), runID: "run-a", duration: time.Hour, want: "store is required"},
		{name: "empty run ID", lifecycle: NewLifecycle(&lifecycleStore{}), duration: time.Hour, want: "run ID is required"},
		{name: "invalid duration", lifecycle: NewLifecycle(&lifecycleStore{}), runID: "run-a", want: "duration must be greater than zero"},
		{name: "missing manifest", lifecycle: NewLifecycle(&lifecycleStore{}), runID: "run-a", duration: time.Hour, is: ErrManifestNotFound},
		{name: "get failure", lifecycle: NewLifecycle(&lifecycleStore{getErr: wantErr}), runID: "run-a", duration: time.Hour, is: wantErr},
		{name: "put failure", lifecycle: NewLifecycle(&lifecycleStore{manifest: &Manifest{ID: "run-a", State: StateSeeded}, putErr: wantErr}), runID: "run-a", duration: time.Hour, is: wantErr},
		{name: "invalid state", lifecycle: NewLifecycle(&lifecycleStore{manifest: &Manifest{ID: "run-a", State: StateCleaned}}), runID: "run-a", duration: time.Hour, want: "cannot run"},
		{name: "duration rejects continuous manifest", lifecycle: NewLifecycle(&lifecycleStore{manifest: &Manifest{ID: "run-a", State: StateSeeded, RunMode: "continuous"}}), runID: "run-a", duration: time.Hour, want: "cannot use a duration deadline"},
		{name: "continuous rejects duration manifest", lifecycle: NewLifecycle(&lifecycleStore{manifest: &Manifest{ID: "run-a", State: StateSeeded, RunMode: "duration"}}), runID: "run-a", continuous: true, want: "is not continuous"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.lifecycle.Prepare(
				context.Background(), tt.runID, tt.duration, tt.continuous, now,
			)
			require.Error(t, err)
			if tt.want != "" {
				assert.Contains(t, err.Error(), tt.want)
			}
			if tt.is != nil {
				assert.ErrorIs(t, err, tt.is)
			}
		})
	}
}

type lifecycleStore struct {
	manifest  *Manifest
	heartbeat time.Time
	getErr    error
	putErr    error
	touchErr  error
}

func (s *lifecycleStore) GetManifest(context.Context, string) (*Manifest, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.manifest == nil {
		return nil, nil
	}
	manifest := *s.manifest
	return &manifest, nil
}

func (s *lifecycleStore) PutManifest(_ context.Context, manifest *Manifest) error {
	if s.putErr != nil {
		return s.putErr
	}
	copy := *manifest
	s.manifest = &copy
	return nil
}

func (s *lifecycleStore) TouchHeartbeat(_ context.Context, _ string, at time.Time) error {
	if s.touchErr != nil {
		return s.touchErr
	}
	s.heartbeat = at
	return nil
}

func (s *lifecycleStore) snapshot() Manifest {
	if s.manifest == nil {
		return Manifest{}
	}
	return *s.manifest
}
