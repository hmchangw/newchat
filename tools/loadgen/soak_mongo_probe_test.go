package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type fakeSoakMongoPinger struct {
	mu        sync.Mutex
	errors    []error
	deadlines []time.Time
	called    chan struct{}
}

func (p *fakeSoakMongoPinger) Ping(ctx context.Context, _ *readpref.ReadPref) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		p.deadlines = append(p.deadlines, deadline)
	}
	if p.called != nil {
		select {
		case p.called <- struct{}{}:
		default:
		}
	}
	if len(p.errors) == 0 {
		return nil
	}
	err := p.errors[0]
	p.errors = p.errors[1:]
	return err
}

func (p *fakeSoakMongoPinger) snapshotDeadlines() []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]time.Time(nil), p.deadlines...)
}

func TestSoakMongoProbe_RecordsSuccessFailureAndAttemptDeadline(t *testing.T) {
	wantErr := errors.New("primary unavailable")
	pinger := &fakeSoakMongoPinger{errors: []error{nil, wantErr}}
	now := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	probe := newSoakMongoProbe(pinger, 2*time.Second, func() time.Time { return now })

	require.NoError(t, probe.Probe(context.Background()))
	snapshot := probe.Snapshot()
	assert.True(t, snapshot.Up)
	assert.Equal(t, now, snapshot.CompletedAt)

	now = now.Add(5 * time.Second)
	assert.ErrorIs(t, probe.Probe(context.Background()), wantErr)
	snapshot = probe.Snapshot()
	assert.False(t, snapshot.Up)
	assert.Equal(t, now, snapshot.CompletedAt)
	deadlines := pinger.snapshotDeadlines()
	require.Len(t, deadlines, 2)
	assert.WithinDuration(t, time.Now().Add(2*time.Second), deadlines[0], time.Second)
}

func TestSoakMongoProbe_ShutdownCancellationPreservesLastCompletedSnapshot(t *testing.T) {
	pinger := &fakeSoakMongoPinger{errors: []error{nil, context.Canceled}}
	now := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	probe := newSoakMongoProbe(pinger, 2*time.Second, func() time.Time { return now })

	require.NoError(t, probe.Probe(context.Background()))
	want := probe.Snapshot()

	now = now.Add(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, probe.Probe(ctx), context.Canceled)
	assert.Equal(t, want, probe.Snapshot())
}

func TestRunSoakMongoProbe_StopsWhenTicksClose(t *testing.T) {
	pinger := &fakeSoakMongoPinger{}
	probe := newSoakMongoProbe(pinger, 2*time.Second, time.Now)
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	close(ticks)

	require.NoError(t, runSoakMongoProbe(context.Background(), probe, ticks))
	completedAt := probe.Snapshot().CompletedAt
	require.False(t, completedAt.IsZero())
	assert.Equal(t, completedAt, probe.Snapshot().CompletedAt)
}

func TestSoakMongoProbe_StartsImmediatelyAndStopsIdempotently(t *testing.T) {
	called := make(chan struct{}, 1)
	pinger := &fakeSoakMongoPinger{called: called}
	metrics := NewMetrics()
	now := time.Unix(456, 0).UTC()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := startSoakMongoProbe(ctx, pinger, metrics, func() time.Time { return now })
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("Mongo probe did not run immediately")
	}
	shutdown()
	shutdown()

	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.MongoUp))
	assert.Equal(t, float64(now.Unix()), testutil.ToFloat64(metrics.MongoProbeTimestamp))
}

func TestSoakHeartbeatStatus_ExportsAttemptsHealthAndFreshness(t *testing.T) {
	metrics := NewMetrics()
	status := newSoakHeartbeatStatus(metrics)
	failedAt := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	succeededAt := failedAt.Add(5 * time.Second)

	status.RecordHeartbeatAttempt(soakHeartbeatError, true, failedAt)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatError))))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SoakHeartbeatDegraded))
	assert.Zero(t, testutil.ToFloat64(metrics.SoakHeartbeatSuccessTimestamp))

	status.RecordHeartbeatAttempt(soakHeartbeatSuccess, false, succeededAt)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatSuccess))))
	assert.Zero(t, testutil.ToFloat64(metrics.SoakHeartbeatDegraded))
	assert.Equal(t, float64(succeededAt.Unix()),
		testutil.ToFloat64(metrics.SoakHeartbeatSuccessTimestamp))

	status.RecordHeartbeatAttempt(soakHeartbeatNotActive, false, succeededAt.Add(time.Second))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatNotActive))))
}

func TestSoakMongoProbeMetrics_DefaultToUnknownAndReadOneSnapshot(t *testing.T) {
	metrics := NewMetrics()
	assert.Zero(t, testutil.ToFloat64(metrics.MongoUp))
	assert.Zero(t, testutil.ToFloat64(metrics.MongoProbeTimestamp))

	probe := newSoakMongoProbe(
		&fakeSoakMongoPinger{}, time.Second,
		func() time.Time { return time.Unix(123, 0).UTC() },
	)
	metrics.SetMongoProbeSource(probe.Snapshot)
	require.NoError(t, probe.Probe(context.Background()))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.MongoUp))
	assert.Equal(t, float64(123), testutil.ToFloat64(metrics.MongoProbeTimestamp))
}
