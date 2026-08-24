package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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
	metrics := NewMetrics()
	probe := newSoakMongoProbe(pinger, 2*time.Second, func() time.Time { return now }, metrics)

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
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.MongoProbeAttempts.WithLabelValues(string(soakMongoProbeSuccess))))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.MongoProbeAttempts.WithLabelValues(string(soakMongoProbeError))))
}

func TestSoakMongoProbe_ShutdownCancellationPreservesLastCompletedSnapshot(t *testing.T) {
	pinger := &fakeSoakMongoPinger{errors: []error{nil, context.Canceled}}
	now := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	metrics := NewMetrics()
	probe := newSoakMongoProbe(pinger, 2*time.Second, func() time.Time { return now }, metrics)

	require.NoError(t, probe.Probe(context.Background()))
	want := probe.Snapshot()

	now = now.Add(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, probe.Probe(ctx), context.Canceled)
	assert.Equal(t, want, probe.Snapshot())
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.MongoProbeAttempts.WithLabelValues(string(soakMongoProbeSuccess))))
	assert.Zero(t, testutil.ToFloat64(
		metrics.MongoProbeAttempts.WithLabelValues(string(soakMongoProbeError))),
		"shutdown cancellation must not manufacture a Mongo outage attempt")
}

func TestSoakMongoProbe_LogsOnlyHealthTransitions(t *testing.T) {
	wantErr := errors.New("primary unavailable")
	pinger := &fakeSoakMongoPinger{errors: []error{wantErr, wantErr, nil, nil}}
	probe := newSoakMongoProbe(pinger, time.Second, time.Now, nil)
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	assert.ErrorIs(t, probe.Probe(context.Background()), wantErr)
	assert.ErrorIs(t, probe.Probe(context.Background()), wantErr)
	require.NoError(t, probe.Probe(context.Background()))
	require.NoError(t, probe.Probe(context.Background()))

	output := logs.String()
	assert.Equal(t, 1, strings.Count(output, "Mongo primary probe entered degraded state"))
	assert.Equal(t, 1, strings.Count(output, "Mongo primary probe recovered"))
}

func TestRunSoakMongoProbe_StopsWhenTicksClose(t *testing.T) {
	pinger := &fakeSoakMongoPinger{}
	probe := newSoakMongoProbe(pinger, 2*time.Second, time.Now, nil)
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

	values := gatherLoadgenMetricValues(
		t, metrics, "loadgen_mongo_up", "loadgen_mongo_probe_timestamp_seconds",
	)
	assert.Equal(t, float64(1), values["loadgen_mongo_up"])
	assert.Equal(t, float64(now.Unix()), values["loadgen_mongo_probe_timestamp_seconds"])
}

func TestSoakHeartbeatStatus_ExportsAttemptsHealthAndFreshness(t *testing.T) {
	metrics := NewMetrics()
	status := newSoakHeartbeatStatus(metrics)
	failedAt := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	succeededAt := failedAt.Add(5 * time.Second)

	status.RecordHeartbeatAttempt(soakHeartbeatError, true, failedAt)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatError))))
	values := gatherLoadgenMetricValues(
		t, metrics,
		"loadgen_soak_heartbeat_degraded",
		"loadgen_soak_heartbeat_success_timestamp_seconds",
	)
	assert.Equal(t, float64(1), values["loadgen_soak_heartbeat_degraded"])
	assert.Zero(t, values["loadgen_soak_heartbeat_success_timestamp_seconds"])

	status.RecordHeartbeatAttempt(soakHeartbeatSuccess, false, succeededAt)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatSuccess))))
	values = gatherLoadgenMetricValues(
		t, metrics,
		"loadgen_soak_heartbeat_degraded",
		"loadgen_soak_heartbeat_success_timestamp_seconds",
	)
	assert.Zero(t, values["loadgen_soak_heartbeat_degraded"])
	assert.Equal(t, float64(succeededAt.Unix()),
		values["loadgen_soak_heartbeat_success_timestamp_seconds"])

	status.RecordHeartbeatAttempt(soakHeartbeatNotActive, false, succeededAt.Add(time.Second))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakHeartbeatAttempts.WithLabelValues(string(soakHeartbeatNotActive))))
}

func TestSoakMongoProbeMetrics_DefaultToUnknownAndReadOneSnapshot(t *testing.T) {
	metrics := NewMetrics()
	values := gatherLoadgenMetricValues(
		t, metrics, "loadgen_mongo_up", "loadgen_mongo_probe_timestamp_seconds",
	)
	assert.Zero(t, values["loadgen_mongo_up"])
	assert.Zero(t, values["loadgen_mongo_probe_timestamp_seconds"])

	probe := newSoakMongoProbe(
		&fakeSoakMongoPinger{}, time.Second,
		func() time.Time { return time.Unix(123, 0).UTC() },
		metrics,
	)
	metrics.SetMongoProbeSource(probe.Snapshot)
	require.NoError(t, probe.Probe(context.Background()))
	values = gatherLoadgenMetricValues(
		t, metrics, "loadgen_mongo_up", "loadgen_mongo_probe_timestamp_seconds",
	)
	assert.Equal(t, float64(1), values["loadgen_mongo_up"])
	assert.Equal(t, float64(123), values["loadgen_mongo_probe_timestamp_seconds"])
}

func TestSoakControlPlaneCollectors_ReadEachSnapshotOncePerScrape(t *testing.T) {
	metrics := NewMetrics()
	var mongoCalls atomic.Int64
	metrics.SetMongoProbeSource(func() soakMongoProbeSnapshot {
		call := mongoCalls.Add(1)
		return soakMongoProbeSnapshot{
			Up: call == 1, CompletedAt: time.Unix(122+call, 0).UTC(),
		}
	})
	var heartbeatCalls atomic.Int64
	metrics.SetSoakHeartbeatSource(func() soakHeartbeatSnapshot {
		call := heartbeatCalls.Add(1)
		return soakHeartbeatSnapshot{
			Degraded: call != 1, LastSuccess: time.Unix(455+call, 0).UTC(),
		}
	})

	values := gatherLoadgenMetricValues(
		t, metrics,
		"loadgen_mongo_up",
		"loadgen_mongo_probe_timestamp_seconds",
		"loadgen_soak_heartbeat_degraded",
		"loadgen_soak_heartbeat_success_timestamp_seconds",
	)

	assert.Equal(t, int64(1), mongoCalls.Load())
	assert.Equal(t, float64(1), values["loadgen_mongo_up"])
	assert.Equal(t, float64(123), values["loadgen_mongo_probe_timestamp_seconds"])
	assert.Equal(t, int64(1), heartbeatCalls.Load())
	assert.Zero(t, values["loadgen_soak_heartbeat_degraded"])
	assert.Equal(t, float64(456), values["loadgen_soak_heartbeat_success_timestamp_seconds"])
}

func gatherLoadgenMetricValues(
	t *testing.T,
	metrics *Metrics,
	names ...string,
) map[string]float64 {
	t.Helper()
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	values := make(map[string]float64, len(names))
	for _, family := range families {
		if _, ok := wanted[family.GetName()]; !ok {
			continue
		}
		require.Len(t, family.Metric, 1)
		values[family.GetName()] = family.Metric[0].GetGauge().GetValue()
	}
	for _, name := range names {
		assert.Contains(t, values, name)
	}
	return values
}
