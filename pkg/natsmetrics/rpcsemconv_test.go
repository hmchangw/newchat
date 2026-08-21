package natsmetrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The request families follow the OpenTelemetry RPC semantic conventions rather
// than a name of our own. Two reasons, in order of weight:
//
//  1. The SLO roadmap (docs/load-testing/common/sli-slo.md §8 P1) already asks
//     for `rpc_server_duration_seconds{subject_pattern, errcode_category}`. The
//     intent is right and the spelling is not: the convention names the
//     instrument `rpc.server.call.duration`, the method label `rpc.method`, and
//     the error label `error.type`. Shipping the convention's spelling means the
//     dashboards, the collector processors and anyone's RPC panel understand it
//     without a translation table.
//  2. Semconv reserves `error.type` for calls that failed. Successes carry no
//     such label at all, which removes a label value from the busiest series in
//     the family instead of adding one.
//
// Verified against go.opentelemetry.io/otel/semconv/v1.40.0/rpcconv (instrument
// names, `s` unit, `rpc.system.name`) and the generator's own boundary table in
// semconv/templates/registry/go/weaver.yaml.
func TestRequestInstrumentsFollowRPCSemanticConventions(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	p := m.Publisher("s1")

	p.Request(ctx, OperationHistoryRead, 12*time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, 8*time.Millisecond, RequestSuccess)
	rm := collect(t, reader)

	client := histogramPoints(t, rm, "rpc.client.call.duration")
	require.Len(t, client, 1)
	assert.Equal(t, "nats", attrsOf(client[0])["rpc.system.name"])
	assert.Equal(t, "history_read", attrsOf(client[0])["rpc.method"])

	server := histogramPoints(t, rm, "rpc.server.call.duration")
	require.Len(t, server, 1)
	assert.Equal(t, "nats", attrsOf(server[0])["rpc.system.name"])
	assert.Equal(t, "room_read", attrsOf(server[0])["rpc.method"])
}

// A successful call must not carry error.type. The convention makes the label
// conditionally required — present only if the operation ended in error — so
// its absence is what tells a query that a call succeeded.
func TestRequestInstruments_SuccessCarriesNoErrorType(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	p := m.Publisher("s1")

	p.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestSuccess)
	rm := collect(t, reader)

	for _, name := range []string{"rpc.client.call.duration", "rpc.server.call.duration"} {
		points := histogramPoints(t, rm, name)
		require.Len(t, points, 1, name)
		assert.NotContains(t, attrsOf(points[0]), "error.type",
			"%s: a successful call must not be labelled with an error class", name)
	}
}

// A failed call carries the bounded class it failed with, from the same closed
// vocabularies the old `outcome`/`result` labels used.
func TestRequestInstruments_FailureCarriesBoundedErrorType(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	p := m.Publisher("s1")

	p.Request(ctx, OperationHistoryRead, time.Millisecond, nats.ErrTimeout)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestNotFound)
	rm := collect(t, reader)

	client := histogramPoints(t, rm, "rpc.client.call.duration")
	require.Len(t, client, 1)
	assert.Equal(t, "timeout", attrsOf(client[0])["error.type"])

	server := histogramPoints(t, rm, "rpc.server.call.duration")
	require.Len(t, server, 1)
	assert.Equal(t, "not_found", attrsOf(server[0])["error.type"])
}

// The paired counters are gone. A histogram already publishes its own count, so
// `chat_nats_requests_total` was `rpc_client_call_duration_seconds_count` under
// a second name, on a second series, built from a second attribute set.
func TestRequestCountersAreReplacedByTheHistogramCount(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	p := m.Publisher("s1")

	p.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	p.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestSuccess)
	rm := collect(t, reader)

	for _, gone := range []string{"chat.nats.requests", "chat.nats.request.duration",
		"chat.nats.request.handled", "chat.nats.request.handler.duration"} {
		assert.Empty(t, metricPoints[int64](t, rm, gone), "%s must no longer be recorded", gone)
		assert.Empty(t, histogramPoints(t, rm, gone), "%s must no longer be recorded", gone)
	}

	client := histogramPoints(t, rm, "rpc.client.call.duration")
	require.Len(t, client, 1)
	assert.Equal(t, uint64(2), client[0].Count, "the histogram count is the request count")
}

// Buckets come from the convention's own table, not from our generic latency
// boundaries: an RPC panel that assumes the standard boundaries reads the
// wrong quantile otherwise.
func TestRequestInstruments_UseSemconvBucketBoundaries(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	p := m.Publisher("s1")

	p.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestSuccess)
	rm := collect(t, reader)

	want := []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}
	for _, name := range []string{"rpc.client.call.duration", "rpc.server.call.duration"} {
		points := histogramPoints(t, rm, name)
		require.Len(t, points, 1, name)
		assert.Equal(t, want, points[0].Bounds, "%s must use the semconv boundaries", name)
	}
}

// The exported Prometheus names are what a dashboard actually queries, and the
// exporter derives them from the instrument name plus its unit.
func TestRequestInstruments_ExportUnderSemconvPrometheusNames(t *testing.T) {
	m, reg := newPrometheusExportSetup(t)
	ctx := context.Background()
	p := m.Publisher("s1")
	p.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestNotFound)

	families, err := reg.Gather()
	require.NoError(t, err, "gather must succeed — a failure here is the /metrics 500")

	names := map[string]bool{}
	for _, mf := range families {
		names[mf.GetName()] = true
		if strings.HasPrefix(mf.GetName(), "rpc_") {
			for _, point := range mf.GetMetric() {
				seen := map[string]int{}
				for _, lp := range point.GetLabel() {
					seen[lp.GetName()]++
				}
				for label, count := range seen {
					assert.Equalf(t, 1, count, "family %s repeats label %s", mf.GetName(), label)
				}
			}
		}
	}
	assert.True(t, names["rpc_client_call_duration_seconds"], "exported families: %v", keysOf(names))
	assert.True(t, names["rpc_server_call_duration_seconds"], "exported families: %v", keysOf(names))
}

func attrsOf(point metricdata.HistogramDataPoint[float64]) map[string]string {
	out := map[string]string{}
	for _, kv := range point.Attributes.ToSlice() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
