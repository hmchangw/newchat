//go:build integration

package valkeyutil

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/testutil"
)

// recorderObs satisfies Observability with an in-memory span recorder so the
// test can assert that instrumentation actually emits command spans.
type recorderObs struct {
	tp *trace.TracerProvider
}

func (r recorderObs) TracerProvider() oteltrace.TracerProvider { return r.tp }
func (r recorderObs) MeterProvider() metric.MeterProvider      { return metricnoop.NewMeterProvider() }

func TestInstrumentCluster_RecordsCommandSpan(t *testing.T) {
	// Per-test cluster: instrumentCluster mutates the client's hook chain, and
	// hooks stacked on the shared client would leak into sibling tests (node
	// connections created under one test's hooks keep serving later tests).
	c := testutil.StartValkeyCluster(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// Exercise the exact instrumentation path ConnectCluster uses. The
	// container client is built with a ClusterSlots override (ConnectCluster's
	// auto-discovery can't reach it), so wrap it directly here.
	require.NoError(t, instrumentCluster(c, newConnectConfig(WithObservability(recorderObs{tp: tp}))))
	client := &clusterClient{c: c}

	ctx := context.Background()
	require.NoError(t, client.Set(ctx, "obs-k", "v", time.Hour))
	val, err := client.Get(ctx, "obs-k")
	require.NoError(t, err)
	require.Equal(t, "v", val)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one Valkey command span")

	// o11y/redis names command spans "redis.{OPERATION}" (e.g. "redis.GET").
	var sawGet bool
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "redis.GET") {
			sawGet = true
		}
	}
	assert.True(t, sawGet, "expected a GET command span, got %v", spanNames(spans))
}

func TestInstrumentCluster_RequireParentSpan(t *testing.T) {
	// Per-test cluster for the same hook-isolation reason as above.
	c := testutil.StartValkeyCluster(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	require.NoError(t, instrumentCluster(c, newConnectConfig(
		WithObservability(recorderObs{tp: tp}),
		WithRequireParentSpan(true),
	)))
	client := &clusterClient{c: c}

	require.NoError(t, client.Set(context.Background(), "obs-no-parent", "v", time.Hour))
	assert.Empty(t, exporter.GetSpans(), "unparented Valkey command should be suppressed")

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	got, err := client.Get(ctx, "obs-no-parent")
	require.NoError(t, err)
	require.Equal(t, "v", got)
	span.End()

	names := spanNames(exporter.GetSpans())
	assert.Contains(t, names, "parent")
	assert.Contains(t, names, "redis.GET")
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
