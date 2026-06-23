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
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := testutil.SharedValkeyCluster(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// Exercise the exact instrumentation path ConnectCluster uses. The shared
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

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
