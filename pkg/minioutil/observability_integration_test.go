//go:build integration

package minioutil

import (
	"context"
	"strings"
	"testing"

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
// test can assert that instrumentation actually emits operation spans.
type recorderObs struct {
	tp *trace.TracerProvider
}

func (r recorderObs) TracerProvider() oteltrace.TracerProvider { return r.tp }
func (r recorderObs) MeterProvider() metric.MeterProvider      { return metricnoop.NewMeterProvider() }

func TestConnect_WithObservability_RecordsOperationSpan(t *testing.T) {
	// testutil.MinIO provisions a per-test bucket on the shared container;
	// MinIOEndpoint hands us the dial info so Connect builds its own
	// instrumented client against that same container.
	_, bucket := testutil.MinIO(t, "minioutilobs")
	endpoint, accessKey, secretKey := testutil.MinIOEndpoint(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := context.Background()
	client, err := Connect(ctx, endpoint, false, accessKey, secretKey,
		WithObservability(recorderObs{tp: tp}))
	require.NoError(t, err)

	type doc struct {
		Name string `json:"name"`
	}
	b, err := NewBucket[doc](ctx, client, bucket)
	require.NoError(t, err)
	require.NoError(t, b.Put(ctx, "obs-key", doc{Name: "Alice"}))

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one MinIO operation span")

	// o11y/minio names operation spans "s3.{Operation} {bucket}"
	// (e.g. "s3.PutObject mybucket").
	var sawPut bool
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "s3.PutObject") {
			sawPut = true
		}
	}
	assert.True(t, sawPut, "expected a PutObject span, got %v", spanNames(spans))
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
