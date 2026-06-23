//go:build integration

package mongoutil

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
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

func TestConnect_WithObservability_RecordsCommandSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := context.Background()
	client, err := Connect(ctx, testutil.MongoURI(t), "", "", WithObservability(recorderObs{tp: tp}))
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	_, err = client.Database("obs_it").Collection("docs").
		InsertOne(ctx, bson.M{"_id": "d1", "name": "Alice"})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one MongoDB command span")

	// o11y/mongo names command spans "mongodb.{operation} {collection}"
	// (e.g. "mongodb.insert docs") via its own SpanNameFormatter, which is more
	// stable than the contrib's semconv attribute keys across OTel versions.
	var sawInsert bool
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "mongodb.insert") {
			sawInsert = true
		}
	}
	assert.True(t, sawInsert, "expected an insert command span, got %v", spanNames(spans))
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
