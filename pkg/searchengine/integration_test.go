//go:build integration

package searchengine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// recorderObs satisfies Observability with an in-memory span recorder so the
// test can assert that the adapter emits ES-semantic spans.
type recorderObs struct {
	tp *trace.TracerProvider
}

func (r recorderObs) TracerProvider() oteltrace.TracerProvider { return r.tp }

func TestNew_WithObservability_RecordsESSemanticSpan(t *testing.T) {
	esURL := testutil.Elasticsearch(t)
	index := testutil.ElasticsearchIndex(t, "searchengineobs")

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx := context.Background()
	engine, err := New(ctx, Config{Backend: "elasticsearch", URL: esURL},
		WithObservability(recorderObs{tp: tp}))
	require.NoError(t, err)

	_, err = engine.Search(ctx, []string{index}, json.RawMessage(`{"query":{"match_all":{}}}`))
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one Elasticsearch span")

	// o11y/elasticsearch names spans "elasticsearch.{op} {index}" and tags them
	// with the database semconv. Assert the search span carries both, proving
	// the adapter drives the first-party instrumentation around its raw request.
	var search *tracetest.SpanStub
	for i := range spans {
		if strings.HasPrefix(spans[i].Name, "elasticsearch.search") {
			search = &spans[i]
		}
	}
	require.NotNil(t, search, "expected an elasticsearch.search span, got %v", spanNames(spans))
	assert.Contains(t, search.Name, index, "span name should carry the target index")
	assert.Equal(t, "elasticsearch", attrValue(search.Attributes, "db.system"))
	assert.Equal(t, "search", attrValue(search.Attributes, "db.operation"))
	assert.Equal(t, index, attrValue(search.Attributes, "db.elasticsearch.path_parts.index"))
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}
