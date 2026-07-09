//go:build integration

package cassutil

import (
	"context"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// recorderObs satisfies Observability with an in-memory span recorder so the
// test can assert that instrumentation actually emits query spans.
type recorderObs struct {
	tp *trace.TracerProvider
}

func (r recorderObs) TracerProvider() oteltrace.TracerProvider { return r.tp }
func (r recorderObs) MeterProvider() metric.MeterProvider      { return metricnoop.NewMeterProvider() }

func TestConnect_WithObservability_RecordsQuerySpan(t *testing.T) {
	keyspace, admin, host := testutil.CassandraKeyspace(t, "cassutil_obs")
	// The admin session has no default keyspace, so the DDL must qualify the table.
	require.NoError(t, admin.Query(
		`CREATE TABLE IF NOT EXISTS `+keyspace+`.items (id text PRIMARY KEY, name text)`,
	).Exec())

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	session, err := Connect(
		Config{Hosts: host, Keyspace: keyspace},
		WithObservability(recorderObs{tp: tp}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { Close(session) })

	var name string
	err = session.Query(`SELECT name FROM items WHERE id = ?`, "missing").
		WithContext(context.Background()).Scan(&name)
	require.ErrorIs(t, err, gocql.ErrNotFound, "expected no rows for a missing id")

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one Cassandra query span")

	// o11y/cassandra names query spans "cassandra.{OPERATION} {table}"
	// (e.g. "cassandra.SELECT items") via its own observer.
	var sawSelect bool
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "cassandra.SELECT") {
			sawSelect = true
		}
	}
	assert.True(t, sawSelect, "expected a SELECT query span, got %v", spanNames(spans))
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
