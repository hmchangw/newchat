package searchengine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// *o11y.SDK must satisfy the minimal Observability interface so services pass
// the SDK directly without searchengine importing the concrete type.
var _ Observability = (*o11y.SDK)(nil)

type fakeObs struct{}

func (fakeObs) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }
func (fakeObs) MeterProvider() metric.MeterProvider  { return metricnoop.NewMeterProvider() }

func TestNewConnectConfig_NoOptions(t *testing.T) {
	cfg := newConnectConfig()
	assert.Nil(t, cfg.obs, "without options, no instrumentation should be configured")
}

func TestNewConnectConfig_WithObservability(t *testing.T) {
	obs := fakeObs{}
	cfg := newConnectConfig(WithObservability(obs))
	assert.Equal(t, obs, cfg.obs)
}

func TestNewConnectConfig_NilOptionIgnored(t *testing.T) {
	cfg := newConnectConfig(nil, WithObservability(fakeObs{}))
	assert.NotNil(t, cfg.obs, "nil options must be skipped without panicking")
}

// A bare Transporter (the fake used across adapter tests) does not implement
// elastictransport.Instrumented, so newAdapter leaves instr nil and every
// operation must flow through the plain Perform path unchanged. This guards the
// backend-agnostic seam: OpenSearch and un-instrumented clients keep working.
func TestNewAdapter_UninstrumentedTransport_UsesPlainPerform(t *testing.T) {
	var called bool
	ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
		called = true
		assert.Equal(t, "/", req.URL.Path)
		return jsonResponse(200, `{}`), nil
	}}
	a := newAdapter(ft)
	require.Nil(t, a.instr, "fake transport must not be detected as instrumented")

	require.NoError(t, a.Ping(context.Background()))
	assert.True(t, called, "request must still reach the transport on the no-op path")
}

// meterObs satisfies Observability with a real SDK MeterProvider so the test can
// assert that newESClient hands it to the o11y instrumentation. The o11y
// elasticsearch facade never falls back to the global MeterProvider, so a metric
// arriving on this reader proves the wiring rather than an ambient default.
type meterObs struct{ mp *sdkmetric.MeterProvider }

func (meterObs) TracerProvider() trace.TracerProvider  { return tracenoop.NewTracerProvider() }
func (m meterObs) MeterProvider() metric.MeterProvider { return m.mp }

// esStubServer answers every request with the X-Elastic-Product header the
// client's product check requires, and the status the test asks for.
func esStubServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// instrumentedAdapter builds the adapter over an instrumented client pointed at
// srv, the way New does, and returns it with a reader over the MeterProvider the
// client was given. Tests drive the adapter rather than the generated esapi
// methods because httpAdapter.do is the only path this repo ships: it hand-drives
// the instrumentation callbacks around a raw request, so a metric proven through
// client.Search would not prove anything about production traffic.
func instrumentedAdapter(t *testing.T, srv *httptest.Server) (*httpAdapter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	client, err := newESClient(&elasticsearch.Config{Addresses: []string{srv.URL}}, meterObs{mp: mp})
	require.NoError(t, err)

	a := newAdapter(client)
	require.NotNil(t, a.instr, "instrumented client must expose the instrumentation to the adapter")
	return a, reader
}

// The o11y Elasticsearch integration owns a db.client.operation.duration
// histogram; searchengine must route it to the caller's SDK MeterProvider so ES
// latency lands beside the Mongo and Cassandra wrappers' equivalents.
func TestAdapter_RecordsOperationDurationOnSuppliedMeterProvider(t *testing.T) {
	a, reader := instrumentedAdapter(t, esStubServer(t, http.StatusOK,
		`{"took":1,"hits":{"total":{"value":0},"hits":[]}}`))

	_, err := a.Search(context.Background(), []string{"rooms"}, json.RawMessage(`{"query":{"match_all":{}}}`))
	require.NoError(t, err)

	dp := singleDataPoint(t, reader, "db.client.operation.duration")
	assert.Equal(t, "search", attrString(t, dp, "db.operation.name"))

	// The index label is left on (o11y's default). Our message indices roll
	// monthly, so this is a deliberate cardinality choice recorded in
	// docs/specs/o11y/storage-dependency-metrics.md §4 — pinned here so flipping
	// it is a visible decision rather than a silent one.
	assert.Equal(t, "rooms", attrString(t, dp, "db.collection.name"))

	_, hasErr := dp.Attributes.Value("error.type")
	assert.False(t, hasErr, "a 200 must not carry error.type")
}

// searchengine builds the LOW-LEVEL client, whose failure contract is
// esapi.Response.IsError (status > 299) — so every status past 299 is counted,
// including a 404 the caller treats as a routine miss. Two endpoints do exactly
// that, and docs/specs/o11y/storage-dependency-metrics.md §7 excludes their 404s
// from the error-ratio query by db.operation.name, so the operation strings are
// pinned here rather than left to drift.
//
// update_by_query is deliberately absent: its path carries no
// ignore_unavailable, and the adapter fails the caller on any non-200, so a 404
// there is a real failure and must stay in the ratio.
func TestAdapter_RoutineMissIsCountedAsAnError(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		call      func(t *testing.T, a *httpAdapter)
	}{
		{
			name:      "GetDoc miss",
			operation: "get",
			call: func(t *testing.T, a *httpAdapter) {
				t.Helper()
				_, found, err := a.GetDoc(context.Background(), "userroom", "nobody")
				require.NoError(t, err, "a 404 is a miss, not an error, to the caller")
				require.False(t, found)
			},
		},
		{
			name:      "GetIndexMapping miss",
			operation: "indices.get_mapping",
			call: func(t *testing.T, a *httpAdapter) {
				t.Helper()
				body, err := a.GetIndexMapping(context.Background(), "missing-index")
				require.NoError(t, err, "a 404 is a miss, not an error, to the caller")
				require.Nil(t, body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, reader := instrumentedAdapter(t, esStubServer(t, http.StatusNotFound, `{"found":false}`))
			tt.call(t, a)

			dp := singleDataPoint(t, reader, "db.client.operation.duration")
			assert.Equal(t, tt.operation, attrString(t, dp, "db.operation.name"))
			assert.Equal(t, "404", attrString(t, dp, "error.type"))
			// semconv records the ES status as a string, not an int.
			assert.Equal(t, "404", attrString(t, dp, "db.response.status_code"))
		})
	}
}

// A 404 from update_by_query is a real failure — the path carries no
// ignore_unavailable, so a missing index 404s and the adapter returns an error.
// The §7 query must keep counting it, which it does by excluding 404 only for
// the two document-miss operations above.
func TestAdapter_UpdateByQueryMissIsARealFailure(t *testing.T) {
	a, reader := instrumentedAdapter(t, esStubServer(t, http.StatusNotFound,
		`{"error":{"type":"index_not_found_exception"}}`))

	err := a.UpdateByQuery(context.Background(), "missing-index", json.RawMessage(`{"query":{"match_all":{}}}`))
	require.Error(t, err, "a 404 here fails the caller")

	dp := singleDataPoint(t, reader, "db.client.operation.duration")
	assert.Equal(t, "update_by_query", attrString(t, dp, "db.operation.name"))
	assert.Equal(t, "404", attrString(t, dp, "error.type"))
}

// o11yESScope is the instrumentation scope the o11y elasticsearch package
// records its metrics under. Spans keep the upstream "elasticsearch-api" scope.
const o11yESScope = "github.com/flywindy/o11y/elasticsearch"

func singleDataPoint(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != o11yESScope {
			continue
		}
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s must be a float64 histogram", name)
			require.Len(t, h.DataPoints, 1)
			return h.DataPoints[0]
		}
	}
	t.Fatalf("no %s under scope %s", name, o11yESScope)
	return metricdata.HistogramDataPoint[float64]{}
}

func mustValue(t *testing.T, dp metricdata.HistogramDataPoint[float64], key string) attribute.Value {
	t.Helper()
	v, ok := dp.Attributes.Value(attribute.Key(key))
	require.True(t, ok, "expected attribute %s on the sample", key)
	return v
}

func attrString(t *testing.T, dp metricdata.HistogramDataPoint[float64], key string) string {
	t.Helper()
	return mustValue(t, dp, key).AsString()
}
