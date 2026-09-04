package searchengine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// client's product check requires, so a search reaches the instrumentation.
func esStubServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0},"hits":[]}}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The o11y Elasticsearch integration owns a db.client.operation.duration
// histogram; searchengine must route it to the caller's SDK MeterProvider so ES
// latency lands beside the Mongo and Cassandra wrappers' equivalents.
func TestNewESClient_RecordsOperationDurationOnSuppliedMeterProvider(t *testing.T) {
	srv := esStubServer(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	client, err := newESClient(&elasticsearch.Config{Addresses: []string{srv.URL}}, meterObs{mp: mp})
	require.NoError(t, err)

	res, err := client.Search(client.Search.WithContext(context.Background()), client.Search.WithIndex("rooms"))
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	hist := findHistogram(rm, o11yESScope, "db.client.operation.duration")
	require.NotNil(t, hist, "expected the o11y ES operation-duration histogram on the supplied MeterProvider")
	require.Len(t, hist.DataPoints, 1)

	// The index label is left on (o11y's default). Our message indices roll
	// monthly, so this is a deliberate cardinality choice recorded in
	// docs/specs/o11y/storage-dependency-metrics.md §4 — pinned here so flipping
	// it is a visible decision rather than a silent one.
	index, ok := hist.DataPoints[0].Attributes.Value("db.collection.name")
	require.True(t, ok, "db.collection.name must stay on the metric for a single-index request")
	assert.Equal(t, "rooms", index.AsString())
}

// o11yESScope is the instrumentation scope the o11y elasticsearch package
// records its metrics under. Spans keep the upstream "elasticsearch-api" scope.
const o11yESScope = "github.com/flywindy/o11y/elasticsearch"

func findHistogram(rm metricdata.ResourceMetrics, scope, name string) *metricdata.Histogram[float64] {
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != scope {
			continue
		}
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
				return &h
			}
		}
	}
	return nil
}
