package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// clearEnv unsets every variable obs reads so each test starts from a known
// baseline regardless of the ambient environment.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"OTEL_SERVICE_NAME", "SERVICE_VERSION", "DEPLOY_ENV", "SERVICE_NAMESPACE",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_PROMETHEUS_HOST", "OTEL_EXPORTER_PROMETHEUS_PORT",
		"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG",
		"O11Y_TRACE_ENABLED", "O11Y_METRICS_ENABLED", "O11Y_LOG_ENABLED", "O11Y_PROFILING_ENABLED",
	} {
		t.Setenv(k, "")
		require.NoError(t, os.Unsetenv(k))
	}
}

func TestParseConfig_Sampler(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "svc")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "parentbased_traceidratio", cfg.TracesSampler)
	assert.InDelta(t, 0.25, cfg.TracesSamplerArg, 1e-9)
}

// Default (no OTEL_TRACES_SAMPLER) samples every root span (100%).
func TestInit_SamplerDefaultSamples(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "sampler-default-svc")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := sdk.Tracer("t").Start(context.Background(), "root")
	defer span.End()
	assert.True(t, span.SpanContext().IsSampled(), "default sampler must sample")
}

// OTEL_TRACES_SAMPLER=always_off drops every span (the env now actually wires
// the SDK sampler through pkg/obs).
func TestInit_SamplerAlwaysOff(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "sampler-off-svc")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := sdk.Tracer("t").Start(context.Background(), "root")
	defer span.End()
	assert.False(t, span.SpanContext().IsSampled(), "always_off must not sample")
}

// A parentbased_traceidratio of 0 drops every root span.
func TestInit_SamplerRatioZero(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "sampler-ratio0-svc")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := sdk.Tracer("t").Start(context.Background(), "root")
	defer span.End()
	assert.False(t, span.SpanContext().IsSampled(), "ratio 0 must not sample")
}

func TestParseConfig_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "message-worker")

	cfg, err := parseConfig()
	require.NoError(t, err)

	assert.Equal(t, "message-worker", cfg.ServiceName)
	assert.Equal(t, "dev", cfg.ServiceVersion)
	assert.Equal(t, "development", cfg.Environment)
	assert.Equal(t, "chat", cfg.Namespace)
	assert.Equal(t, "http://localhost:4318", cfg.OTLPEndpoint)
	assert.Equal(t, "2112", cfg.PrometheusPort)
	assert.Empty(t, cfg.PrometheusHost)
	assert.Empty(t, cfg.OTLPHeaders)
}

func TestParseConfig_DefaultsServiceName(t *testing.T) {
	clearEnv(t)

	// OTEL_SERVICE_NAME is no longer required: a missing env degrades to a
	// visible placeholder rather than a startup crash-loop.
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "unknown-service", cfg.ServiceName)
}

func TestParseConfig_Overrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "auth-service")
	t.Setenv("SERVICE_VERSION", "1.2.3")
	t.Setenv("DEPLOY_ENV", "production")
	t.Setenv("SERVICE_NAMESPACE", "chat-prod")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_HOST", "0.0.0.0")
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_PORT", "9464")

	cfg, err := parseConfig()
	require.NoError(t, err)

	assert.Equal(t, "auth-service", cfg.ServiceName)
	assert.Equal(t, "1.2.3", cfg.ServiceVersion)
	assert.Equal(t, "production", cfg.Environment)
	assert.Equal(t, "chat-prod", cfg.Namespace)
	assert.Equal(t, "http://collector:4318", cfg.OTLPEndpoint)
	assert.Equal(t, "0.0.0.0", cfg.PrometheusHost)
	assert.Equal(t, "9464", cfg.PrometheusPort)
}

func TestParseConfig_Headers(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "svc")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer abc,x-tenant=acme")

	cfg, err := parseConfig()
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"authorization": "Bearer abc",
		"x-tenant":      "acme",
	}, cfg.OTLPHeaders)
}

func TestConfig_MetricsAddr(t *testing.T) {
	tests := []struct {
		name string
		host string
		port string
		want string
	}{
		{"port only", "", "2112", ":2112"},
		{"host and port", "0.0.0.0", "9464", "0.0.0.0:9464"},
		{"random port", "", "0", ":0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{PrometheusHost: tc.host, PrometheusPort: tc.port}
			assert.Equal(t, tc.want, c.metricsAddr())
		})
	}
}

func TestConfig_Options_HeadersOptional(t *testing.T) {
	base := Config{
		ServiceName: "svc", ServiceVersion: "dev", Environment: "development",
		Namespace: "chat", OTLPEndpoint: "http://localhost:4318", PrometheusPort: "2112",
	}
	withoutHeaders := base.options()

	withHeaders := base
	withHeaders.OTLPHeaders = map[string]string{"a": "b"}

	assert.Len(t, withoutHeaders, 6)
	assert.Len(t, withHeaders.options(), 7, "WithOTLPHeaders should be appended only when headers are set")
}

// testEnv sets the minimum env for a successful Init with a random metrics port
// (so parallel/sequential tests never collide on :2112).
func testEnv(t *testing.T, name string) {
	t.Helper()
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", name)
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_PORT", "0")
}

func TestInit_InstallsGlobals(t *testing.T) {
	testEnv(t, "globals-svc")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sdk)
	require.NotNil(t, shutdown)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	assert.Same(t, sdk.TracerProvider(), otel.GetTracerProvider(),
		"obs.Init must install the SDK tracer provider as the OTel global")
	assert.Equal(t, sdk.Propagator, otel.GetTextMapPropagator(),
		"obs.Init must install the SDK propagator as the OTel global")
	assert.Same(t, sdk.Logger, slog.Default(),
		"obs.Init must set the SDK logger as the slog default")
}

func TestInit_DefaultsMissingServiceName(t *testing.T) {
	clearEnv(t)

	// A missing OTEL_SERVICE_NAME must NOT crash startup — Init succeeds with the
	// placeholder service name (production is expected to set it explicitly).
	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sdk)
	require.NotNil(t, shutdown)
	t.Cleanup(func() { _ = shutdown(context.Background()) })
}

func TestInit_InvalidEnvironment(t *testing.T) {
	testEnv(t, "bad-env-svc")
	t.Setenv("DEPLOY_ENV", "not-a-real-env")

	sdk, shutdown, err := Init(context.Background())
	require.Error(t, err)
	assert.Nil(t, sdk)
	assert.Nil(t, shutdown)
}

func TestInit_ShutdownIdempotent(t *testing.T) {
	testEnv(t, "shutdown-svc")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)

	assert.NoError(t, shutdown(context.Background()))
	assert.NoError(t, shutdown(context.Background()), "shutdown must be idempotent")
}

func TestInit_TogglesRespectEnv(t *testing.T) {
	testEnv(t, "toggle-svc")
	t.Setenv("O11Y_TRACE_ENABLED", "false")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	assert.False(t, sdk.Toggles.Trace, "O11Y_TRACE_ENABLED=false must disable the trace pillar")
}

// TestInit_LogTraceCorrelation is the Phase 1 package acceptance: a stdout JSON
// log line emitted inside an active span must carry traceId, spanId,
// service.name, and the caller-supplied request_id together in one record.
func TestInit_LogTraceCorrelation(t *testing.T) {
	testEnv(t, "correlation-svc")
	t.Setenv("O11Y_METRICS_ENABLED", "false")

	// The SDK captures os.Stdout at Init time, so redirect before Init.
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	restore := func() string {
		_ = w.Close()
		os.Stdout = orig
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		return buf.String()
	}

	sdk, shutdown, initErr := Init(context.Background())
	require.NoError(t, initErr)
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	require.True(t, sdk.Toggles.Trace, "trace pillar must be on for this test")

	ctx, span := sdk.Tracer("test").Start(context.Background(), "op")
	wantTrace := span.SpanContext().TraceID().String()
	wantSpan := span.SpanContext().SpanID().String()
	sdk.Logger.InfoContext(ctx, "hello", slog.String("request_id", "req-123"))
	span.End()

	out := restore()
	require.NotEmpty(t, out, "expected a stdout log line")

	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(lastJSONLine(out)), &rec))

	assert.Equal(t, "correlation-svc", rec["service.name"])
	assert.Equal(t, "req-123", rec["request_id"])
	assert.Equal(t, wantTrace, rec["traceId"])
	assert.Equal(t, wantSpan, rec["spanId"])
}

func lastJSONLine(s string) string {
	last := ""
	for _, line := range bytes.Split([]byte(s), []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			last = string(line)
		}
	}
	return last
}
