package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
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
		"O11Y_ENABLED", "O11Y_USER_BAGGAGE_ENABLED",
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
	testEnv(t, "sampler-default-svc") // o11y enabled

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
	testEnv(t, "sampler-off-svc") // o11y enabled
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
	testEnv(t, "sampler-ratio0-svc") // o11y enabled
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
	assert.False(t, cfg.Enabled, "O11Y_ENABLED must default to false (zero-impact master switch)")
	assert.False(t, cfg.UserBaggageEnabled, "user.name baggage must remain an explicit PII opt-in")
}

func TestParseConfig_UserBaggageOptIn(t *testing.T) {
	clearEnv(t)
	t.Setenv("O11Y_USER_BAGGAGE_ENABLED", "true")

	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.True(t, cfg.UserBaggageEnabled)
}

// With the master switch off (default, no env), Init still succeeds and returns
// a usable SDK, but everything is a no-op: spans are not recorded/sampled. This
// is the zero-impact default — o11y builds only noop providers + a stdout logger.
func TestInit_MasterSwitchDisabledIsNoop(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "disabled-svc")
	// O11Y_ENABLED deliberately unset → defaults to false.

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sdk)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := sdk.Tracer("t").Start(context.Background(), "root")
	defer span.End()
	assert.False(t, span.IsRecording(), "master-off must produce non-recording (noop) spans")
	assert.False(t, span.SpanContext().IsSampled(), "master-off must not sample")
}

// options() force-disables all four pillars when the master switch is off.
func TestOptions_MasterOffDisablesPillars(t *testing.T) {
	off := Config{ServiceName: "svc", PrometheusPort: "2112", Enabled: false}
	on := Config{ServiceName: "svc", PrometheusPort: "2112", Enabled: true}
	// Master-off omits baggage materialization and appends four pillar disables.
	assert.Len(t, off.options(), 10, "master-off must append the four WithXxxEnabled(false) opts")
	assert.Len(t, on.options(), 7, "master-on with no sampler/headers adds no extra opts")
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
		Enabled: true, // isolate the headers behavior from the master-off pillar opts
	}
	withoutHeaders := base.options()

	withHeaders := base
	withHeaders.OTLPHeaders = map[string]string{"a": "b"}
	withUser := base
	withUser.UserBaggageEnabled = true

	assert.Len(t, withoutHeaders, 7, "room/site baggage materialization is always configured")
	assert.Len(t, withHeaders.options(), 8, "WithOTLPHeaders should be appended only when headers are set")
	assert.Len(t, withUser.options(), 8, "WithUserBaggage should be appended only after the PII opt-in")
}

func TestContextWithIdentity_RecordsBaggageAndCurrentSpanAttributes(t *testing.T) {
	testEnv(t, "identity-svc")
	t.Setenv("O11Y_METRICS_ENABLED", "false")
	t.Setenv("O11Y_USER_BAGGAGE_ENABLED", "true")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "entry")

	ctx = ContextWithIdentity(ctx, "alice", "room-42", "site-a")
	span.End()

	bag := baggage.FromContext(ctx)
	assert.Equal(t, "alice", bag.Member("user.name").Value())
	assert.Equal(t, "room-42", bag.Member(RoomIDKey).Value())
	assert.Equal(t, "site-a", bag.Member(SiteIDKey).Value())

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	attrs := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	assert.Equal(t, "alice", attrs["user.name"])
	assert.Equal(t, "room-42", attrs[RoomIDKey])
	assert.Equal(t, "site-a", attrs[SiteIDKey])
}

func TestContextWithIdentity_UserBaggageDisabled(t *testing.T) {
	testEnv(t, "identity-user-off-svc")
	t.Setenv("O11Y_METRICS_ENABLED", "false")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, err := o11y.ContextWithUser(context.Background(), "forged-user")
	require.NoError(t, err)
	ctx, span := tp.Tracer("test").Start(ctx, "entry")
	defer span.End()

	ctx = ContextWithIdentity(ctx, "alice", "room-42", "site-a")
	bag := baggage.FromContext(ctx)
	assert.Empty(t, bag.Member("user.name").Value())
	assert.Equal(t, "room-42", bag.Member(RoomIDKey).Value())
}

func TestContextWithIdentity_RejectsOversizedValuesAndClearsSupersededBaggage(t *testing.T) {
	testEnv(t, "identity-bounds-svc")
	t.Setenv("O11Y_TRACE_ENABLED", "false")
	t.Setenv("O11Y_METRICS_ENABLED", "false")
	t.Setenv("O11Y_LOG_ENABLED", "false")
	t.Setenv("O11Y_USER_BAGGAGE_ENABLED", "true")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	ctx, err := o11y.ContextWithUser(context.Background(), "forged-user")
	require.NoError(t, err)
	ctx, err = o11y.ContextWithBaggageValue(ctx, RoomIDKey, "forged-room")
	require.NoError(t, err)
	tooLong := strings.Repeat("x", o11y.MaxBaggageValueBytes+1)
	ctx = ContextWithIdentity(ctx, tooLong, tooLong, "")
	bag := baggage.FromContext(ctx)
	assert.Empty(t, bag.Member("user.name").Value())
	assert.Empty(t, bag.Member(RoomIDKey).Value())
}

// TestContextWithPublicIdentity_DropsForgedManagedKeys covers the ingress a
// client reaches directly: every managed key the caller supplied must go, even
// the ones this boundary has no trusted replacement for, and the entry span's
// already-materialized attributes must be zeroed with them.
func TestContextWithPublicIdentity_DropsForgedManagedKeys(t *testing.T) {
	testEnv(t, "public-identity-svc")
	t.Setenv("O11Y_METRICS_ENABLED", "false")
	t.Setenv("O11Y_USER_BAGGAGE_ENABLED", "true")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, err := o11y.ContextWithUser(context.Background(), "forged-user")
	require.NoError(t, err)
	ctx, err = o11y.ContextWithBaggageValue(ctx, RoomIDKey, "forged-room")
	require.NoError(t, err)
	ctx, err = o11y.ContextWithBaggageValue(ctx, SiteIDKey, "forged-site")
	require.NoError(t, err)

	// The entry span starts with the forged baggage already in context, the way
	// the SDK's OnStart processor sees it on a real NATS consumer span.
	ctx, span := tp.Tracer("test").Start(ctx, "entry")
	for _, key := range ManagedBaggageKeys() {
		span.SetAttributes(attribute.String(key, "forged"))
	}

	// The route supplies account and room but not site — the case a static
	// subject token creates, where the old behavior left the forged value.
	ctx = ContextWithPublicIdentity(ctx, "alice", "room-42", "")
	span.End()

	bag := baggage.FromContext(ctx)
	assert.Equal(t, "alice", bag.Member("user.name").Value())
	assert.Equal(t, "room-42", bag.Member(RoomIDKey).Value())
	assert.Empty(t, bag.Member(SiteIDKey).Value(), "an unsupplied key must be dropped, not inherited from the caller")

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	attrs := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	assert.Equal(t, "alice", attrs["user.name"])
	assert.Equal(t, "room-42", attrs[RoomIDKey])
	assert.Empty(t, attrs[SiteIDKey], "the forged value must not survive on the entry span either")
}

// Public ingress sanitization is a trust-boundary guarantee, not a telemetry
// emission feature. It must still run when the o11y master switch is off but
// NATS propagation has been enabled independently by env or relay.
func TestContextWithPublicIdentity_MasterOffStillDropsManagedKeys(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "public-identity-master-off-svc")

	_, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, shutdown(context.Background())) })

	forged, err := baggage.New(
		mustBaggageMember(t, o11y.UserNameKey, "forged-user"),
		mustBaggageMember(t, RoomIDKey, "forged-room"),
		mustBaggageMember(t, SiteIDKey, "forged-site"),
		mustBaggageMember(t, "tenant.id", "trusted-upstream-value"),
	)
	require.NoError(t, err)
	ctx := baggage.ContextWithBaggage(context.Background(), forged)

	ctx = ContextWithPublicIdentity(ctx, "alice", "room-42", "site-a")
	bag := baggage.FromContext(ctx)
	assert.Empty(t, bag.Member(o11y.UserNameKey).Value())
	assert.Empty(t, bag.Member(RoomIDKey).Value())
	assert.Empty(t, bag.Member(SiteIDKey).Value())
	assert.Equal(t, "trusted-upstream-value", bag.Member("tenant.id").Value(),
		"sanitization must remove only application-managed identity keys")
}

func mustBaggageMember(t *testing.T, key, value string) baggage.Member {
	t.Helper()
	member, err := baggage.NewMember(key, value)
	require.NoError(t, err)
	return member
}

// TestUnsuppliedManagedKeys guards the drift the public-ingress optimization
// introduces: it clears only the keys no trusted value will overwrite, so a
// managed key this helper forgets would stay caller-controlled.
func TestUnsuppliedManagedKeys(t *testing.T) {
	assert.ElementsMatch(t, ManagedBaggageKeys(), unsuppliedManagedKeys("", "", ""),
		"with no trusted values every managed key must be cleared")
	assert.Empty(t, unsuppliedManagedKeys("alice", "room-42", "site-a"),
		"a fully supplied identity leaves nothing to pre-clear")
	assert.Equal(t, []string{SiteIDKey}, unsuppliedManagedKeys("alice", "room-42", ""))
	assert.Equal(t, []string{o11y.UserNameKey, RoomIDKey}, unsuppliedManagedKeys("", "", "site-a"))
}

// TestManagedBaggageKeys_MatchesMaterializedKeys guards the single source of
// truth: a key registered for materialization but missing from the clear list
// would be forgeable at public ingress.
func TestManagedBaggageKeys_MatchesMaterializedKeys(t *testing.T) {
	assert.ElementsMatch(t, []string{o11y.UserNameKey, RoomIDKey, SiteIDKey}, ManagedBaggageKeys())
	assert.NotSame(t, &managedBaggageKeys, &[]string{}, "callers must not be able to mutate the package list")

	got := ManagedBaggageKeys()
	got[0] = "mutated"
	assert.NotEqual(t, "mutated", ManagedBaggageKeys()[0], "ManagedBaggageKeys must return a copy")
}

func TestPublicIngressPropagator_IgnoresUntrustedBaggage(t *testing.T) {
	carrier := propagation.MapCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
		"baggage":     "user.name=forged-user,chat.room.id=forged-room",
	}

	ctx := PublicIngressPropagator().Extract(context.Background(), carrier)
	assert.True(t, oteltrace.SpanContextFromContext(ctx).IsValid(), "trusted trace context must still propagate")
	assert.Empty(t, baggage.FromContext(ctx).Members(), "public ingress must not accept caller-controlled baggage")
	assert.ElementsMatch(t, []string{"traceparent", "tracestate"}, PublicIngressPropagator().Fields())
}

// testEnv sets the minimum env for a successful Init with a random metrics port
// (so parallel/sequential tests never collide on :2112).
func testEnv(t *testing.T, name string) {
	t.Helper()
	clearEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", name)
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_PORT", "0")
	t.Setenv("O11Y_ENABLED", "true") // master switch: tests that assert real SDK behavior
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

type markerHandler struct{ slog.Handler }

func TestInitWithLoggerHandler_WrapsSDKLogger(t *testing.T) {
	testEnv(t, "wrapped-logger-svc")

	var wrappedBase slog.Handler
	sdk, shutdown, err := InitWithLoggerHandler(context.Background(), slog.LevelDebug, func(h slog.Handler) slog.Handler {
		wrappedBase = h
		return &markerHandler{Handler: h}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	assert.Same(t, sdk.Logger.Handler(), wrappedBase)
	assert.True(t, wrappedBase.Enabled(context.Background(), slog.LevelDebug))
	assert.IsType(t, &markerHandler{}, slog.Default().Handler())
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
	// This test verifies lifecycle idempotence, not exporter reachability.
	t.Setenv("O11Y_TRACE_ENABLED", "false")
	t.Setenv("O11Y_LOG_ENABLED", "false")

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
