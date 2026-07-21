package searchengine

import (
	"context"
	"net/http"
	"testing"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// *o11y.SDK must satisfy the minimal Observability interface so services pass
// the SDK directly without searchengine importing the concrete type.
var _ Observability = (*o11y.SDK)(nil)

type fakeObs struct{}

func (fakeObs) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }

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
