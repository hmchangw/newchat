package mongoutil

import (
	"testing"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// *o11y.SDK must satisfy the minimal Observability interface so services can
// pass the SDK directly without mongoutil importing the concrete type.
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
