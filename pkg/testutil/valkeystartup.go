package testutil

import (
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// NoopObservability satisfies valkeyutil.Observability structurally, without
// this package importing valkeyutil — which it cannot, since valkeyutil's own
// integration tests import this one.
type NoopObservability struct{}

func (NoopObservability) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }
func (NoopObservability) MeterProvider() metric.MeterProvider  { return metricnoop.NewMeterProvider() }

// ValkeyObservability returns a no-op Observability that is NOT nil, which is
// the whole point: valkeyutil skips instrumentation when obs is nil, so a
// startup test passing nil never reaches the hook install — and that is exactly
// how an instrumentation-gated startup reached production unnoticed.
//
// Pass this, never nil, to any test asserting what a service's dial does.
func ValkeyObservability() NoopObservability { return NoopObservability{} }

// DeadValkeyAddr is an address that refuses connections — a Valkey that is down
// while a pod boots.
//
// A fixed low port, not an ephemeral one reserved and released: these guards
// pass whether the dial refuses or connects, so a reused port would make them
// silently vacuous rather than flaky. Port 1 needs root to bind, so nothing
// takes it.
func DeadValkeyAddr(t *testing.T) string {
	t.Helper()
	const addr = "127.0.0.1:1"
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("%s unexpectedly accepts connections; this guard needs a dead address", addr)
	}
	return addr
}
