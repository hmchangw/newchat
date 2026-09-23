package testutil

import (
	"net"
	"testing"

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

// DeadValkeyAddr reserves and frees a port, yielding an address that refuses
// connections — a Valkey that is down while a pod boots.
//
// Refused rather than blackholed on purpose: deterministic and fast, and the
// property under test is that the dial does not fail, not how long it takes.
func DeadValkeyAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a dead address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("free the dead address: %v", err)
	}
	return addr
}
