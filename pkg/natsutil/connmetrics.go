package natsutil

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const connMetricsScope = "github.com/hmchangw/chat/pkg/natsutil"

// connMetrics reports the client's own view of its broker connection.
//
// Without it a broker outage and a wedged consumer loop look identical: the
// JetStream consumer-loop gauge can stay at one while the client is detached,
// and a Core NATS publish keeps returning nil because nats.go buffers across a
// disconnect. Reconnect-buffer overflow — the one condition that means
// client-side message loss — is not reported here: nats.go returns
// ErrReconnectBufExceeded synchronously from Publish and never routes it
// through ErrorHandler, so it is counted at the publish boundary instead, as
// chat.nats.publish.attempts{outcome="buffer_full"}.
//
// service_name and site are not labels here: they come from the OTel resource
// that pkg/obs installs, which is also how nats_slow_consumer_events_total is
// scoped. Adding them per-series would duplicate the resource and drift from it.
type connMetrics struct {
	connected metric.Int64UpDownCounter
	events    metric.Int64Counter

	// gaugeOpt carries no attributes: the gauge is one series per process.
	gaugeOpt        metric.MeasurementOption
	connectedOpt    metric.MeasurementOption
	disconnectedOpt metric.MeasurementOption
	reconnectedOpt  metric.MeasurementOption
	closedOpt       metric.MeasurementOption
	asyncErrorOpt   metric.MeasurementOption
}

// connMetricState owns one connection's lifecycle state. Multiple instrumented
// connections share the same OTel instruments but transition independently, so
// the up/down counter reports the number of live connections in the process.
type connMetricState struct {
	metrics *connMetrics
	up      atomic.Bool
}

func newConnMetrics(meter metric.Meter) *connMetrics {
	noopMeter := noop.NewMeterProvider().Meter("nats")
	connected, err := meter.Int64UpDownCounter("chat.nats.client.connected",
		metric.WithDescription("Number of live NATS broker connections in this process."))
	if err != nil {
		connected, _ = noopMeter.Int64UpDownCounter("chat.nats.client.connected")
	}
	events, err := meter.Int64Counter("chat.nats.client.connection.events",
		metric.WithDescription("NATS client connection lifecycle transitions by bounded event."))
	if err != nil {
		events, _ = noopMeter.Int64Counter("chat.nats.client.connection.events")
	}
	event := func(value string) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String("event", value))
	}
	return &connMetrics{
		connected:       connected,
		events:          events,
		gaugeOpt:        metric.WithAttributes(),
		connectedOpt:    event("connected"),
		disconnectedOpt: event("disconnected"),
		reconnectedOpt:  event("reconnected"),
		closedOpt:       event("closed"),
		asyncErrorOpt:   event("async_error"),
	}
}

func (c *connMetrics) Connection() *connMetricState {
	if c == nil {
		return nil
	}
	return &connMetricState{metrics: c}
}

// Connected records the first successful connect. Later reconnects go through
// Reconnected so the two are distinguishable in the events counter.
func (c *connMetricState) Connected(ctx context.Context) {
	if c == nil {
		return
	}
	c.markUp(ctx, c.metrics.connectedOpt)
}

func (c *connMetricState) Reconnected(ctx context.Context) {
	if c == nil {
		return
	}
	c.markUp(ctx, c.metrics.reconnectedOpt)
}

// Disconnected takes err only to keep the nats.DisconnectErrHandler signature
// honest at the call site; the text is never a label.
func (c *connMetricState) Disconnected(ctx context.Context, _ error) {
	if c == nil {
		return
	}
	c.markDown(ctx, c.metrics.disconnectedOpt)
}

func (c *connMetricState) Closed(ctx context.Context) {
	if c == nil {
		return
	}
	c.markDown(ctx, c.metrics.closedOpt)
}

// AsyncError counts the connection-level async errors nats.go reports through
// ErrorHandler. The error text is never a label: it is unbounded, and slow
// consumers — the one async error worth separating — already have their own
// dedicated counter.
func (c *connMetricState) AsyncError(ctx context.Context, _ error) {
	if c == nil {
		return
	}
	c.metrics.events.Add(ctx, 1, c.metrics.asyncErrorOpt)
}

func (c *connMetricState) markUp(ctx context.Context, opt metric.MeasurementOption) {
	if c == nil {
		return
	}
	c.metrics.events.Add(ctx, 1, opt)
	if c.up.CompareAndSwap(false, true) {
		c.metrics.connected.Add(ctx, 1, c.metrics.gaugeOpt)
	}
}

func (c *connMetricState) markDown(ctx context.Context, opt metric.MeasurementOption) {
	if c == nil {
		return
	}
	c.metrics.events.Add(ctx, 1, opt)
	if c.up.CompareAndSwap(true, false) {
		c.metrics.connected.Add(ctx, -1, c.metrics.gaugeOpt)
	}
}
