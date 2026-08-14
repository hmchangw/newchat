package natsutil

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// connMetrics reports the client's own view of its broker connection.
//
// Without it a broker outage and a wedged consumer loop look identical: the
// JetStream consumer-loop gauge can stay at one while the client is detached,
// and a Core NATS publish keeps returning nil because nats.go buffers across a
// disconnect. The events counter is the only place a reconnect-buffer overflow
// — the one condition that means client-side message loss — becomes queryable.
//
// service_name and site are not labels here: they come from the OTel resource
// that pkg/obs installs, which is also how nats_slow_consumer_events_total is
// scoped. Adding them per-series would duplicate the resource and drift from it.
type connMetrics struct {
	connected metric.Int64UpDownCounter
	events    metric.Int64Counter
	up        atomic.Bool

	// gaugeOpt carries no attributes: the gauge is one series per process.
	gaugeOpt        metric.MeasurementOption
	connectedOpt    metric.MeasurementOption
	disconnectedOpt metric.MeasurementOption
	reconnectedOpt  metric.MeasurementOption
	closedOpt       metric.MeasurementOption
	bufferFullOpt   metric.MeasurementOption
	asyncErrorOpt   metric.MeasurementOption
}

var connEvents *connMetrics

func init() { connEvents = newConnMetrics(otel.Meter("nats")) }

func newConnMetrics(meter metric.Meter) *connMetrics {
	noopMeter := noop.NewMeterProvider().Meter("nats")
	connected, err := meter.Int64UpDownCounter("chat.nats.client.connected",
		metric.WithDescription("Whether this process currently has a live NATS broker connection."))
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
		bufferFullOpt:   event("buffer_full"),
		asyncErrorOpt:   event("async_error"),
	}
}

// Connected records the first successful connect. Later reconnects go through
// Reconnected so the two are distinguishable in the events counter.
func (c *connMetrics) Connected(ctx context.Context) {
	if c == nil {
		return
	}
	c.markUp(ctx, c.connectedOpt)
}

func (c *connMetrics) Reconnected(ctx context.Context) {
	if c == nil {
		return
	}
	c.markUp(ctx, c.reconnectedOpt)
}

// Disconnected takes err only to keep the nats.DisconnectErrHandler signature
// honest at the call site; the text is never a label.
func (c *connMetrics) Disconnected(ctx context.Context, _ error) {
	if c == nil {
		return
	}
	c.markDown(ctx, c.disconnectedOpt)
}

func (c *connMetrics) Closed(ctx context.Context) {
	if c == nil {
		return
	}
	c.markDown(ctx, c.closedOpt)
}

// AsyncError classifies the connection-level async errors that matter to a
// failure campaign. A reconnect-buffer overflow is client-side loss and gets
// its own event; slow consumers keep their existing dedicated counter.
func (c *connMetrics) AsyncError(ctx context.Context, err error) {
	if c == nil {
		return
	}
	opt := c.asyncErrorOpt
	if errors.Is(err, nats.ErrReconnectBufExceeded) {
		opt = c.bufferFullOpt
	}
	c.events.Add(ctx, 1, opt)
}

func (c *connMetrics) markUp(ctx context.Context, opt metric.MeasurementOption) {
	if c == nil {
		return
	}
	c.events.Add(ctx, 1, opt)
	if c.up.CompareAndSwap(false, true) {
		c.connected.Add(ctx, 1, c.gaugeOpt)
	}
}

func (c *connMetrics) markDown(ctx context.Context, opt metric.MeasurementOption) {
	if c == nil {
		return
	}
	c.events.Add(ctx, 1, opt)
	if c.up.CompareAndSwap(true, false) {
		c.connected.Add(ctx, -1, c.gaugeOpt)
	}
}
