package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func dialNATSWithMetrics(
	url string,
	credsFile string,
	metrics *Metrics,
) (*o11ynats.Conn, error) {
	health := newLoadgenNATSHealth(metrics, nil)
	connection, err := natsutil.Connect(
		context.Background(),
		url,
		credsFile,
		noop.NewTracerProvider(),
		propagation.TraceContext{},
		false,
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			health.disconnected(err)
		}),
		nats.ReconnectHandler(func(connection *nats.Conn) {
			health.reconnected(connection.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			health.closed()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			health.asyncError(err)
		}),
	)
	if err != nil {
		return nil, err
	}
	health.connected()
	return connection, nil
}

type loadgenNATSHealth struct {
	mu sync.Mutex

	metrics        *Metrics
	pool           string
	now            func() time.Time
	disconnectedAt time.Time
}

func newLoadgenNATSHealth(
	metrics *Metrics,
	now func() time.Time,
) *loadgenNATSHealth {
	if now == nil {
		now = time.Now
	}
	return &loadgenNATSHealth{metrics: metrics, pool: "soak", now: now}
}

func (h *loadgenNATSHealth) connected() {
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(1)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "connected").Inc()
	}
}

func (h *loadgenNATSHealth) disconnected(err error) {
	h.mu.Lock()
	if h.disconnectedAt.IsZero() {
		h.disconnectedAt = h.now().UTC()
	}
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(0)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "disconnected").Inc()
	}
	slog.Warn("nats disconnected", "error", err)
}

func (h *loadgenNATSHealth) reconnected(url string) {
	now := h.now().UTC()
	h.mu.Lock()
	disconnectedAt := h.disconnectedAt
	h.disconnectedAt = time.Time{}
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(1)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "reconnected").Inc()
		if !disconnectedAt.IsZero() {
			h.metrics.NATSOutageDuration.WithLabelValues(h.pool).Observe(
				now.Sub(disconnectedAt).Seconds(),
			)
		}
	}
	slog.Info("nats reconnected", "url", url)
}

func (h *loadgenNATSHealth) closed() {
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(0)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "closed").Inc()
	}
	slog.Warn("nats connection closed")
}

func (h *loadgenNATSHealth) asyncError(err error) {
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "async_error").Inc()
	}
	slog.Error("nats async error", "error", err)
}
