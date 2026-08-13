package main

import (
	"context"
	"errors"
	"fmt"
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
	return dialNATSPoolWithMetrics(url, credsFile, "soak", metrics, nil)
}

func dialNATSPoolWithMetrics(
	url string,
	credsFile string,
	pool string,
	metrics *Metrics,
	observer *failureObserverHealth,
) (*o11ynats.Conn, error) {
	health := newLoadgenNATSHealth(pool, metrics, nil)
	if health == nil {
		return nil, fmt.Errorf("unknown loadgen NATS pool %q", pool)
	}
	health.observer = observer
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
			health.reconnected(connection.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			health.closed()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				health.bufferFull(err)
				return
			}
			health.asyncError(err)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS for loadgen: %w", err)
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
	callbackSeen   bool
	closedState    bool
	observer       *failureObserverHealth
	outageStop     chan struct{}
}

func newLoadgenNATSHealth(
	pool string,
	metrics *Metrics,
	now func() time.Time,
) *loadgenNATSHealth {
	if _, ok := loadgenNATSPoolRegistry[pool]; !ok {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &loadgenNATSHealth{metrics: metrics, pool: pool, now: now}
}

var loadgenNATSPoolRegistry = map[string]struct{}{
	"soak": {}, "daily": {}, "members": {}, "presence_publish": {},
	"presence_observer": {}, "recipient_observer": {}, "general": {},
}

func (h *loadgenNATSHealth) connected() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.callbackSeen {
		return
	}
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(1)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "connected").Inc()
		h.metrics.NATSCurrentOutage.WithLabelValues(h.pool).Set(0)
	}
}

func (h *loadgenNATSHealth) disconnected(err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closedState {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	if h.disconnectedAt.IsZero() {
		h.disconnectedAt = h.now().UTC()
	}
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(0)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "disconnected").Inc()
	}
	h.startOutageTickerLocked()
	if h.observer != nil {
		h.observer.Set(false, h.now().UTC(), "disconnected")
	}
	h.mu.Unlock()
	slog.Warn("nats disconnected", "error", err)
}

func (h *loadgenNATSHealth) reconnected(url string) {
	if h == nil {
		return
	}
	now := h.now().UTC()
	h.mu.Lock()
	if h.closedState {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	disconnectedAt := h.disconnectedAt
	h.disconnectedAt = time.Time{}
	h.stopOutageTickerLocked()
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(1)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "reconnected").Inc()
		if !disconnectedAt.IsZero() {
			h.metrics.NATSOutageDuration.WithLabelValues(h.pool).Observe(
				now.Sub(disconnectedAt).Seconds(),
			)
		}
		h.metrics.NATSCurrentOutage.WithLabelValues(h.pool).Set(0)
	}
	if h.observer != nil {
		h.observer.Set(true, now, "reconnected")
	}
	h.mu.Unlock()
	slog.Info("nats reconnected", "url", url)
}

func (h *loadgenNATSHealth) closed() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closedState {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	h.closedState = true
	h.stopOutageTickerLocked()
	if h.metrics != nil {
		h.metrics.NATSConnected.WithLabelValues(h.pool).Set(0)
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "closed").Inc()
	}
	if h.observer != nil {
		h.observer.Set(false, h.now().UTC(), "closed")
	}
	h.mu.Unlock()
	slog.Warn("nats connection closed")
}

func (h *loadgenNATSHealth) asyncError(err error) {
	if h == nil {
		return
	}
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "async_error").Inc()
	}
	slog.Error("nats async error", "error", err)
}

func (h *loadgenNATSHealth) bufferFull(err error) {
	if h == nil {
		return
	}
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "buffer_full").Inc()
	}
	if h.observer != nil {
		h.observer.Set(false, h.now().UTC(), "buffer_full")
	}
	slog.Error("nats observer buffer full", "error", err)
}

func (h *loadgenNATSHealth) updateCurrentOutage() {
	if h == nil || h.metrics == nil {
		return
	}
	h.mu.Lock()
	disconnectedAt := h.disconnectedAt
	h.mu.Unlock()
	seconds := float64(0)
	if !disconnectedAt.IsZero() {
		seconds = max(0, h.now().UTC().Sub(disconnectedAt).Seconds())
	}
	h.metrics.NATSCurrentOutage.WithLabelValues(h.pool).Set(seconds)
}

func (h *loadgenNATSHealth) startOutageTickerLocked() {
	if h.metrics == nil || h.outageStop != nil {
		return
	}
	stop := make(chan struct{})
	h.outageStop = stop
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.updateCurrentOutage()
			case <-stop:
				return
			}
		}
	}()
}

func (h *loadgenNATSHealth) stopOutageTickerLocked() {
	if h.outageStop == nil {
		return
	}
	close(h.outageStop)
	h.outageStop = nil
}
