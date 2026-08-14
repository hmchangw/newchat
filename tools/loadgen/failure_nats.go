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
	callbackSeen   bool
	connectedState bool
	closedState    bool
	observer       *failureObserverHealth
	poolState      *loadgenNATSPoolState
}

type loadgenNATSPoolState struct {
	mu sync.Mutex

	metrics        *Metrics
	pool           string
	now            func() time.Time
	connections    map[*loadgenNATSHealth]bool
	observer       *failureObserverHealth
	disconnectedAt time.Time
	outageStop     chan struct{}
	stopped        bool
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
	health := &loadgenNATSHealth{metrics: metrics, pool: pool, now: now}
	if metrics == nil {
		health.poolState = &loadgenNATSPoolState{
			pool: pool, now: now, connections: make(map[*loadgenNATSHealth]bool),
		}
		return health
	}
	metrics.natsHealthMu.Lock()
	state := metrics.natsPoolHealth[pool]
	if state == nil {
		state = &loadgenNATSPoolState{
			metrics: metrics, pool: pool, now: now,
			connections: make(map[*loadgenNATSHealth]bool),
		}
		metrics.natsPoolHealth[pool] = state
	}
	metrics.natsHealthMu.Unlock()
	health.poolState = state
	return health
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
	if h.callbackSeen {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	h.connectedState = true
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "connected").Inc()
	}
	observer := h.observer
	h.mu.Unlock()
	h.poolState.update(h, true, observer, "connected")
}

func (h *loadgenNATSHealth) disconnected(err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closedState || (h.callbackSeen && !h.connectedState) {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	h.connectedState = false
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "disconnected").Inc()
	}
	observer := h.observer
	h.mu.Unlock()
	h.poolState.update(h, false, observer, "disconnected")
	slog.Warn("nats disconnected", "error", err)
}

func (h *loadgenNATSHealth) reconnected(url string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closedState || (h.callbackSeen && h.connectedState) {
		h.mu.Unlock()
		return
	}
	h.callbackSeen = true
	h.connectedState = true
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "reconnected").Inc()
	}
	observer := h.observer
	h.mu.Unlock()
	h.poolState.update(h, true, observer, "reconnected")
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
	h.connectedState = false
	if h.metrics != nil {
		h.metrics.NATSConnectionEvents.WithLabelValues(h.pool, "closed").Inc()
	}
	observer := h.observer
	h.mu.Unlock()
	h.poolState.remove(h, observer, "closed")
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
	if h == nil || h.poolState == nil {
		return
	}
	h.poolState.updateCurrentOutage()
}

func (s *loadgenNATSPoolState) update(
	connection *loadgenNATSHealth,
	connected bool,
	observer *failureObserverHealth,
	reason string,
) {
	if s == nil {
		return
	}
	now := s.now().UTC()
	s.mu.Lock()
	wasUp := s.allConnectedLocked()
	s.connections[connection] = connected
	if observer != nil {
		s.observer = observer
	}
	isUp := s.allConnectedLocked()
	if s.metrics != nil {
		connectedValue := float64(0)
		if isUp {
			connectedValue = 1
		}
		s.metrics.NATSConnected.WithLabelValues(s.pool).Set(connectedValue)
	}
	if wasUp == isUp {
		s.mu.Unlock()
		return
	}
	if isUp {
		disconnectedAt := s.disconnectedAt
		s.disconnectedAt = time.Time{}
		s.stopOutageTickerLocked()
		if s.metrics != nil {
			if !disconnectedAt.IsZero() {
				s.metrics.NATSOutageDuration.WithLabelValues(s.pool).Observe(now.Sub(disconnectedAt).Seconds())
			}
			s.metrics.NATSCurrentOutage.WithLabelValues(s.pool).Set(0)
		}
		if s.observer != nil {
			s.observer.Set(true, now, reason)
		}
		s.mu.Unlock()
		return
	}
	if s.disconnectedAt.IsZero() {
		s.disconnectedAt = now
	}
	s.startOutageTickerLocked()
	if s.observer != nil {
		s.observer.Set(false, now, reason)
	}
	s.mu.Unlock()
}

func (s *loadgenNATSPoolState) remove(
	connection *loadgenNATSHealth,
	observer *failureObserverHealth,
	reason string,
) {
	if s == nil {
		return
	}
	now := s.now().UTC()
	s.mu.Lock()
	wasUp := s.allConnectedLocked()
	delete(s.connections, connection)
	if observer != nil {
		s.observer = observer
	}
	isUp := s.allConnectedLocked()
	if s.metrics != nil {
		connectedValue := float64(0)
		if isUp {
			connectedValue = 1
		}
		s.metrics.NATSConnected.WithLabelValues(s.pool).Set(connectedValue)
	}
	if wasUp == isUp {
		s.mu.Unlock()
		return
	}
	if isUp {
		disconnectedAt := s.disconnectedAt
		s.disconnectedAt = time.Time{}
		s.stopOutageTickerLocked()
		if s.metrics != nil {
			if !disconnectedAt.IsZero() {
				s.metrics.NATSOutageDuration.WithLabelValues(s.pool).Observe(now.Sub(disconnectedAt).Seconds())
			}
			s.metrics.NATSCurrentOutage.WithLabelValues(s.pool).Set(0)
		}
		if s.observer != nil {
			s.observer.Set(true, now, reason)
		}
		s.mu.Unlock()
		return
	}
	if s.disconnectedAt.IsZero() {
		s.disconnectedAt = now
	}
	s.startOutageTickerLocked()
	if s.observer != nil {
		s.observer.Set(false, now, reason)
	}
	s.mu.Unlock()
}

func (s *loadgenNATSPoolState) allConnectedLocked() bool {
	if len(s.connections) == 0 {
		return false
	}
	for _, connected := range s.connections {
		if !connected {
			return false
		}
	}
	return true
}

func (s *loadgenNATSPoolState) updateCurrentOutage() {
	if s == nil || s.metrics == nil {
		return
	}
	s.mu.Lock()
	disconnectedAt := s.disconnectedAt
	s.mu.Unlock()
	seconds := float64(0)
	if !disconnectedAt.IsZero() {
		seconds = max(0, s.now().UTC().Sub(disconnectedAt).Seconds())
	}
	s.metrics.NATSCurrentOutage.WithLabelValues(s.pool).Set(seconds)
}

func (s *loadgenNATSPoolState) startOutageTickerLocked() {
	if s.metrics == nil || s.outageStop != nil || s.stopped {
		return
	}
	stop := make(chan struct{})
	s.outageStop = stop
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.updateCurrentOutage()
			case <-stop:
				return
			}
		}
	}()
}

func (s *loadgenNATSPoolState) stopOutageTickerLocked() {
	if s.outageStop == nil {
		return
	}
	close(s.outageStop)
	s.outageStop = nil
}

func (s *loadgenNATSPoolState) stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.stopped = true
	s.stopOutageTickerLocked()
	s.mu.Unlock()
}

// StopNATSHealth terminates background outage sampling owned by this registry.
func (m *Metrics) StopNATSHealth() {
	if m == nil {
		return
	}
	m.natsHealthMu.Lock()
	states := make([]*loadgenNATSPoolState, 0, len(m.natsPoolHealth))
	for _, state := range m.natsPoolHealth {
		states = append(states, state)
	}
	m.natsHealthMu.Unlock()
	for _, state := range states {
		state.stop()
	}
}
