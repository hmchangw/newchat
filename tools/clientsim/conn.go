package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// simSub is the slice of *nats.Subscription the client uses, so unit tests
// can fake subscriptions without a broker.
type simSub interface {
	Unsubscribe() error
}

// simConn abstracts the NATS connection. realConn wraps *nats.Conn; tests
// inject fakes through simClient.dial.
type simConn interface {
	// SubscribeCB is an async subscription with its own dispatcher
	// goroutine — used only for the two per-user lanes.
	SubscribeCB(subj string, cb nats.MsgHandler) (simSub, error)
	// SubscribeChan delivers into ch with NO per-subscription goroutine —
	// used for every room subscription, so a user in N rooms costs one
	// pump goroutine instead of N dispatchers (the 10k-conns/process
	// budget lives or dies on this).
	SubscribeChan(subj string, ch chan *nats.Msg) (simSub, error)
	Request(ctx context.Context, subj string, data []byte) (*nats.Msg, error)
	ForceReconnect() error
	Close()
}

// dialFunc opens the transport; tests substitute a fake.
type dialFunc func(ctx context.Context) (simConn, error)

// realConn adapts *nats.Conn to simConn, applying the configured pending
// limits to callback subscriptions.
type realConn struct {
	nc           *nats.Conn
	pendingMsgs  int
	pendingBytes int
}

func (r *realConn) SubscribeCB(subj string, cb nats.MsgHandler) (simSub, error) {
	sub, err := r.nc.Subscribe(subj, cb)
	if err != nil {
		return nil, err
	}
	if err := sub.SetPendingLimits(r.pendingMsgs, r.pendingBytes); err != nil {
		_ = sub.Unsubscribe() // limit setup failed; don't keep a half-configured sub
		return nil, fmt.Errorf("set pending limits on %s: %w", subj, err)
	}
	return sub, nil
}

func (r *realConn) SubscribeChan(subj string, ch chan *nats.Msg) (simSub, error) {
	return r.nc.ChanSubscribe(subj, ch)
}

func (r *realConn) Request(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
	return r.nc.RequestWithContext(ctx, subj, data)
}

func (r *realConn) ForceReconnect() error { return r.nc.ForceReconnect() }
func (r *realConn) Close()                { r.nc.Close() }

// realDial opens the production WebSocket connection.
func (s *simClient) realDial(ctx context.Context) (simConn, error) {
	nc, err := nats.Connect(s.cfg.NATSWSURL,
		nats.UserJWT(s.userCB, s.sigCB),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(s.cfg.ReconnectBufBytes),
		nats.PingInterval(s.cfg.PingInterval),
		// Spread the herd: 10k clients reconnecting after a NATS bounce
		// must not re-dial (and, in expiry mode, re-mint) in lockstep.
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(2*time.Second, 2*time.Second),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			s.m.Reconnects.Inc()
			s.markConnUp()
			go s.resync(ctx)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			// nats.go passes a nil error when the client itself closed the
			// connection; labelling that "eof" makes a deliberate teardown
			// read like a network drop on the dashboard.
			reason := "closed"
			if err != nil {
				reason = disconnectReason(err)
			}
			s.m.Disconnects.WithLabelValues(reason).Inc()
			// Separate calls, not one nested helper: invalidatePlan takes
			// s.mu and markConnDown holds stateMu, and the lock order forbids
			// stateMu -> s.mu.
			s.invalidatePlan()
			// Without this the active gauge would sit at full fleet for the
			// whole outage — the exact reading a failure test must not get.
			s.markConnDown()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			s.handleAsyncError(err)
		}),
	)
	if err != nil {
		return nil, err
	}
	return &realConn{nc: nc, pendingMsgs: s.cfg.SubPendingMsgs, pendingBytes: s.cfg.SubPendingBytes}, nil
}

func (s *simClient) handleAsyncError(err error) {
	// Episode semantics only: one increment per Active->SlowConsumer
	// transition; never add Subscription.Dropped() here (see the metric's
	// doc comment and pkg/natsutil/slowconsumer.go).
	if errors.Is(err, nats.ErrSlowConsumer) {
		s.m.SlowConsumer.Inc()
		return
	}
	// Subscription permission violations and other asynchronous faults can
	// arrive after Subscribe returned nil. The client can no longer prove it
	// carries its full plan, so fail the readiness state closed.
	s.m.Errors.WithLabelValues("async").Inc()
	s.markNotReady()
	slog.Warn("nats async error", "account", s.account, "error", err)
}

// disconnectReason maps common close errors to a bounded label set so the
// disconnects counter's cardinality stays sane.
func disconnectReason(err error) string {
	switch {
	case errors.Is(err, nats.ErrAuthExpired):
		return "auth_expired"
	case errors.Is(err, nats.ErrAuthorization):
		return "authorization"
	case errors.Is(err, nats.ErrConnectionClosed):
		return "closed"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "other"
	}
}

// connect dials and installs the connection — unless the client was closed
// (or ctx cancelled) while the dial was in flight, in which case the fresh
// connection is closed instead of leaked (churn can pick a client
// mid-connect).
func (s *simClient) connect(ctx context.Context) error {
	s.m.ConnsConnecting.Inc()
	defer s.m.ConnsConnecting.Dec()
	start := time.Now()
	conn, err := s.dial(ctx)
	if err != nil {
		s.m.Errors.WithLabelValues("connect").Inc()
		return fmt.Errorf("connect %s to %s: %w", s.account, s.cfg.NATSWSURL, err)
	}
	s.mu.Lock()
	if s.closed || ctx.Err() != nil {
		s.mu.Unlock()
		conn.Close()
		return fmt.Errorf("client %s closed during connect", s.account)
	}
	s.conn = conn
	s.mu.Unlock()
	s.m.ConnectDuration.Observe(time.Since(start).Seconds())
	s.markConnUp()
	return nil
}

// close tears the connection down. Idempotent; also flips the closed flag
// so an in-flight connect cannot install a connection afterwards.
func (s *simClient) close() {
	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.closed = true
	s.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	s.markConnDown()
}
