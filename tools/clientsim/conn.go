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
	// IsClosed reports a permanently closed connection. With
	// MaxReconnects(-1) nats.go never gives up on its own, so this is only
	// true once it has closed the connection for good (repeated auth
	// failures) — the health check's whole purpose.
	IsClosed() bool
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
func (r *realConn) IsClosed() bool        { return r.nc.IsClosed() }
func (r *realConn) Close()                { r.nc.Close() }

// connNamePrefix is the whole ops-facing contract: a connection whose name
// starts with this is a simulated one. Deliberately not configurable —
// a knob here would let a fleet disguise itself as real traffic.
const connNamePrefix = "clientsim-"

// connName mirrors the desktop client's desktop-{account}[-{hostname}]
// shape, so tooling that splits on the first dash keeps working while the
// leading token still tells the two apart. The run and shard take the slot
// hostname occupies for a real client: together they say which load run and
// which replica a connection in /connz belongs to.
func connName(account, runID string, shardIndex int) string {
	return fmt.Sprintf("%s%s-%s-s%d", connNamePrefix, account, runID, shardIndex)
}

// reconnectBackoffBase is the nats.ws client's curve, band for band:
// attempts 1-5 wait 2s, 6-10 wait 5s, 11+ double from 10s to a 60s cap,
// and everything past that long-polls at 60s. nats.go's own ReconnectWait
// is a single flat delay, so a fleet using it would hammer a recovering
// broker far harder than the real one does.
func reconnectBackoffBase(attempt int) time.Duration {
	const cap60 = 60 * time.Second
	switch {
	case attempt <= 5:
		return 2 * time.Second
	case attempt <= 10:
		return 5 * time.Second
	case attempt >= 14:
		return cap60
	default:
		// 11 -> 10s, 12 -> 20s, 13 -> 40s.
		return 10 * time.Second << (attempt - 11)
	}
}

// reconnectDelay adds the client's jitter: up to +50%, never negative, so
// the band floor is preserved while a fleet does not retry in lockstep.
func reconnectDelay(attempt int) time.Duration {
	base := reconnectBackoffBase(attempt)
	return base + time.Duration(secureIntN(int(base/2)))
}

// realDial opens the production WebSocket connection.
func (s *simClient) realDial(ctx context.Context) (simConn, error) {
	nc, err := nats.Connect(s.cfg.NATSWSURL,
		nats.UserJWT(s.userCB, s.sigCB),
		nats.Name(connName(s.account, s.runID, s.cfg.ShardIndex)),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(s.cfg.ReconnectBufBytes),
		nats.PingInterval(s.cfg.PingInterval),
		// Supersedes ReconnectWait/ReconnectJitter: the attempt counter is
		// ours, not nats.go's, because nats.go resets its own on the first
		// successful reconnect and the real client only resets after five
		// minutes of stability.
		nats.CustomReconnectDelay(s.nextReconnectDelay),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			s.m.Reconnects.Inc()
			s.m.ReconnectAttempt.Observe(float64(s.currentReconnectAttempt()))
			s.markConnUp()
			s.armStability()
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
			// A reconnect that did not survive the stability window leaves the
			// attempt counter where it is, so a flapping link climbs the curve.
			s.cancelStability()
			s.invalidatePlan()
			// Without this the active gauge would sit at full fleet for the
			// whole outage — the exact reading a failure test must not get.
			s.markConnDown()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			s.handleAsyncError(sub, err)
		}),
	)
	if err != nil {
		return nil, err
	}
	return &realConn{nc: nc, pendingMsgs: s.cfg.SubPendingMsgs, pendingBytes: s.cfg.SubPendingBytes}, nil
}

// nextReconnectDelay is nats.go's CustomReconnectDelay callback. Its own
// attempt argument is ignored: nats.go resets that counter on the first
// successful reconnect, and the real client only resets after the stability
// window, so the fleet must carry its own.
func (s *simClient) nextReconnectDelay(int) time.Duration {
	return reconnectDelay(s.nextReconnectAttempt())
}

// handleAsyncError takes the subscription nats.go blames, because a fault
// that fails readiness closed until the next reconnect is unusable to an
// operator without the subject that caused it. It is nil for connection-level
// faults.
func (s *simClient) handleAsyncError(sub *nats.Subscription, err error) {
	// Episode semantics only: one increment per Active->SlowConsumer
	// transition; never add Subscription.Dropped() here (see the metric's
	// doc comment and pkg/natsutil/slowconsumer.go).
	if errors.Is(err, nats.ErrSlowConsumer) {
		s.m.SlowConsumer.Inc()
		return
	}
	// Subscription permission violations and other asynchronous faults can
	// arrive after Subscribe returned nil. The client can no longer prove it
	// carries its full plan, so fail the readiness state closed — and record
	// it, because a bare demote is undone by the next live update.
	s.m.Errors.WithLabelValues("async").Inc()
	s.mu.Lock()
	s.asyncFault = true
	s.updateReadyLocked() // one place decides readiness; lock order s.mu -> stateMu
	s.mu.Unlock()
	subj := ""
	if sub != nil {
		subj = sub.Subject
	}
	slog.Warn("nats async error", "account", s.account, "subject", subj, "error", err)
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
