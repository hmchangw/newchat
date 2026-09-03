package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// simClient is one simulated user: an NKey identity, a cached JWT, one WSS
// connection, and the live subscription set. roomSubs is the single source
// of truth for what is subscribed — the dedupe view handed to
// applySubscriptionUpdate is derived from it under s.mu, so the two can
// never diverge and a failed open is retried by the next add/resync.
type simClient struct {
	account string
	runID   string // from the pool artifact; only ever used in the conn name
	cfg     *config
	mint    minter
	m       *metrics
	dial    dialFunc // defaults to s.realDial; tests inject fakes

	nkeyPair nkeys.KeyPair
	nkeyPub  string
	cache    jwtCache

	mu       sync.Mutex
	conn     simConn
	closed   bool
	roomSubs map[string]openSub
	// missingRooms tracks desired subscriptions whose Subscribe call failed.
	// roomSubs alone cannot represent that intent, so without this set one
	// successful repair could incorrectly promote a still-incomplete client.
	missingRooms map[string]struct{}
	// planVerified records that a bootstrap walk has completed since the
	// current connection came up. missingRooms answers "is anything known to
	// be broken"; only a completed walk answers "is the plan complete at all",
	// and a live update can satisfy the first while the second is still false.
	planVerified bool
	// asyncFault records that the server rejected something asynchronously —
	// a SUB permission violation arrives after Subscribe already returned nil,
	// so it leaves no trace in missingRooms. Without it a one-shot demote is
	// undone by the very next live update, because updateReadyLocked would
	// still see a verified plan and nothing missing.
	//
	// Scoped to the connection, because that is what its permissions came
	// from: invalidatePlan clears it, and a still-denied room raises it again
	// as soon as the new connection re-subscribes. Nothing weaker works — the
	// client cannot prove a SUB is authorized (a successful walk says nothing
	// about it), so readiness fails closed until the connection is replaced.
	asyncFault bool
	// planEpoch advances on every disconnect. A walk reads it before its RPC
	// and again before applying: the RPC happens with no lock held, so without
	// this a walk whose reply arrived over a now-dead connection could set
	// planVerified back to true after invalidatePlan cleared it.
	planEpoch uint64
	// touched tracks per-room mutation generations so a resync walk never
	// reverts a live update that landed while its RPC was in flight.
	touched map[string]uint64
	gen     uint64

	roomCh chan *nats.Msg

	resyncMu      sync.Mutex // guards the resync coalescing state below
	resyncActive  bool
	resyncPending bool
	resyncJitter  func() time.Duration // injectable for tests
	resyncRetry   func(int) time.Duration

	// stateMu guards the gauge-backing connection state, separately from
	// s.mu so the nats.go async callbacks can flip it without waiting on a
	// bootstrap walk. LOCK ORDER: s.mu may be held while taking stateMu
	// (subscriptions.go does exactly that when a failed room subscribe
	// demotes readiness); the reverse must never happen, and nothing under
	// stateMu takes s.mu today.
	// reconnectMu guards the reconnect-attempt counter and its stability
	// timer. Separate again from s.mu: nats.go asks for the next delay from
	// its own reconnect goroutine, which must not queue behind a walk.
	reconnectMu       sync.Mutex
	reconnectAttempts int
	stabilityTimer    *time.Timer
	// stabilityGen invalidates a timer whose episode has already ended, so a
	// late fire cannot reset a counter that has moved on.
	stabilityGen    uint64
	stabilityWindow time.Duration
	healthInterval  time.Duration
	// refreshRand is the jitter source for the proactive JWT schedule, and
	// minRefreshDelay its floor — both injectable so a test can pin the
	// deadline instead of racing it.
	refreshRand     func() float64
	minRefreshDelay time.Duration

	stateMu sync.Mutex
	connUp  bool
	ready   bool
}

// stabilityWindow / healthInterval defaults, matching the real client: a
// reconnect counts as recovered only after five uninterrupted minutes, and
// a three-minute tick is what notices a connection nats.go gave up on.
const (
	defaultStabilityWindow = 5 * time.Minute
	defaultHealthInterval  = 3 * time.Minute
)

// nextReconnectAttempt advances and returns the attempt number nats.go's
// delay callback should price. It is never reset by a successful reconnect
// alone — only armStability's window does that.
func (s *simClient) nextReconnectAttempt() int {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	s.reconnectAttempts++
	return s.reconnectAttempts
}

func (s *simClient) currentReconnectAttempt() int {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	return s.reconnectAttempts
}

// armStability starts the window after a successful reconnect. Surviving it
// resets the attempt counter; dropping first does not.
func (s *simClient) armStability() {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	s.stabilityGen++
	gen := s.stabilityGen
	if s.stabilityTimer != nil {
		s.stabilityTimer.Stop()
	}
	s.stabilityTimer = time.AfterFunc(s.stabilityWindow, func() {
		s.reconnectMu.Lock()
		defer s.reconnectMu.Unlock()
		if s.stabilityGen != gen {
			return // this episode ended; a newer one owns the counter
		}
		s.reconnectAttempts = 0
	})
}

// cancelStability ends the current episode. Bumping the generation is what
// makes an already-firing timer a no-op, which Stop alone cannot guarantee.
func (s *simClient) cancelStability() {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	s.stabilityGen++
	if s.stabilityTimer != nil {
		s.stabilityTimer.Stop()
		s.stabilityTimer = nil
	}
}

func newSimClient(account, runID string, cfg *config, mint minter, m *metrics) (*simClient, error) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, fmt.Errorf("create user nkey for %s: %w", account, err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("user nkey public key for %s: %w", account, err)
	}
	s := &simClient{
		account:      account,
		runID:        runID,
		cfg:          cfg,
		mint:         mint,
		m:            m,
		nkeyPair:     kp,
		nkeyPub:      pub,
		roomSubs:     map[string]openSub{},
		missingRooms: map[string]struct{}{},
		touched:      map[string]uint64{},
		roomCh:       make(chan *nats.Msg, cfg.SubPendingMsgs),

		stabilityWindow: defaultStabilityWindow,
		healthInterval:  defaultHealthInterval,
		refreshRand:     secureFloat64,
		minRefreshDelay: defaultMinRefreshDelay,
	}
	s.dial = s.realDial
	s.resyncJitter = func() time.Duration {
		return time.Duration(secureIntN(int(2 * time.Second)))
	}
	s.resyncRetry = defaultResyncRetryDelay
	return s, nil
}

// connSnapshot returns the live connection, or nil once closed/not yet
// connected. Every cross-goroutine use of the conn goes through this.
func (s *simClient) connSnapshot() simConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// run drives the client until ctx ends: prime, connect, subscribe, walk,
// then hold — with the proactive refresh loop when configured. Any failure
// after connect tears the connection down before returning, so a client
// that exits early never lingers as a zombie in the active gauge.
func (s *simClient) run(ctx context.Context) error {
	if err := s.primeJWT(ctx); err != nil {
		s.m.Errors.WithLabelValues("auth").Inc()
		return err
	}
	if s.cfg.JWTMode == jwtModeExpiry {
		if _, expiresAt := s.cache.get(); expiresAt.IsZero() {
			slog.Warn("expiry mode with a no-exp JWT: the expiry lifecycle will never trigger", "account", s.account)
		}
	}
	if err := s.connect(ctx); err != nil {
		return err
	}
	// Every exit from here owns its teardown, rather than trusting the swarm
	// to call close(). Idempotent, so the swarm's own close() is still safe.
	defer s.close()
	pumpCtx, stopPump := context.WithCancel(ctx)
	defer stopPump()
	go s.pump(pumpCtx)

	conn := s.connSnapshot()
	if conn == nil {
		return fmt.Errorf("client %s closed during startup", s.account)
	}
	if err := s.subscribeLanes(conn); err != nil {
		return err
	}
	// A walk whose connection died mid-RPC is a race the resync already owns;
	// only a real failure ends the client.
	if err := s.bootstrapWalk(ctx); err != nil && !errors.Is(err, errPlanEpochChanged) {
		s.close()
		return err
	}

	return s.hold(ctx)
}

// hold is the steady state: the JWT refresh schedule and the health check on
// one goroutine, so neither costs a per-client goroutine of its own.
//
// The health check is what keeps a client from becoming a zombie. nats.go
// closes a connection for good after repeated auth failures even with
// MaxReconnects(-1); before this, run() would sit here forever with a dead
// connection, never reporting an exit, so the swarm never restarted it and
// the fleet silently shrank.
func (s *simClient) hold(ctx context.Context) error {
	health := time.NewTicker(s.healthInterval)
	defer health.Stop()

	// The refresh timer is armed ONCE and re-armed only after it fires.
	// Rebuilding it per loop iteration would let the health tick postpone the
	// deadline indefinitely: nextRefreshDelay is a share of *remaining* life,
	// so every tick recomputes a later fire time and the schedule converges
	// on expiry instead of 80% of the token's life.
	var timer *time.Timer
	var refresh <-chan time.Time
	if s.cfg.JWTMode == jwtModeProactive {
		timer = time.NewTimer(s.nextRefreshDelay())
		defer timer.Stop()
		refresh = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-health.C:
			if conn := s.connSnapshot(); conn != nil && conn.IsClosed() {
				s.m.Errors.WithLabelValues("conn_closed").Inc()
				// Returning hands the account back to the swarm, which
				// restarts it at the ramp rate — a far longer wait than the
				// 60s backoff ceiling, which is the point.
				return fmt.Errorf("connection for %s was closed permanently", s.account)
			}
		case <-refresh:
			s.refreshAndReconnect(ctx)
			// Drained by the receive above, so Reset is safe here.
			timer.Reset(s.nextRefreshDelay())
		}
	}
}
