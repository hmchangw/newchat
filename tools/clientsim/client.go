package main

import (
	"context"
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
	// touched tracks per-room mutation generations so a resync walk never
	// reverts a live update that landed while its RPC was in flight.
	touched map[string]uint64
	gen     uint64

	roomCh chan *nats.Msg

	resyncMu      sync.Mutex // guards the resync coalescing state below
	resyncActive  bool
	resyncPending bool
	resyncJitter  func() time.Duration // injectable for tests

	// stateMu guards the gauge-backing connection state, separately from
	// s.mu so the nats.go async callbacks can flip it without waiting on a
	// bootstrap walk. LOCK ORDER: s.mu may be held while taking stateMu
	// (subscriptions.go does exactly that when a failed room subscribe
	// demotes readiness); the reverse must never happen, and nothing under
	// stateMu takes s.mu today.
	stateMu sync.Mutex
	connUp  bool
	ready   bool
}

func newSimClient(account string, cfg *config, mint minter, m *metrics) (*simClient, error) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, fmt.Errorf("create user nkey for %s: %w", account, err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("user nkey public key for %s: %w", account, err)
	}
	s := &simClient{
		account:  account,
		cfg:      cfg,
		mint:     mint,
		m:        m,
		nkeyPair: kp,
		nkeyPub:  pub,
		roomSubs: map[string]openSub{},
		touched:  map[string]uint64{},
		roomCh:   make(chan *nats.Msg, cfg.SubPendingMsgs),
	}
	s.dial = s.realDial
	s.resyncJitter = func() time.Duration {
		return time.Duration(secureIntN(int(2 * time.Second)))
	}
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
	pumpCtx, stopPump := context.WithCancel(ctx)
	defer stopPump()
	go s.pump(pumpCtx)

	conn := s.connSnapshot()
	if conn == nil {
		return fmt.Errorf("client %s closed during startup", s.account)
	}
	if err := s.subscribeLanes(conn); err != nil {
		s.close()
		return err
	}
	if err := s.bootstrapWalk(ctx); err != nil {
		s.close()
		return err
	}

	if s.cfg.JWTMode == jwtModeProactive {
		s.proactiveRefreshLoop(ctx)
	} else {
		<-ctx.Done()
	}
	return nil
}
