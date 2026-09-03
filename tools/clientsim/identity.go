package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
)

// minter is the auth-exchange seam; authClient is the production
// implementation, tests inject counters.
type minter interface {
	Mint(ctx context.Context, account, natsPubKey string) (string, error)
}

// jwtCache holds the client's current JWT and the expiry parsed from its
// claims at set time. The connect callback reads it; minting writes it —
// exactly one mint per refresh cycle (spec §5.2 steps 3/6).
type jwtCache struct {
	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func (c *jwtCache) set(token string) error {
	claims, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return fmt.Errorf("decode minted JWT claims: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	if claims.Expires > 0 {
		c.expiresAt = time.Unix(claims.Expires, 0)
	} else {
		c.expiresAt = time.Time{} // no expiry claim: never treated as expired
	}
	return nil
}

func (c *jwtCache) get() (string, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token, c.expiresAt
}

// jwtRefreshFraction / jwtRefreshJitter shape the OPTIONAL proactive mode.
//
// The production client does NOT refresh proactively: it holds its JWT until
// the server drops the connection at expiry and re-mints on the reconnect,
// which is what jwtModeExpiry (the default) models. This schedule is copied
// from chat-frontend's useJwtRefresh.js and exists to put re-mint load on
// auth-service on purpose — a stress knob, not client parity, so its numbers
// do not need to match anything.
//
// The jitter is MULTIPLICATIVE on the fraction — 0.80 * (1 ± 0.05), i.e.
// 76%-84% of remaining life — not ±5 percentage points, which would be
// 75%-85% and is a different convention.
const (
	jwtRefreshFraction = 0.80
	jwtRefreshJitter   = 0.05
)

// defaultMinRefreshDelay floors the proactive schedule. A failed mint leaves the
// cached expiry untouched, so the next delay is computed from a token with
// almost no life left — and once it is past expiry the proportional formula
// yields zero. Without a floor every client in a 30k fleet would retry
// against auth-service roughly once a second, turning one auth blip into a
// self-inflicted outage. The cost is that a token with under ~37s of life is
// refreshed after it expires rather than before; auth-service mints 2h
// tokens, so that trade only ever bites a test fixture.
const defaultMinRefreshDelay = 30 * time.Second

// refreshDelay is the proactive schedule: a share of the JWT's remaining life,
// jittered so a fleet does not re-mint in lockstep.
func refreshDelay(expiresAt, now time.Time, randFloat func() float64) time.Duration {
	remaining := expiresAt.Sub(now)
	if remaining <= 0 {
		return 0
	}
	frac := jwtRefreshFraction * (1 + jwtRefreshJitter*(2*randFloat()-1))
	return time.Duration(float64(remaining) * frac)
}

// primeJWT performs the initial mint into the cache.
func (s *simClient) primeJWT(ctx context.Context) error {
	token, err := s.mint.Mint(ctx, s.account, s.nkeyPub)
	if err != nil {
		return fmt.Errorf("prime JWT for %s: %w", s.account, err)
	}
	return s.cache.set(token)
}

// refreshJWT re-mints into the cache (the proactive timer's mint).
func (s *simClient) refreshJWT(ctx context.Context) error {
	token, err := s.mint.Mint(ctx, s.account, s.nkeyPub)
	if err != nil {
		return fmt.Errorf("refresh JWT for %s: %w", s.account, err)
	}
	if err := s.cache.set(token); err != nil {
		return err
	}
	s.m.JWTRefreshes.WithLabelValues(s.cfg.JWTMode).Inc()
	return nil
}

// userCB is the nats.UserJWT token callback. Proactive mode only reads the
// cache (the timer owns minting). Expiry mode mints iff the cached JWT has
// expired — that IS the reconnect path, and it mints exactly once.
// context.Background() is deliberate: the nats.UserJWT callback carries no
// ctx; the mint is bounded by the auth client's own 10s timeout.
func (s *simClient) userCB() (string, error) {
	token, expiresAt := s.cache.get()
	if token == "" {
		return "", errors.New("jwt cache empty: primeJWT must run before connect")
	}
	// forceRefresh is the broker's verdict and outranks the local clock in
	// either mode: waiting for a timer or a local expiry that will not arrive
	// hands the same rejected credential back on every reconnect.
	forced := s.forceRefresh.CompareAndSwap(true, false)
	if forced || (s.cfg.JWTMode == jwtModeExpiry && !expiresAt.IsZero() && time.Now().After(expiresAt)) {
		if err := s.refreshJWT(context.Background()); err != nil {
			return "", err
		}
		token, _ = s.cache.get()
	}
	return token, nil
}

// sigCB signs the server nonce with the client's user nkey.
func (s *simClient) sigCB(nonce []byte) ([]byte, error) {
	return s.nkeyPair.Sign(nonce)
}

// nextRefreshDelay is the frontend's proactive schedule for the cached JWT.
// A token with no expiry claim gets an idle re-check rather than a busy loop.
func (s *simClient) nextRefreshDelay() time.Duration {
	_, expiresAt := s.cache.get()
	if expiresAt.IsZero() {
		return time.Hour
	}
	if delay := refreshDelay(expiresAt, time.Now(), s.refreshRand); delay > s.minRefreshDelay {
		return delay
	}
	return s.minRefreshDelay
}

// refreshAndReconnect re-mints and then forces a reconnect so the fresh JWT
// is presented (frontend parity). A failed mint is counted and left for the
// next tick — the cached token is still valid until its own expiry.
func (s *simClient) refreshAndReconnect(ctx context.Context) {
	if err := s.refreshJWT(ctx); err != nil {
		s.m.Errors.WithLabelValues("auth").Inc()
		slog.Warn("proactive JWT refresh", "account", s.account, "error", err)
		return
	}
	if conn := s.connSnapshot(); conn != nil {
		s.markIntentionalReconnect()
		if err := conn.ForceReconnect(); err != nil {
			// No disconnect will follow, so the latch would mislabel the next
			// one that does.
			s.intentionalReconnect.Store(false)
			slog.Warn("force reconnect after refresh", "account", s.account, "error", err)
		}
	}
}
