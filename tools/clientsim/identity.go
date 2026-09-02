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

// jwtRefreshFraction / jwtRefreshJitter mirror useJwtRefresh.js's
// REFRESH_FRACTION and REFRESH_JITTER. The jitter is MULTIPLICATIVE on the
// fraction — 0.80 * (1 ± 0.05), i.e. 76%-84% of remaining life — not ±5
// percentage points, which would be 75%-85% and is a different convention.
//
// UNVERIFIED against the production client: chat-frontend is a test frontend,
// not the reference. If the real client's schedule differs, the fleet's
// re-mint cadence against auth-service is off by that much.
const (
	jwtRefreshFraction = 0.80
	jwtRefreshJitter   = 0.05
)

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
	if s.cfg.JWTMode == jwtModeExpiry && !expiresAt.IsZero() && time.Now().After(expiresAt) {
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
	if delay := refreshDelay(expiresAt, time.Now(), secureFloat64); delay > 0 {
		return delay
	}
	return time.Second
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
		if err := conn.ForceReconnect(); err != nil {
			slog.Warn("force reconnect after refresh", "account", s.account, "error", err)
		}
	}
}
