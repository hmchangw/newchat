package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// accessTokenRequest is the J1→J2 exchange request body: the J1 token is sent as
// the "key" field, not an Authorization header.
type accessTokenRequest struct {
	Key string `json:"key"`
}

// accessTokenResponse is the accessToken API reply. token is the J2 token used as
// the Authorization header on the translate API.
type accessTokenResponse struct {
	Token        string `json:"token"`
	ExpiresAt    string `json:"expiresAt"`
	Username     string `json:"username"`
	JwtRequestID string `json:"jwtRequestId"`
}

// tokenEntry is an immutable snapshot of a cached J2 token and its validity
// deadline (parsed expiresAt minus skew). Published via an atomic pointer so the
// read path needs no lock.
type tokenEntry struct {
	token     string
	expiresAt time.Time
}

// tokenProvider exchanges a J1 token for a J2 token via the accessToken API,
// caching it until shortly before expiresAt (minus skew). The J1 token comes
// from readJ1 and is read fresh on every exchange (see j1Source). Safe for
// concurrent use: the cached token is read lock-free via an atomic pointer, and
// mu serializes only the (rare) fetches.
type tokenProvider struct {
	client *resty.Client
	readJ1 j1Source
	skew   time.Duration
	now    func() time.Time

	cached atomic.Pointer[tokenEntry] // valid cached J2 token; read without a lock
	mu     sync.Mutex                 // serializes fetchLocked (one exchange at a time)
}

func newTokenProvider(accessTokenURL string, j1 j1Source, timeout, skew time.Duration) *tokenProvider {
	// Cap the token-exchange timeout well below a translate call so a hung accessToken
	// endpoint can't hold the lock (and every waiting translate) for the full timeout.
	tokenTimeout := timeout
	if tokenTimeout > 5*time.Second {
		tokenTimeout = 5 * time.Second
	}
	return &tokenProvider{
		client: restyutil.New(accessTokenURL, restyutil.WithTimeout(tokenTimeout)),
		readJ1: j1,
		skew:   skew,
		now:    time.Now,
	}
}

// Token returns a valid J2 token, fetching a fresh one when the cache is empty or
// past its (skew-adjusted) expiry.
func (p *tokenProvider) Token(ctx context.Context) (string, error) {
	if e := p.cached.Load(); e != nil && p.now().Before(e.expiresAt) {
		return e.token, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Re-check under the lock: another goroutine may have refreshed while we
	// waited, so at most one fetch happens per expiry.
	if e := p.cached.Load(); e != nil && p.now().Before(e.expiresAt) {
		return e.token, nil
	}
	return p.fetchLocked(ctx)
}

// Refresh forces a fresh J2 token. stale is the token the caller just used and had
// rejected; if the cache has already advanced past it (another goroutine
// refreshed), the newer token is returned without a redundant fetch.
func (p *tokenProvider) Refresh(ctx context.Context, stale string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.cached.Load(); e != nil && e.token != "" && e.token != stale {
		return e.token, nil
	}
	return p.fetchLocked(ctx)
}

// fetchLocked calls the accessToken API, sending the J1 token in the JSON body
// ({"key": <J1>}), and caches the J2 result. Caller must hold p.mu.
func (p *tokenProvider) fetchLocked(ctx context.Context) (string, error) {
	key, err := p.readJ1()
	if err != nil {
		return "", fmt.Errorf("read j1 token: %w", err)
	}
	resp, err := p.client.R().
		SetContext(ctx).
		SetBody(accessTokenRequest{Key: key}).
		Post("")
	if err != nil {
		return "", fmt.Errorf("request access token: %w", err)
	}
	if resp.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("access token endpoint status %d", resp.StatusCode())
	}
	var out accessTokenResponse
	if err := json.Unmarshal(resp.Body(), &out); err != nil {
		return "", fmt.Errorf("decode access token response: %w", err)
	}
	if out.Token == "" {
		return "", fmt.Errorf("access token response missing token")
	}
	exp, err := time.Parse(time.RFC3339, out.ExpiresAt)
	if err != nil {
		return "", fmt.Errorf("parse access token expiresAt %q: %w", out.ExpiresAt, err)
	}
	p.cached.Store(&tokenEntry{token: out.Token, expiresAt: exp.Add(-p.skew)})
	return out.Token, nil
}
