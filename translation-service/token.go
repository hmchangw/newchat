package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// accessTokenResponse is the accessToken API reply. token is the J2 token used as
// the Authorization header on the translate API.
type accessTokenResponse struct {
	Token        string `json:"token"`
	ExpiresAt    string `json:"expiresAt"`
	Username     string `json:"username"`
	JwtRequestID string `json:"jwtRequestId"`
}

// tokenProvider exchanges the J1 token (from config/env) for a J2 token via the
// accessToken API, caching it until shortly before expiresAt (minus skew). Safe
// for concurrent use.
type tokenProvider struct {
	client  *resty.Client
	j1Token string
	skew    time.Duration
	now     func() time.Time

	mu        sync.Mutex
	token     string    // cached J2 token
	expiresAt time.Time // cache validity (parsed expiresAt minus skew)
}

func newTokenProvider(accessTokenURL, j1Token string, timeout, skew time.Duration) *tokenProvider {
	return &tokenProvider{
		client:  restyutil.New(accessTokenURL, restyutil.WithTimeout(timeout)),
		j1Token: j1Token,
		skew:    skew,
		now:     time.Now,
	}
}

// Token returns a valid J2 token, fetching a fresh one when the cache is empty or
// past its (skew-adjusted) expiry.
func (p *tokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && p.now().Before(p.expiresAt) {
		return p.token, nil
	}
	return p.fetchLocked(ctx)
}

// Refresh forces a fresh J2 token. stale is the token the caller just used and had
// rejected; if the cache has already advanced past it (another goroutine
// refreshed), the newer token is returned without a redundant fetch.
func (p *tokenProvider) Refresh(ctx context.Context, stale string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && p.token != stale {
		return p.token, nil
	}
	return p.fetchLocked(ctx)
}

// fetchLocked calls the accessToken API with the J1 token and caches the J2
// result. Caller must hold p.mu.
func (p *tokenProvider) fetchLocked(ctx context.Context) (string, error) {
	resp, err := p.client.R().
		SetContext(ctx).
		SetHeader("Authorization", p.j1Token).
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
	p.token = out.Token
	p.expiresAt = exp.Add(-p.skew)
	return p.token, nil
}
