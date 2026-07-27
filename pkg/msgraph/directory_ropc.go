package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// directoryClient is a ROPC (resource-owner password) backed DirectoryReader.
// It mirrors presenceClient: its own token cache and grant_type=password token
// acquisition, delegating the actual account->objectID resolution to the shared
// resolveAccountIDs helper. Used by room-service to resolve Teams meeting
// organizer/attendee object IDs via a User.Read.All service account.
type directoryClient struct {
	cfg       Config
	creds     ROPCCredentials
	hc        *http.Client
	baseURL   string
	tokenURL  string
	userAgent string

	mu      sync.Mutex
	token   string
	tokenAt time.Time
}

// NewDirectoryROPCClient builds a ROPC-backed DirectoryReader. Like
// NewPresenceClient it resolves options via a throwaway graphClient, applies
// cfg.ProxyURL (reporting an invalid value at construction), and resolves the
// User-Agent.
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewDirectoryROPCClient(cfg Config, creds ROPCCredentials, opts ...Option) (DirectoryReader, error) {
	g := New(cfg, opts...).(*graphClient)
	hc := g.httpClient
	if err := applyProxyURL(hc, cfg.ProxyURL); err != nil {
		return nil, err
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	return &directoryClient{
		cfg: cfg, creds: creds, hc: hc, baseURL: g.baseURL, tokenURL: g.tokenURL, userAgent: ua,
	}, nil
}

// accessToken returns a cached delegated bearer token, fetching a fresh one via
// the resource-owner password (ROPC) grant when the cache is empty or expiring.
func (d *directoryClient) accessToken(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.token != "" && time.Now().Before(d.tokenAt) {
		return d.token, nil
	}
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", d.cfg.ClientID)
	form.Set("client_secret", d.cfg.ClientSecret)
	form.Set("scope", graphScope)
	form.Set("username", d.creds.Username)
	form.Set("password", d.creds.Password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build directory ropc token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", d.userAgent)
	resp, err := d.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("request directory ropc token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read directory ropc token response: %w", err)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("decode directory ropc token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || tr.AccessToken == "" {
		// Never log credentials; surface only the OAuth error code.
		return "", fmt.Errorf("directory ropc token endpoint returned status %d: %s", resp.StatusCode, tr.Error)
	}
	d.token = tr.AccessToken
	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		lifetime = tokenExpirySkew
	}
	d.tokenAt = time.Now().Add(lifetime - tokenExpirySkew)
	return d.token, nil
}

// ResolveAccountIDs resolves account local-parts to Azure object IDs using a
// ROPC-acquired delegated token.
func (d *directoryClient) ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire directory ropc token: %w", err)
	}
	return resolveAccountIDs(ctx, d.hc, d.baseURL, d.userAgent, token, accounts)
}
