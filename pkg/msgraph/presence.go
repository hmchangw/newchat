package msgraph

import (
	"bytes"
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

// Presence is the subset of a Graph presence resource we consume.
type Presence struct {
	ID           string `json:"id"`
	Availability string `json:"availability"`
	Activity     string `json:"activity"`
}

// ROPCCredentials are the service-account credentials for the resource-owner
// password grant used to read presence (Presence.Read.All is delegated).
type ROPCCredentials struct {
	Username string
	Password string
}

// PresenceReader is the Graph presence surface the sync depends on.
type PresenceReader interface {
	GetPresencesByUserId(ctx context.Context, ids []string) ([]Presence, error)
}

// maxPresenceIDs is Graph's documented per-request cap for getPresencesByUserId.
const maxPresenceIDs = 650

type presenceClient struct {
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

// NewPresenceClient builds an ROPC-backed presence reader. It reuses the
// app-only client's options (WithHTTPClient/WithBaseURL/WithTokenURL) by
// constructing a throwaway graphClient to resolve them. When cfg.ProxyURL is
// set, the presence client's transport is pointed at that proxy (overriding
// HTTPS_PROXY); an invalid ProxyURL is reported at construction so it fails
// fast rather than surfacing as an opaque per-request error.
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewPresenceClient(cfg Config, creds ROPCCredentials, opts ...Option) (PresenceReader, error) {
	g := New(cfg, opts...).(*graphClient)
	hc := g.httpClient
	if cfg.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse graph proxy url: %w", err)
		}
		if proxyURL.Scheme == "" || proxyURL.Host == "" {
			// Redacted() masks any embedded proxy credentials before it reaches logs.
			return nil, fmt.Errorf("invalid graph proxy url %q: scheme and host are required", proxyURL.Redacted())
		}
		// Reuse the throwaway client's transport when it is already a concrete
		// *http.Transport (preserving TLSInsecureSkipVerify settings); otherwise
		// clone the default transport so proxy/dial defaults survive.
		tr, ok := hc.Transport.(*http.Transport)
		if !ok || tr == nil {
			tr = http.DefaultTransport.(*http.Transport).Clone()
		}
		tr.Proxy = http.ProxyURL(proxyURL)
		hc.Transport = tr
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	return &presenceClient{
		cfg: cfg, creds: creds, hc: hc, baseURL: g.baseURL, tokenURL: g.tokenURL, userAgent: ua,
	}, nil
}

// accessToken returns a cached delegated bearer token, fetching a fresh one via
// the resource-owner password (ROPC) grant when the cache is empty or expiring.
func (p *presenceClient) accessToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Now().Before(p.tokenAt) {
		return p.token, nil
	}
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", p.cfg.ClientID)
	form.Set("client_secret", p.cfg.ClientSecret)
	form.Set("scope", graphScope)
	form.Set("username", p.creds.Username)
	form.Set("password", p.creds.Password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build ropc token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", p.userAgent)
	resp, err := p.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("request ropc token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read ropc token response: %w", err)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("decode ropc token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || tr.AccessToken == "" {
		// Never log credentials; surface only the OAuth error code.
		return "", fmt.Errorf("ropc token endpoint returned status %d: %s", resp.StatusCode, tr.Error)
	}
	p.token = tr.AccessToken
	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		lifetime = tokenExpirySkew
	}
	p.tokenAt = time.Now().Add(lifetime - tokenExpirySkew)
	return p.token, nil
}

// GetPresencesByUserId returns Teams presence for the given Azure object IDs,
// batched at Graph's per-request cap.
func (p *presenceClient) GetPresencesByUserId(ctx context.Context, ids []string) ([]Presence, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire presence token: %w", err)
	}
	var out []Presence
	for start := 0; start < len(ids); start += maxPresenceIDs {
		end := start + maxPresenceIDs
		if end > len(ids) {
			end = len(ids)
		}
		batch, err := p.fetch(ctx, token, ids[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
	}
	return out, nil
}

func (p *presenceClient) fetch(ctx context.Context, token string, ids []string) ([]Presence, error) {
	payload, err := json.Marshal(struct {
		IDs []string `json:"ids"`
	}{IDs: ids})
	if err != nil {
		return nil, fmt.Errorf("marshal presence ids: %w", err)
	}
	endpoint := p.baseURL + "/communications/getPresencesByUserId"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build presence request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", p.userAgent)
	resp, err := p.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get presences: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("read presence response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get presences: graph returned status %d", resp.StatusCode)
	}
	var pr struct {
		Value []Presence `json:"value"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("decode presence response: %w", err)
	}
	return pr.Value, nil
}
