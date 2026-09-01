package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// authRequestFields mirrors auth-service's authRequest wire shape.
type authRequestFields struct {
	SSOToken      string `json:"ssoToken,omitempty"`
	AuthToken     string `json:"authToken,omitempty"`
	Account       string `json:"account,omitempty"`
	NATSPublicKey string `json:"natsPublicKey"`
}

// tokenProvider supplies the auth material for one account. devProvider is
// the only implementation today; a file-backed SSO/session provider is the
// spec's future extension point.
type tokenProvider interface {
	Material(account string) (authRequestFields, error)
}

type devProvider struct{}

func (devProvider) Material(account string) (authRequestFields, error) {
	return authRequestFields{Account: account}, nil
}

type authClient struct {
	rc       *resty.Client
	provider tokenProvider
	metrics  *metrics
}

func newAuthClient(baseURL string, p tokenProvider, m *metrics) *authClient {
	return &authClient{
		// The idle pool must exceed the ramp's mint concurrency: the stdlib
		// default of 2 keep-alive conns per host would push every further
		// mint into a fresh TCP connection and TIME_WAIT (spec §5.3's
		// ephemeral-port budget).
		rc: restyutil.New(baseURL,
			restyutil.WithTimeout(10*time.Second),
			restyutil.WithMaxIdleConns(64)),
		provider: p,
		metrics:  m,
	}
}

type authMintResponse struct {
	NATSJWT string `json:"natsJwt"`
}

type authErrorEnvelope struct {
	Code    string `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"error"`
}

// Mint runs the full auth exchange and returns the freshly minted NATS user
// JWT. It never logs the JWT or any token material.
func (c *authClient) Mint(ctx context.Context, account, natsPubKey string) (string, error) {
	fields, err := c.provider.Material(account)
	if err != nil {
		return "", fmt.Errorf("token material for %s: %w", account, err)
	}
	fields.NATSPublicKey = natsPubKey

	start := time.Now()
	var ok authMintResponse
	var bad authErrorEnvelope
	resp, err := c.rc.R().SetContext(ctx).
		SetBody(fields).SetResult(&ok).SetError(&bad).
		Post("/api/v1/auth")
	if err != nil {
		c.metrics.AuthFailures.Inc()
		return "", fmt.Errorf("auth exchange: %w", err)
	}
	if resp.IsError() {
		c.metrics.AuthFailures.Inc()
		return "", fmt.Errorf("auth exchange rejected (%d %s/%s): %s",
			resp.StatusCode(), bad.Code, bad.Reason, bad.Message)
	}
	if ok.NATSJWT == "" {
		c.metrics.AuthFailures.Inc()
		return "", fmt.Errorf("auth exchange returned no natsJwt (status %d)", resp.StatusCode())
	}
	// Success-only observation: mixing 4xx/5xx durations into the same
	// histogram would corrupt the auth-latency signal.
	c.metrics.AuthDuration.Observe(time.Since(start).Seconds())
	return ok.NATSJWT, nil
}
