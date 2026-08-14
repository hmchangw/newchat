package oidc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Claims holds the validated identity extracted from an OIDC token.
type Claims struct {
	Subject           string
	Email             string
	Name              string
	PreferredUsername string
	GivenName         string
	FamilyName        string
	Description       string
	DeptID            string
	DeptName          string
	// Expiry is the verified token's exp claim (zero when unset).
	Expiry time.Time
	// Issuer is the token's iss claim. Surfaced for logging: under
	// Config.SkipIssuerCheck it is the only record of which hostname minted the token.
	Issuer string
	Extra  map[string]interface{}
}

// Account returns preferred_username — the only claim trusted as a principal;
// name is user-editable display data. Empty means callers must reject the token.
func (c *Claims) Account() string {
	return c.PreferredUsername
}

var (
	ErrTokenExpired       = errors.New("oidc: token has expired")
	ErrNoAudiences        = errors.New("oidc: at least one allowed audience is required")
	ErrAudienceNotAllowed = errors.New("oidc: token audience not in allowed list")
)

// Config controls how the OIDC validator behaves.
type Config struct {
	IssuerURL string
	// A token is accepted when any of its `aud` claim entries appears here.
	Audiences     []string
	TLSSkipVerify bool
	// SkipIssuerCheck accepts a token whose `iss` differs from IssuerURL — for a realm
	// fronted by several ingress hostnames, where `iss` follows the host that minted the
	// token. Safe ONLY while every such hostname serves the same realm keys: the JWKS
	// fetched from IssuerURL stays the trust anchor, so a token from a different realm
	// still fails the signature check. Never enable it against a multi-tenant issuer
	// (Azure `common`, or realms sharing a keyset) — there `iss` is the only tenant boundary.
	SkipIssuerCheck bool
	// ClientID is the OAuth client used by Refresh (public client, no secret); validators that never call Refresh may omit it.
	ClientID string
}

// Validator verifies OIDC tokens against an issuer's JWKS endpoint.
type Validator struct {
	verifier      *oidc.IDTokenVerifier
	httpClient    *http.Client // go-oidc mandates an *http.Client (ClientContext); the refresh POST reuses it (not Resty) for one instrumented client + connection reuse
	audiences     []string
	tokenEndpoint string
	clientID      string
}

const httpTimeout = 10 * time.Second

// NewValidator connects to the OIDC issuer and fetches its JWKS keys.
func NewValidator(ctx context.Context, cfg Config) (*Validator, error) {
	if len(cfg.Audiences) == 0 {
		return nil, ErrNoAudiences
	}

	transport := http.DefaultTransport
	if cfg.TLSSkipVerify {
		transport = insecureTLSTransport()
	}
	// otelhttp instruments the client shared by go-oidc discovery/JWKS and the refresh
	// POST: emits egress spans and injects traceparent (parity with the Resty helper).
	httpClient := &http.Client{Transport: otelhttp.NewTransport(transport), Timeout: httpTimeout}
	ctx = oidc.ClientContext(ctx, httpClient)

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, httpTimeout)
		defer cancel()
	}

	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("connect to oidc issuer %q: %w", cfg.IssuerURL, err)
	}

	// SkipClientIDCheck: we enforce a multi-audience allow-list ourselves below.
	// SkipIssuerCheck: one realm behind several ingress hostnames mints `iss` per host; the JWKS stays the trust anchor.
	oidcConfig := &oidc.Config{
		SkipClientIDCheck: true,
		SkipIssuerCheck:   cfg.SkipIssuerCheck,

		// go-oidc's remaining knobs, unused here — listed so adopting one later is a
		// one-line change instead of a hunt through the library:
		// ClientID: "x",                     // single expected aud; our Audiences allow-list supersedes it
		// SupportedSigningAlgs: []string{},  // pin accepted algs; defaults to the provider's advertised set
		// SkipExpiryCheck: true,             // accept expired tokens; would break ErrTokenExpired and the refresh window
		// Now: func() time.Time { … },       // clock source for exp/nbf; inject to test skew
		// InsecureSkipSignatureCheck: true,  // accepts ANY token — never enable, it is the check the others rely on
	}

	// Retain the token_endpoint via provider.Claims, not provider.Endpoint() (the latter would make golang.org/x/oauth2 a direct dependency).
	var meta struct {
		TokenEndpoint string `json:"token_endpoint"`
	}
	if err := provider.Claims(&meta); err != nil {
		return nil, fmt.Errorf("parse oidc issuer metadata: %w", err)
	}
	// Fail fast at startup rather than on the first Refresh in production.
	if cfg.ClientID != "" && meta.TokenEndpoint == "" {
		return nil, fmt.Errorf("oidc: issuer %q exposes no token_endpoint but ClientID is set", cfg.IssuerURL)
	}

	return &Validator{
		verifier:      provider.Verifier(oidcConfig),
		httpClient:    httpClient,
		audiences:     cfg.Audiences,
		tokenEndpoint: meta.TokenEndpoint,
		clientID:      cfg.ClientID,
	}, nil
}

// insecureTLSTransport clones http.DefaultTransport (keeping its idle/handshake
// timeouts and HTTP/2 defaults) and skips TLS verification — TLSSkipVerify, dev only.
func insecureTLSTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{
		// #nosec G402 -- InsecureSkipVerify is opt-in via TLSSkipVerify config for dev environments
		InsecureSkipVerify: true, //nolint:gosec
		MinVersion:         tls.VersionTLS12,
	}
	return t
}

// Validate verifies the raw OIDC token string and extracts user claims.
// Returns ErrTokenExpired when go-oidc reports an expired exp claim.
func (v *Validator) Validate(ctx context.Context, rawToken string) (Claims, error) {
	ctx = oidc.ClientContext(ctx, v.httpClient)

	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		var expErr *oidc.TokenExpiredError
		if errors.As(err, &expErr) {
			return Claims{}, ErrTokenExpired
		}
		return Claims{}, fmt.Errorf("oidc token verification failed: %w", err)
	}

	if !containsAudience(idToken.Audience, v.audiences) {
		slog.WarnContext(ctx, "oidc audience mismatch", "token_aud", idToken.Audience, "allowed", v.audiences)
		return Claims{}, ErrAudienceNotAllowed
	}

	// Single decode of the JWT payload; typed fields pulled from the same map that
	// becomes Extra (minus the known keys), avoiding a second idToken.Claims parse.
	var allClaims map[string]interface{}
	if err := idToken.Claims(&allClaims); err != nil {
		return Claims{}, fmt.Errorf("parse oidc token claims: %w", err)
	}
	str := func(key string) string {
		s, _ := allClaims[key].(string)
		return s
	}
	claims := Claims{
		Subject:           idToken.Subject,
		Email:             str("email"),
		Name:              str("name"),
		PreferredUsername: str("preferred_username"),
		GivenName:         str("given_name"),
		FamilyName:        str("family_name"),
		Description:       str("description"),
		DeptID:            str("deptid"),
		DeptName:          str("deptname"),
		Expiry:            idToken.Expiry,
		Issuer:            idToken.Issuer,
	}
	for _, key := range []string{
		"sub", "email", "name", "preferred_username",
		"given_name", "family_name", "description", "deptid", "deptname",
		"iss", "aud", "exp", "iat", "nbf", "jti",
		"typ", "sid", "at_hash", "email_verified",
	} {
		delete(allClaims, key)
	}
	claims.Extra = allClaims
	return claims, nil
}

func containsAudience(tokenAud, allowed []string) bool {
	return slices.ContainsFunc(tokenAud, func(t string) bool {
		return slices.Contains(allowed, t)
	})
}
