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
	Extra             map[string]interface{}
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
}

// Validator verifies OIDC tokens against an issuer's JWKS endpoint.
type Validator struct {
	verifier   *oidc.IDTokenVerifier
	httpClient *http.Client
	audiences  []string
}

const issuerDiscoveryTimeout = 10 * time.Second

// NewValidator connects to the OIDC issuer and fetches its JWKS keys.
func NewValidator(ctx context.Context, cfg Config) (*Validator, error) {
	if len(cfg.Audiences) == 0 {
		return nil, ErrNoAudiences
	}

	var httpClient *http.Client

	if cfg.TLSSkipVerify {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				// #nosec G402 -- InsecureSkipVerify is opt-in via TLSSkipVerify config for dev environments
				InsecureSkipVerify: true, //nolint:gosec
				MinVersion:         tls.VersionTLS12,
			},
		}
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   issuerDiscoveryTimeout,
		}
		ctx = oidc.ClientContext(ctx, httpClient)
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, issuerDiscoveryTimeout)
		defer cancel()
	}

	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("connect to oidc issuer %q: %w", cfg.IssuerURL, err)
	}

	// SkipClientIDCheck: we enforce a multi-audience allow-list ourselves below.
	oidcConfig := &oidc.Config{SkipClientIDCheck: true}

	return &Validator{
		verifier:   provider.Verifier(oidcConfig),
		httpClient: httpClient,
		audiences:  cfg.Audiences,
	}, nil
}

// Validate verifies the raw OIDC token string and extracts user claims.
// Returns ErrTokenExpired when go-oidc reports an expired exp claim.
func (v *Validator) Validate(ctx context.Context, rawToken string) (Claims, error) {
	if v.httpClient != nil {
		ctx = oidc.ClientContext(ctx, v.httpClient)
	}

	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		var expErr *oidc.TokenExpiredError
		if errors.As(err, &expErr) {
			return Claims{}, ErrTokenExpired
		}
		return Claims{}, fmt.Errorf("oidc token verification failed: %w", err)
	}

	if !containsAudience(idToken.Audience, v.audiences) {
		slog.Warn("oidc audience mismatch", "token_aud", idToken.Audience, "allowed", v.audiences)
		return Claims{}, ErrAudienceNotAllowed
	}

	var tokenClaims struct {
		Email             string `json:"email"`
		Name              string `json:"name"`
		PreferredUsername string `json:"preferred_username"`
		GivenName         string `json:"given_name"`
		FamilyName        string `json:"family_name"`
		Description       string `json:"description"`
		DeptID            string `json:"deptid"`
		DeptName          string `json:"deptname"`
	}

	if err := idToken.Claims(&tokenClaims); err != nil {
		return Claims{}, fmt.Errorf("parse oidc token claims: %w", err)
	}

	var allClaims map[string]interface{}
	if err := idToken.Claims(&allClaims); err != nil {
		return Claims{}, fmt.Errorf("parse oidc extra claims: %w", err)
	}
	for _, key := range []string{
		"sub", "email", "name", "preferred_username",
		"given_name", "family_name", "description", "deptid", "deptname",
		"iss", "aud", "exp", "iat", "nbf", "jti",
		"typ", "sid", "at_hash", "email_verified",
	} {
		delete(allClaims, key)
	}

	return Claims{
		Subject:           idToken.Subject,
		Email:             tokenClaims.Email,
		Name:              tokenClaims.Name,
		PreferredUsername: tokenClaims.PreferredUsername,
		GivenName:         tokenClaims.GivenName,
		FamilyName:        tokenClaims.FamilyName,
		Description:       tokenClaims.Description,
		DeptID:            tokenClaims.DeptID,
		DeptName:          tokenClaims.DeptName,
		Extra:             allClaims,
	}, nil
}

func containsAudience(tokenAud, allowed []string) bool {
	return slices.ContainsFunc(tokenAud, func(t string) bool {
		return slices.Contains(allowed, t)
	})
}
