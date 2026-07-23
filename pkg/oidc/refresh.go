package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrRefreshRejected marks an OAuth-level refresh rejection (invalid_grant et al.), not a transport/server failure.
	ErrRefreshRejected = errors.New("oidc: refresh token rejected by issuer")
	// ErrNoAccessToken marks a token response without an access_token.
	ErrNoAccessToken = errors.New("oidc: token response missing access_token")
)

const maxTokenResponseBytes = 1 << 20

// TokenSet is the verified outcome of a refresh grant; SSOToken is the response's access_token, the platform's "ssoToken" convention.
type TokenSet struct {
	SSOToken     string
	RefreshToken string
	Account      string // preferred_username of the verified access token
	Expiry       time.Time
}

// Refresh exchanges refreshToken (public client) and verifies the returned access token before returning it. OAuth rejections wrap ErrRefreshRejected; no token logs.
func (v *Validator) Refresh(ctx context.Context, refreshToken string) (TokenSet, error) {
	if v.tokenEndpoint == "" {
		return TokenSet{}, errors.New("oidc: issuer exposes no token_endpoint")
	}
	if v.clientID == "" {
		return TokenSet{}, errors.New("oidc: refresh requires Config.ClientID")
	}

	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {v.clientID},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return TokenSet{}, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return TokenSet{}, fmt.Errorf("post token endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseBytes))
	if err != nil {
		return TokenSet{}, fmt.Errorf("read token response: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized:
		// Only the safe `error` code is surfaced; a parse miss just leaves it empty.
		var oauthErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &oauthErr)
		return TokenSet{}, fmt.Errorf("%w: %s", ErrRefreshRejected, oauthErr.Error)
	case resp.StatusCode != http.StatusOK:
		return TokenSet{}, fmt.Errorf("token endpoint returned status %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return TokenSet{}, fmt.Errorf("parse token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return TokenSet{}, ErrNoAccessToken
	}

	// Re-verify the minted access token against the same OIDC_AUDIENCES allow-list used
	// at set time: the access token's `aud` MUST be an allowed audience or every refresh
	// fails (ErrAudienceNotAllowed) → forced re-login. Configure the realm accordingly.
	claims, err := v.Validate(ctx, tokenResp.AccessToken)
	if err != nil {
		return TokenSet{}, fmt.Errorf("verify refreshed access token: %w", err)
	}

	return TokenSet{
		SSOToken:     tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		Account:      claims.Account(),
		Expiry:       claims.Expiry,
	}, nil
}
