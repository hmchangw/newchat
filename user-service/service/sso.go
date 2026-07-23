package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/user-service/models"
)

// The not-configured sentinel reuses upstream_unavailable (auth-service BOTPLATFORM_URL-unset precedent).
var (
	errSSONotConfigured = errcode.Unavailable("sso is not configured on this site", errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	errSSOTokenMismatch = errcode.BadRequest("sso token does not belong to this account")
	// On refresh a mismatch means the STORED refresh token minted another identity's token —
	// a server-side integrity anomaly, not a client error; drive re-login per spec §8.
	errSSORefreshMismatch = errcode.Unauthenticated("refreshed sso token does not belong to this account, please re-login", errcode.WithReason(errcode.AuthTokenExpired))
)

// SSOSet verifies and stores the caller's own SSO token pair. The {account} subject token is the
// NATS-JWT-authenticated caller, so no admin gate or user lookup is needed — the frontend calls
// this on every login.
func (s *UserService) SSOSet(c *natsrouter.Context, req models.SSOSetRequest) (*models.OKResponse, error) {
	// ssoTokens is nil only in unit tests — prod wires the repo unconditionally.
	if s.tokenValidator == nil || s.ssoTokens == nil {
		return nil, errSSONotConfigured
	}
	account := c.Param("account")
	c.WithLogValues("account", account)

	if req.SSOToken == "" || req.RefreshToken == "" {
		return nil, errcode.BadRequest("ssoToken and refreshToken are required", errcode.WithReason(errcode.AuthMissingFields))
	}

	claims, err := s.tokenValidator.Validate(c, req.SSOToken)
	if err != nil {
		if errors.Is(err, oidc.ErrTokenExpired) {
			return nil, errcode.Unauthenticated("sso token has expired", errcode.WithReason(errcode.AuthTokenExpired))
		}
		// Cause carries the verification error (never token bytes) to the server log only — auth-service handleSSO precedent.
		return nil, errcode.Unauthenticated("invalid sso token", errcode.WithReason(errcode.AuthInvalidToken), errcode.WithCause(err))
	}
	// Token-owner integrity: the verified token must belong to the caller storing it.
	if claims.Account() != account {
		return nil, errSSOTokenMismatch
	}

	if err := s.ssoTokens.Upsert(c, account, req.SSOToken, claims.Expiry.UnixMilli(), req.RefreshToken); err != nil {
		return nil, fmt.Errorf("store sso token: %w", err)
	}
	return &models.OKResponse{Success: true}, nil
}

// SSORefresh returns the caller's stored ssoToken, refreshing when within ssoRefreshWindow of
// expiry. Self-service only — the {account} subject token is the NATS-JWT-authenticated caller.
func (s *UserService) SSORefresh(c *natsrouter.Context, _ models.SSORefreshRequest) (*models.SSORefreshResponse, error) {
	// ssoTokens is nil only in unit tests — prod wires the repo unconditionally.
	if s.tokenRefresher == nil || s.ssoTokens == nil {
		return nil, errSSONotConfigured
	}
	account := c.Param("account")
	c.WithLogValues("account", account)

	stored, err := s.ssoTokens.GetByUsername(c, account)
	if err != nil {
		return nil, fmt.Errorf("get sso token: %w", err)
	}
	if stored == nil {
		return nil, errcode.NotFound("no sso token stored for this account", errcode.WithReason(errcode.UserSSOTokenNotFound))
	}

	if time.UnixMilli(stored.IDTokenExp).After(time.Now().Add(s.ssoRefreshWindow)) {
		return &models.SSORefreshResponse{SSOToken: stored.IDToken}, nil
	}

	ts, err := s.tokenRefresher.Refresh(c, stored.RefreshToken)
	if err != nil {
		// Product decision (spec §8): ANY refresh failure sends the client to re-login; cause carries the refresh error (never token bytes).
		return nil, errcode.Unauthenticated("sso token has expired, please re-login", errcode.WithReason(errcode.AuthTokenExpired), errcode.WithCause(err))
	}
	// Owner integrity at refresh time: a mismatched stored refresh token must never mint another identity's tokens under this account.
	if ts.Account != account {
		return nil, errSSORefreshMismatch
	}
	// Keep the stored refresh token if the response omits a rotated one.
	newRefresh := ts.RefreshToken
	if newRefresh == "" {
		newRefresh = stored.RefreshToken
	}
	if err := s.ssoTokens.Upsert(c, account, ts.SSOToken, ts.Expiry.UnixMilli(), newRefresh); err != nil {
		return nil, fmt.Errorf("store refreshed sso token: %w", err)
	}
	return &models.SSORefreshResponse{SSOToken: ts.SSOToken}, nil
}
