package main

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

// ssoTokenHeader carries the SSO credential, matching upload-service and the
// Drive endpoints the client already calls.
// #nosec G101 -- HTTP header name, not a credential
// nosemgrep: gosec.G101-1
const ssoTokenHeader = "ssoToken"

// ctxAccountKey holds the account resolved from the caller's credential.
const ctxAccountKey = "auth_account"

// ssoValidator validates an SSO token and returns its OIDC claims.
// Satisfied by *pkg/oidc.Validator.
type ssoValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

// authDeps holds the credential validators. sso is nil when SSO is not
// configured on this site; bot is always set by main, so its nil guard is
// unreachable in production but keeps the failure explicit.
type authDeps struct {
	sso ssoValidator
	bot botauth.TokenValidator
}

// accountFromContext returns the account authMiddleware resolved, or "".
func accountFromContext(c *gin.Context) string { return c.GetString(ctxAccountKey) }

// authMiddleware resolves an ssoToken, or an x-user-id/x-auth-token pair, into
// the caller's account. The account never comes from the request, so a caller
// can only read its own data — the same guarantee NATS subject scoping gives.
func authMiddleware(d authDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Classify reads its logger from the ctx; without this the auth failures
		// would be the only lines here carrying no request_id.
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		ssoToken := c.GetHeader(ssoTokenHeader)
		botUserID, botToken := botauth.Credentials(c.Request.Header)

		account, err := d.resolve(ctx, ssoToken, botUserID, botToken)
		if err != nil {
			errhttp.Write(ctx, c, err)
			c.Abort()
			return
		}
		// Carry the enriched ctx forward so the handler logs the same values.
		c.Request = c.Request.WithContext(errcode.WithLogValues(ctx, "account", account))
		c.Set(ctxAccountKey, account)
		c.Next()
	}
}

// resolve returns the account for exactly one supplied credential. Both or
// neither is a client error, never a silent preference between them.
func (d authDeps) resolve(ctx context.Context, ssoToken, botUserID, botToken string) (string, error) {
	if ssoToken != "" && botToken != "" {
		return "", errcode.BadRequest("set exactly one of ssoToken / x-auth-token",
			errcode.WithReason(errcode.BotplatformAmbiguousToken))
	}
	// Half a session credential is still a session attempt: route it here for the
	// uniform 401 rather than the SSO branch's "missing ssoToken".
	if botToken != "" || (botUserID != "" && ssoToken == "") {
		return d.sessionAccount(ctx, botUserID, botToken)
	}
	if ssoToken == "" {
		return "", errcode.Unauthenticated("missing ssoToken",
			errcode.WithReason(errcode.AuthMissingFields))
	}
	return d.ssoAccount(ctx, ssoToken)
}

// ssoAccount verifies the token's signature locally; sessionAccount reads the
// sessions store instead.
func (d authDeps) ssoAccount(ctx context.Context, token string) (string, error) {
	if d.sso == nil {
		return "", errcode.Unavailable("sso auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	claims, err := d.sso.Validate(ctx, token)
	if err != nil {
		if errors.Is(err, pkgoidc.ErrTokenExpired) {
			return "", errcode.Unauthenticated("sso token has expired, please re-login",
				errcode.WithReason(errcode.AuthTokenExpired))
		}
		// The cause carries the verification error, never the token, to the log.
		return "", errcode.Unauthenticated("invalid sso token",
			errcode.WithReason(errcode.AuthInvalidToken), errcode.WithCause(err))
	}
	// Account() is preferred_username alone. No fallback to name: pkg/oidc marks it
	// user-editable display data, so trusting it would let a user pick an identity.
	account := claims.Account()
	if account == "" {
		return "", errcode.Unauthenticated("sso token carries no account",
			errcode.WithReason(errcode.AuthInvalidToken))
	}
	return account, nil
}

// sessionAccount resolves a botplatform session pair against the sessions store.
func (d authDeps) sessionAccount(ctx context.Context, userID, token string) (string, error) {
	if d.bot == nil {
		return "", errcode.Unavailable("session-token auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	p, err := botauth.Authenticate(ctx, d.bot, userID, token)
	if err != nil {
		return "", err
	}
	return p.Account, nil
}
