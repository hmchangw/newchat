package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/principal"
)

const ctxUserKey = "auth_user"

// ssoTokenName is the shared header and cookie key for the SSO token. HandleSetCookie
// writes this cookie and tokenFromRequest reads it — they must match, so keep one name.
const ssoTokenName = "ssoToken"

// TokenValidator validates a TSSO token and returns OIDC claims.
// Satisfied by *pkg/oidc.Validator.
type TokenValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

// AuthenticatedUser is the identity resolved from a validated credential.
type AuthenticatedUser struct {
	model.User
	Email string
	// Session is the botplatform principal for session-token callers, nil for SSO.
	// Handlers branch on it for setCookie and the Drive email guard.
	Session *principal.Principal
}

// userFromContext returns the AuthenticatedUser set by authMiddleware.
func userFromContext(c *gin.Context) (*AuthenticatedUser, bool) {
	v, ok := c.Get(ctxUserKey)
	if !ok {
		return nil, false
	}
	u, ok := v.(*AuthenticatedUser)
	return u, ok
}

// requestIDMiddleware extracts or mints the request correlation ID.
func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(natsutil.RequestIDHeader)
		if !idgen.IsValidUUID(id) {
			id = idgen.GenerateRequestID()
		}
		c.Set("request_id", id)
		c.Request = c.Request.WithContext(natsutil.WithRequestID(c.Request.Context(), id))
		c.Header(natsutil.RequestIDHeader, id)
		c.Next()
	}
}

// accessLogMiddleware logs one structured line per request.
func accessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.Info("request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", c.ClientIP(),
			"bot_account", c.GetString("bot_account"),
		)
	}
}

// corsMiddleware emits credentialed CORS headers for an allowlisted Origin (wildcard is
// illegal with credentials; a disallowed origin gets none) and answers preflight OPTIONS
// with 204. Register at the engine level so preflight, which falls to NoRoute, reaches it.
func corsMiddleware(allowed []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Once an allowlist is configured the response varies by Origin, so a shared cache
		// must key on it — set Vary even when this Origin isn't allowed, else the cache
		// could serve a no-Allow-Origin response to a later allowed-origin request.
		if len(allowed) > 0 {
			c.Header("Vary", "Origin")
		}
		origin := c.GetHeader("Origin")
		if origin != "" && slices.Contains(allowed, origin) {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, "+ssoTokenName+", X-Request-ID, "+
				botauth.HeaderUserID+", "+botauth.HeaderAuthToken)
			c.Header("Access-Control-Max-Age", "300")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// tokenFromRequest returns the ssoToken from the request header, falling back to
// the ssoToken cookie. <img>-driven download requests cannot set headers, so they
// carry the token in the cookie instead; header-first keeps existing callers exact.
func tokenFromRequest(c *gin.Context) string {
	if t := c.GetHeader(ssoTokenName); t != "" {
		return t
	}
	t, _ := c.Cookie(ssoTokenName)
	return t
}

// authDeps is what authMiddleware needs to resolve either credential — a struct
// rather than four positional parameters.
type authDeps struct {
	sso            TokenValidator
	bot            botauth.TokenValidator
	botEmailDomain string
	devMode        bool
}

// authMiddleware resolves either credential into an AuthenticatedUser. Two explicit
// headers are ambiguous; x-auth-token beats an ssoToken cookie (ambient, not explicit).
func authMiddleware(d authDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		botUserID, botToken := botauth.Credentials(c.Request.Header)
		if botToken != "" && c.GetHeader(ssoTokenName) != "" {
			errhttp.Write(ctx, c, errcode.BadRequest("set exactly one of ssoToken / x-auth-token",
				errcode.WithReason(errcode.BotplatformAmbiguousToken)))
			c.Abort()
			return
		}

		// Half a session credential is still a session attempt: route it here so it
		// gets the uniform 401 rather than the SSO branch's "missing ssoToken".
		if botToken != "" || (botUserID != "" && tokenFromRequest(c) == "") {
			user, err := d.sessionUser(ctx, botUserID, botToken)
			if err != nil {
				errhttp.Write(ctx, c, err)
				c.Abort()
				return
			}
			c.Set(ctxUserKey, user)
			c.Set("bot_account", user.Account)
			c.Next()
			return
		}

		token := tokenFromRequest(c)
		if token == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ssoToken",
				errcode.WithReason(errcode.AuthMissingFields)))
			c.Abort()
			return
		}

		var user AuthenticatedUser
		if d.devMode {
			user = AuthenticatedUser{
				User:  model.User{Account: token, EngName: token},
				Email: token + "@dev.local",
			}
		} else {
			claims, err := d.sso.Validate(ctx, token)
			if err != nil {
				if errors.Is(err, pkgoidc.ErrTokenExpired) {
					errhttp.Write(ctx, c, errcode.Unauthenticated("sso token has expired, please re-login",
						errcode.WithReason(errcode.AuthTokenExpired)))
					c.Abort()
					return
				}
				errhttp.Write(ctx, c, errcode.Unauthenticated("invalid sso token",
					errcode.WithReason(errcode.AuthInvalidToken)))
				c.Abort()
				return
			}
			account := claims.PreferredUsername
			if account == "" {
				account = claims.Name
			}
			engName, chineseName := parseDescription(claims.Description)
			user = AuthenticatedUser{
				User: model.User{
					Account:     account,
					EngName:     engName,
					ChineseName: chineseName,
				},
				Email: claims.Email,
			}
		}

		c.Set(ctxUserKey, &user)
		c.Next()
	}
}

// sessionUser resolves a session token into an AuthenticatedUser. The nil-validator
// branch is unreachable in production but keeps the failure explicit.
//
// Unlike media-service, no p.SiteID check: every endpoint here is gated on room
// membership, which a foreign-site session cannot fake, so locality would add
// nothing. media-service needs it because its PUTs write site-wide assets that
// membership cannot gate.
func (d authDeps) sessionUser(ctx context.Context, userID, token string) (*AuthenticatedUser, error) {
	if d.bot == nil {
		return nil, errcode.Unavailable("session-token auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	p, err := botauth.Authenticate(ctx, d.bot, userID, token)
	if err != nil {
		return nil, err
	}
	return &AuthenticatedUser{
		User:    model.User{Account: p.Account, SiteID: p.SiteID},
		Email:   botEmail(p.Account, d.botEmailDomain),
		Session: &p,
	}, nil
}

// botEmail returns the Drive attribution address for a session caller; empty by
// default, since nothing here reads the value back.
func botEmail(account, domain string) string {
	if domain == "" {
		return ""
	}
	return account + "@" + domain
}

// parseDescription extracts engName and chineseName from the
// "employeeId, engName, chineseName" claim; the employeeId field is unused here.
func parseDescription(desc string) (engName, chineseName string) {
	parts := strings.SplitN(desc, ",", 3)
	if len(parts) >= 2 {
		engName = strings.TrimSpace(parts[1])
	}
	if len(parts) >= 3 {
		chineseName = strings.TrimSpace(parts[2])
	}
	return
}
