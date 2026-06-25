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

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
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

// AuthenticatedUser is the identity resolved from a validated token.
type AuthenticatedUser struct {
	model.User
	Email string
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
			c.Header("Access-Control-Allow-Headers", "Content-Type, "+ssoTokenName+", X-Request-ID")
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

// authMiddleware validates the ssoToken header and stores an AuthenticatedUser
// in the Gin context. In dev mode the header value is treated as the account.
func authMiddleware(v TokenValidator, devMode bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		token := tokenFromRequest(c)
		if token == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ssoToken",
				errcode.WithReason(errcode.AuthMissingFields)))
			c.Abort()
			return
		}

		var user AuthenticatedUser
		if devMode {
			user = AuthenticatedUser{
				User:  model.User{Account: token, EngName: token},
				Email: token + "@dev.local",
			}
		} else {
			claims, err := v.Validate(ctx, token)
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
