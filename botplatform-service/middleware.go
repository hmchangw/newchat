package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

// ctxBotPrincipal is the gin context key requireBot stores the *session.Session under.
const ctxBotPrincipal = "botPrincipal"

// accessLogMiddleware emits one structured line per request.
func accessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.InfoContext(c.Request.Context(), "request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", c.ClientIP(),
			"login_outcome", c.GetString("login_outcome"),
			"validate_outcome", c.GetString("validate_outcome"),
			"bot_account", c.GetString("bot_account"),
		)
	}
}

// requireBot enforces plain-bearer identity: x-user-id + x-auth-token must resolve to a live session
// whose UserID matches and whose Roles include bot.
// The three token-shape failures collapse to one 401/invalid_token so the wire doesn't leak which.
// The role gate is a distinct 403/not_a_bot so callers can tell wrong-audience from wrong-token.
func requireBot(sessions session.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		userID := c.GetHeader("x-user-id")
		token := c.GetHeader("x-auth-token")
		if userID == "" || token == "" {
			errhttp.Write(ctx, c, errBotInvalidToken)
			c.Abort()
			return
		}

		sess, err := sessions.FindByHash(ctx, sessiontoken.Hash(token))
		switch {
		case errors.Is(err, session.ErrNotFound):
			errhttp.Write(ctx, c, errBotInvalidToken)
			c.Abort()
			return
		case err != nil:
			errhttp.Write(ctx, c, errcode.Internal("find session", errcode.WithCause(err)))
			c.Abort()
			return
		}

		if sess.UserID != userID {
			errhttp.Write(ctx, c, errBotInvalidToken)
			c.Abort()
			return
		}

		if !containsBotRole(sess.Roles) {
			errhttp.Write(ctx, c, errBotNotABot)
			c.Abort()
			return
		}

		c.Set(ctxBotPrincipal, sess)
		c.Set("bot_account", sess.Account)
		c.Next()
	}
}

// botPrincipalFrom returns the *session.Session stored by requireBot, or nil.
func botPrincipalFrom(c *gin.Context) *session.Session {
	v, ok := c.Get(ctxBotPrincipal)
	if !ok {
		return nil
	}
	s, _ := v.(*session.Session)
	return s
}

// containsBotRole checks the wire-shape []string against UserRoleBot.
func containsBotRole(roles []string) bool {
	return slices.Contains(roles, string(model.UserRoleBot))
}

// incrExClient is the narrow Valkey surface botRateLimit calls.
type incrExClient interface {
	IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error)
}

// botRateLimit enforces per-caller then global fixed-window counters (60s each); 0 disables.
// Per-caller first so a rejected caller doesn't consume the global budget.
func botRateLimit(client incrExClient, perCaller, perGlobal int) gin.HandlerFunc {
	const window = time.Minute

	return func(c *gin.Context) {
		if perCaller <= 0 && perGlobal <= 0 {
			c.Next()
			return
		}

		ctx := c.Request.Context()

		pr := botPrincipalFrom(c)
		if pr == nil {
			errhttp.Write(ctx, c, errcode.Internal("bot rate limit: missing principal"))
			c.Abort()
			return
		}

		if perCaller > 0 {
			n, err := client.IncrEx(ctx, "botrl:caller:"+pr.UserID, window)
			if err != nil {
				errhttp.Write(ctx, c, errcode.Internal("bot rate limit caller", errcode.WithCause(err)))
				c.Abort()
				return
			}
			if n > int64(perCaller) {
				c.Header("Retry-After", "60")
				errhttp.Write(ctx, c, errBotRateLimitedCaller)
				c.Abort()
				return
			}
		}

		if perGlobal > 0 {
			n, err := client.IncrEx(ctx, "botrl:global", window)
			if err != nil {
				errhttp.Write(ctx, c, errcode.Internal("bot rate limit global", errcode.WithCause(err)))
				c.Abort()
				return
			}
			if n > int64(perGlobal) {
				c.Header("Retry-After", "60")
				errhttp.Write(ctx, c, errBotRateLimitedGlobal)
				c.Abort()
				return
			}
		}

		c.Next()
	}
}
