package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
)

// ctxSessionKey is the gin context key requireSession stores the validated
// principal under.
const ctxSessionKey = "session_principal"

// sessionFromContext returns the principal stored by requireSession, or nil.
func sessionFromContext(c *gin.Context) *principal.Principal {
	v, ok := c.Get(ctxSessionKey)
	if !ok {
		return nil
	}
	p, _ := v.(*principal.Principal)
	return p
}

// requireSession validates the x-user-id / x-auth-token pair and stores the
// principal. Write endpoints only — the GETs stay public for <img src>.
func requireSession(v botauth.TokenValidator, siteID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Classify reads its logger from the ctx, so without this the auth-failure
		// lines are the only ones in the service that carry no request_id.
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		userID, token := botauth.Credentials(c.Request.Header)
		p, err := botauth.Authenticate(ctx, v, userID, token)
		if err != nil {
			errhttp.Write(ctx, c, err)
			c.Abort()
			return
		}

		// A session is issued against the holder's home site, but botplatform's login
		// does not refuse a remote user, so enforce locality here as admin-service does.
		if p.SiteID != siteID {
			errhttp.Write(ctx, c, errcode.Forbidden("session belongs to another site",
				errcode.WithReason(errcode.AdminNotAuthorized)))
			c.Abort()
			return
		}

		// Carry the enriched ctx forward so the role middlewares and handlers log
		// with the same request_id rather than each rebuilding it.
		c.Request = c.Request.WithContext(ctx)
		c.Set(ctxSessionKey, &p)
		c.Set("bot_account", p.Account)
		c.Next()
	}
}

// requireBotSelfOrAdmin: the session must be the bot named in the path, or hold
// admin for provisioning. Runs after requireSession.
func requireBotSelfOrAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		p := sessionFromContext(c)
		if p == nil {
			errhttp.Write(ctx, c, errcode.Internal("avatar upload: missing principal"))
			c.Abort()
			return
		}
		if p.Account == c.Param("botName") || botauth.HasRole(p.Roles, model.UserRoleAdmin) {
			c.Next()
			return
		}

		errhttp.Write(ctx, c, errcode.Forbidden("a bot may only upload its own avatar",
			errcode.WithReason(errcode.AdminNotAuthorized)))
		c.Abort()
	}
}

// requireAdmin: a shortcode is a site-wide shared name, so emoji upload is an
// admin operation. Runs after requireSession.
func requireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		p := sessionFromContext(c)
		if p == nil {
			errhttp.Write(ctx, c, errcode.Internal("emoji upload: missing principal"))
			c.Abort()
			return
		}
		if !botauth.HasRole(p.Roles, model.UserRoleAdmin) {
			errhttp.Write(ctx, c, errcode.Forbidden("admin role required",
				errcode.WithReason(errcode.AdminNotAuthorized)))
			c.Abort()
			return
		}

		c.Next()
	}
}
