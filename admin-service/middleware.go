package main

import (
	"slices"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

const ctxPrincipal = "adminPrincipal"

// bearer extracts the token from an "Authorization: Bearer <token>" header.
// Returns "" when the header is absent or has a different scheme.
func bearer(c *gin.Context) string {
	h := c.GetHeader("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// authenticate validates the bearer session token and its siteID, writing the
// error envelope and aborting on failure — shared by requireAuth/requireAdmin.
func authenticate(c *gin.Context, sessions session.Store, siteID string) (sess *session.Session, ok bool) {
	ctx := c.Request.Context()

	tok := bearer(c)
	if tok == "" {
		errhttp.Write(ctx, c, errcode.Unauthenticated("missing session token",
			errcode.WithReason(errcode.AdminInvalidToken)))
		c.Abort()
		return nil, false
	}

	sess, err := sessions.FindByHash(ctx, sessiontoken.Hash(tok))
	if err != nil {
		errhttp.Write(ctx, c, errcode.Unauthenticated("invalid session token",
			errcode.WithReason(errcode.AdminInvalidToken)))
		c.Abort()
		return nil, false
	}

	if sess.SiteID != siteID {
		errhttp.Write(ctx, c, errcode.Forbidden("admin role required",
			errcode.WithReason(errcode.AdminNotAuthorized)))
		c.Abort()
		return nil, false
	}

	return sess, true
}

// requireAdmin is Gin middleware requiring a valid session for this site that
// also holds the admin role.
func requireAdmin(sessions session.Store, siteID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, ok := authenticate(c, sessions, siteID)
		if !ok {
			return
		}

		if !slices.Contains(sess.Roles, string(model.UserRoleAdmin)) {
			errhttp.Write(c.Request.Context(), c, errcode.Forbidden("admin role required",
				errcode.WithReason(errcode.AdminNotAuthorized)))
			c.Abort()
			return
		}

		c.Set(ctxPrincipal, *sess)
		c.Next()
	}
}

// principalFrom retrieves the Session stored by requireAuth/requireAdmin, or
// zero-value if none.
func principalFrom(c *gin.Context) session.Session {
	v, _ := c.Get(ctxPrincipal)
	s, _ := v.(session.Session)
	return s
}
