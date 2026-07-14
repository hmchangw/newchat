package main

import (
	"slices"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
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
func authenticate(c *gin.Context, store AdminStore, siteID string) (sess *Session, ok bool) {
	ctx := c.Request.Context()

	tok := bearer(c)
	if tok == "" {
		errhttp.Write(ctx, c, errcode.Unauthenticated("missing session token",
			errcode.WithReason(errcode.AdminInvalidToken)))
		c.Abort()
		return nil, false
	}

	sess, err := store.FindSessionByHash(ctx, sessiontoken.Hash(tok))
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
func requireAdmin(store AdminStore, siteID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, ok := authenticate(c, store, siteID)
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
func principalFrom(c *gin.Context) Session {
	v, _ := c.Get(ctxPrincipal)
	s, _ := v.(Session)
	return s
}
