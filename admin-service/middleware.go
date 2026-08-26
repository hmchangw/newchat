package main

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

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

// extendDeadlines pushes this request's read and write deadlines out to d.
//
// admin-service's server-wide ReadTimeout (15s) and WriteTimeout
// (httpWriteTimeout, 40s) are sized for the cross-site permission fanout — see
// config.go and applyBaseMiddleware — and must not be widened for every route
// just because one route needs minutes. An artifact upload extends its own
// instead, leaving every other route's behavior untouched.
//
// A ResponseWriter that cannot take deadlines (httptest's recorder in unit
// tests) reports http.ErrNotSupported; the handler still runs, it simply keeps
// the server-wide timeouts.
func extendDeadlines(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := http.NewResponseController(c.Writer)
		until := time.Now().Add(d)
		if err := rc.SetReadDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
			slog.WarnContext(c.Request.Context(), "extend read deadline failed", "error", err)
		}
		if err := rc.SetWriteDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
			slog.WarnContext(c.Request.Context(), "extend write deadline failed", "error", err)
		}
		c.Next()
	}
}
