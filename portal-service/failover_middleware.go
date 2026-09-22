package main

import (
	"crypto/subtle"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// bearer extracts the token from an "Authorization: Bearer <token>" header, or
// "" when absent or a different scheme.
func bearer(c *gin.Context) string {
	if after, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// requireOps gates the failover control surface on a shared ops bearer token.
// It is the auth seam: a later per-operator login (option 3) replaces this
// middleware without touching the handlers. Compares in constant time so a
// wrong token cannot be timing-probed. The token is never logged.
func requireOps(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		got := bearer(c)
		if got == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ops token",
				errcode.WithReason(errcode.PortalFailoverUnauthorized)))
			c.Abort()
			return
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			errhttp.Write(ctx, c, errcode.Forbidden("invalid ops token",
				errcode.WithReason(errcode.PortalFailoverUnauthorized)))
			c.Abort()
			return
		}
		c.Next()
	}
}
