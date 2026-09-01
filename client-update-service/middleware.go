package main

import (
	"crypto/subtle"
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// ctxServiceAccount holds the authenticated caller's account name, set by
// requireServiceAccount and read by accessLogMiddleware.
const ctxServiceAccount = "service_account"

// bearer extracts the token from "Authorization: Bearer <token>", mirroring
// admin-service/middleware.go so the two services agree on the header shape.
// Returns "" when absent or when the scheme differs.
func bearer(c *gin.Context) string {
	h := c.GetHeader("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// lookupAccount resolves a presented token to its account. It compares against
// every entry with no early break, so response timing cannot reveal which
// account a guessed token is closest to. (ConstantTimeCompare short-circuits on
// a length mismatch; that length leak is inherent and accepted.)
func lookupAccount(tokens map[string]string, tok string) (string, bool) {
	if tok == "" {
		return "", false
	}
	var found string
	for account, want := range tokens {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(want)) == 1 {
			found = account
		}
	}
	return found, found != ""
}

// requireServiceAccount gates a route on a configured service-account token.
// Missing, malformed and unknown credentials all produce one identical 401, so
// the endpoint cannot be used to probe the token table.
func requireServiceAccount(tokens map[string]string) gin.HandlerFunc {
	return func(c *gin.Context) {
		account, ok := lookupAccount(tokens, bearer(c))
		if !ok {
			errhttp.Write(c.Request.Context(), c,
				errcode.Unauthenticated("invalid or missing service account token"))
			c.Abort()
			return
		}
		c.Set(ctxServiceAccount, account)
		c.Next()
	}
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
			"service_account", c.GetString(ctxServiceAccount),
		)
	}
}
