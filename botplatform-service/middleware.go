package main

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

// CORS is provided by ginutil.CORS(), wired in main.go alongside this
// middleware — /api/v1/login and /api/v1/password/change are now
// browser-facing via the gateway/portal.

// accessLogMiddleware emits one structured line per request. Outcomes set by
// the login / validate handlers via c.Set are surfaced for ops triage.
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
		)
	}
}
