package main

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		inbound := c.GetHeader(natsutil.RequestIDHeader)
		id, replaced := idgen.ResolveRequestID(inbound)
		c.Set("request_id", id)
		c.Request = c.Request.WithContext(natsutil.WithRequestID(c.Request.Context(), id))
		c.Header(natsutil.RequestIDHeader, id)
		if replaced {
			slog.WarnContext(c.Request.Context(), "minted request_id (inbound invalid)", "inbound", inbound, "path", c.Request.URL.Path)
		}
		c.Next()
	}
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")
		c.Header("Access-Control-Max-Age", "300")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// accessLogMiddleware logs one structured line per request, including the typed
// avatar outcome set by the read handlers (kind + outcome).
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
			"media_kind", c.GetString("media_kind"),
			"media_outcome", c.GetString("media_outcome"),
		)
	}
}
