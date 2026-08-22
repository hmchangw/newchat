package main

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
)

// httpDeps are the listener knobs registerRoutes takes from config.
type httpDeps struct {
	maxConcurrency int
	gzipMinBytes   int
	handlerTimeout time.Duration
	onShed         func()
}

// registerRoutes wires the authenticated read API. Health probes deliberately
// live on the separate HEALTH_ADDR listener, so a shed request can never fail a
// kubelet probe and restart the pod mid-burst.
func registerRoutes(r *gin.Engine, h *handler, auth authDeps, d httpDeps) {
	api := r.Group("/api/v1")
	// Limiter first: a request about to be shed should not pay for token validation.
	api.Use(ginutil.MaxConcurrency(d.maxConcurrency, ginutil.WithOnShed(d.onShed)))
	api.Use(ginutil.Gzip(d.gzipMinBytes))
	api.Use(handlerTimeout(d.handlerTimeout))
	api.Use(authMiddleware(auth))

	api.GET("/subscriptions", h.ListSubscriptions)
}

// handlerTimeout bounds a request's work. The enrichment fan-out checks the ctx
// between RPCs, so an abandoned request stops doing work rather than running on.
func handlerTimeout(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
