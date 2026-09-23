package main

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/health"
)

// The admission cap is attached to /api/v1/auth only, never to the engine:
// the endpoint is unauthenticated and spends bcrypt/JWKS-scale CPU before any
// credential is known to be valid, while liveness must stay answerable exactly
// when that route is shedding.
func registerRoutes(r *gin.Engine, h *AuthHandler, limit ginutil.ConcurrencyConfig, onShed func()) {
	r.POST("/api/v1/auth", limit.Middleware(onShed), h.HandleAuth)
	r.GET("/healthz", h.HandleHealth)
	r.GET("/readyz", gin.WrapF(health.ReadinessHandler(5*time.Second)))
}
