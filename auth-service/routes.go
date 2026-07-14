package main

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/health"
)

func registerRoutes(r *gin.Engine, h *AuthHandler) {
	r.POST("/api/v1/auth", h.HandleAuth)
	r.GET("/healthz", h.HandleHealth)
	r.GET("/readyz", gin.WrapF(health.ReadinessHandler(5*time.Second)))
}
