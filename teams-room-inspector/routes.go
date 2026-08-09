package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the inspector's two endpoints. The verify endpoint is
// cluster-internal (no auth) — network policy is the boundary.
func registerRoutes(r *gin.Engine, h *Handler) {
	r.GET("/healthz", h.HandleHealth)
	r.POST("/internal/teams/rooms/verify", h.HandleVerify)
}
