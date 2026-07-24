package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *CardHandler) {
	// Refresh/register are POST-only (separate method tree, no conflict with the
	// GET wildcard). The wildcard matches slash-containing card paths.
	r.POST("/api/v1/cards/register", h.HandleRegister)
	r.POST("/api/v1/cards/refresh", h.HandleRefresh)
	r.GET("/api/v1/cards/*file", h.HandleGetTemplate)
	r.GET("/healthz", h.HandleHealth)
	r.GET("/readyz", h.HandleReady)
}
