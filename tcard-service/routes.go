package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *CardHandler) {
	// Refresh is POST-only (mutates state, called by services not browsers);
	// the static /refresh route takes priority over the :file param route.
	r.POST("/api/v1/cards/register", h.HandleRegister)
	r.POST("/api/v1/cards/refresh", h.HandleRefresh)
	r.GET("/api/v1/cards/:file", h.HandleGetTemplate)
	r.GET("/healthz", h.HandleHealth)
	r.GET("/readyz", h.HandleReady)
}
