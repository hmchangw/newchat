package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *handler) {
	r.GET("/healthz", h.HandleHealth)

	r.POST("/api/v1/login", h.HandleLogin)
	r.POST("/api/v1/auth/validate", h.HandleValidate)
}
