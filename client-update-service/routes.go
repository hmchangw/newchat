package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
func registerRoutes(r *gin.Engine, h *Handler) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
