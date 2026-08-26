package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// Upload is gated on a service-account token; download deliberately is not —
// the client fleet pulling updates holds no credential.
func registerRoutes(r *gin.Engine, h *Handler, uploadTokens map[string]string) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", requireServiceAccount(uploadTokens), h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
