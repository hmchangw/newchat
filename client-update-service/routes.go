package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// auth guards the upload only: the download stays open because deployed desktop
// update clients hold no credential and cannot obtain one.
func registerRoutes(r *gin.Engine, h *Handler, auth gin.HandlerFunc) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", auth, h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
