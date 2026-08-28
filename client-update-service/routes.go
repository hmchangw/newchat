package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// Upload is gated on a service-account token; download deliberately is not —
// the client fleet pulling updates holds no credential.
func registerRoutes(r *gin.Engine, h *Handler, uploadTokens map[string]string, maxUploadBytes int64) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", requireServiceAccount(uploadTokens), limitUploadBody(maxUploadBytes), h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}

// limitUploadBody caps one upload's request body. It runs AFTER the credential
// check so an unauthenticated caller cannot spend the cap, and BEFORE the
// handler because c.FormFile spools the whole body to disk before any of the
// handler's own checks are consulted. The error surfaces from that first read,
// which is why HandleUpload classifies it rather than this middleware.
func limitUploadBody(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}
