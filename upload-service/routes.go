package main

import "github.com/gin-gonic/gin"

// registerRoutes wires health plus the authenticated /api/v1 group.
func registerRoutes(r *gin.Engine, h *Handler, d authDeps) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.Use(authMiddleware(d))
	api.POST("/file/setCookie", h.HandleSetCookie)
	api.POST("/file/rooms/:roomId/upload/images", h.HandleUploadImages)
	api.POST("/file/rooms/:roomId/upload/file", h.HandleUploadFile)
	api.GET("/file/rooms/:roomId/file/:fileId", h.HandleDownloadFile)
	api.GET("/file-upload/:fileId/:fileName", h.HandleDownloadMinioS3File)

	// v3 serves the backward-compatible protected-image download for legacy message data from a separate (legacy) Drive backend.
	apiV3 := r.Group("/api/v3")
	apiV3.Use(authMiddleware(d))
	apiV3.GET("/rooms/:roomId/protected-image/:fileId", h.HandleDownloadProtectedImageV3)
}
