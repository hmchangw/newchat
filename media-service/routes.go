package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *handler) {
	r.GET("/healthz", h.HandleHealth)
	r.GET("/api/v1/avatar/room/:roomID", h.HandleRoomAvatar)
	r.GET("/api/v1/avatar/:accountName", h.HandleAccountAvatar)
	r.PUT("/api/v1/avatar/bot/:botName", h.HandleBotUpload) // no auth (§7a.4)
	r.GET("/api/v1/emoji/:shortcode", h.HandleEmojiGet)
	r.PUT("/api/v1/emoji/:shortcode", h.HandleEmojiUpload) // no auth; ?uploader= is audit-only
}
