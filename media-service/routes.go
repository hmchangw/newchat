package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
)

// registerRoutes wires the public reads plus the two session-gated writes.
// GETs stay public: the frontend loads them from <img src>.
func registerRoutes(r *gin.Engine, h *handler, sessions botauth.TokenValidator, siteID string) {
	r.GET("/healthz", h.HandleHealth)
	r.GET("/api/v1/avatar/room/:roomID", h.HandleRoomAvatar)
	r.GET("/api/v1/avatar/:accountName", h.HandleAccountAvatar)
	r.GET("/api/v1/drive.members", h.HandleDriveMembers)
	r.GET("/api/v1/emoji/:shortcode", h.HandleEmojiGet)

	auth := requireSession(sessions, siteID)
	r.PUT("/api/v1/avatar/bot/:botName", auth, requireBotSelfOrAdmin(), h.HandleBotUpload)
	r.PUT("/api/v1/emoji/:shortcode", auth, requireAdmin(), h.HandleEmojiUpload)
}
