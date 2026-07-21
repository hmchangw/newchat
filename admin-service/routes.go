package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/session"
)

// registerRoutes wires all HTTP routes onto r.
func registerRoutes(r *gin.Engine, h *Handler, sessions session.Store, siteID string) {
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	r.POST("/v1/login", h.handleLogin)
	r.POST("/v1/password/change", requireAdmin(sessions, siteID), h.handleChangePassword)

	admin := r.Group("/v1/admin", requireAdmin(sessions, siteID))
	admin.GET("/users", h.listUsers)
	admin.POST("/users", h.createUser)
	admin.GET("/users/:account", h.getUser)
	admin.PATCH("/users/:account", h.updateUser)
	admin.POST("/users/:account/password", h.setPassword)
	admin.GET("/sessions", h.listSessions)
	admin.DELETE("/sessions", h.revokeAllSessions)
	admin.DELETE("/sessions/:sessionId", h.revokeSession)
	admin.GET("/audit", h.listAudit)
}
