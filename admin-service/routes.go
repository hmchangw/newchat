package main

import "github.com/gin-gonic/gin"

// registerRoutes wires all HTTP routes onto r.
func registerRoutes(r *gin.Engine, h *Handler, store AdminStore, siteID string) {
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	admin := r.Group("/v1/admin", requireAdmin(store, siteID))
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
