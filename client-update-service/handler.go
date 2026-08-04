package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Handler holds the client-update-service dependencies.
type Handler struct {
	store versionStore
	cache *blobCache
}

// NewHandler wires the handler. store backs artifact persistence; cache fronts
// downloads with a bounded TTL+size in-memory cache.
func NewHandler(store versionStore, cache *blobCache) *Handler {
	return &Handler{store: store, cache: cache}
}

// HandleHealth is the liveness probe.
func (h *Handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
