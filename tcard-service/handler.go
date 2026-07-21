package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// templateSuffix is the required template filename suffix:
// GET /api/v1/cards/{path}@{cardVersion}.template.json.
const templateSuffix = ".template.json"

// refreshResponse is the /api/v1/cards/refresh success payload: count is the
// number of distinct (path, cardVersion) entries now in the cache.
type refreshResponse struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// CardHandler serves card template documents from the in-memory cache and
// reloads that cache from the cards collection on demand.
type CardHandler struct {
	cache *cardCache
	store CardStore
}

// NewCardHandler creates a CardHandler around the shared cache and the store
// the refresh endpoint reloads it from.
func NewCardHandler(cache *cardCache, store CardStore) *CardHandler {
	return &CardHandler{cache: cache, store: store}
}

// reqCtx carries the request id into the logging context for error classification.
func reqCtx(c *gin.Context) context.Context {
	return errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
}

// HandleRefresh reloads the whole cards collection into the cache. Safe under
// concurrency: each call swaps in a complete snapshot.
func (h *CardHandler) HandleRefresh(c *gin.Context) {
	ctx := reqCtx(c)

	n, err := h.cache.Load(ctx, h.store)
	if err != nil {
		// Infra failure — collapses to `internal` at the boundary.
		errhttp.Write(ctx, c, err)
		return
	}
	c.JSON(http.StatusOK, refreshResponse{Status: "ok", Count: n})
}

// HandleGetTemplate serves the cached card for {path}@{cardVersion} (a lock-free
// lookup, no Mongo read). The version is required: no "@" is a bad request.
func (h *CardHandler) HandleGetTemplate(c *gin.Context) {
	ctx := reqCtx(c)

	file := c.Param("file")
	spec := strings.TrimSuffix(file, templateSuffix)
	if spec == file {
		errhttp.Write(ctx, c, errcode.BadRequest("card template file must be named {path}@{cardVersion}"+templateSuffix))
		return
	}
	if spec == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("card template file name is empty"))
		return
	}
	at := strings.LastIndex(spec, "@")
	if at < 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("card template request must include a version: {path}@{cardVersion}"+templateSuffix))
		return
	}
	path, cardVersion := spec[:at], spec[at+1:]
	if path == "" || cardVersion == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("card template path and cardVersion must both be non-empty"))
		return
	}

	tmpl, ok := h.cache.Get(path, cardVersion)
	if !ok {
		errhttp.Write(ctx, c, errcode.NotFound("card template not found"))
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", tmpl)
}

// HandleHealth is the liveness probe: the process is up and serving HTTP.
func (h *CardHandler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HandleReady is the readiness probe: fails until the first successful cache load.
func (h *CardHandler) HandleReady(c *gin.Context) {
	if !h.cache.Ready() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

const (
	registerType    = "AdaptiveCard"
	registerSchema  = "http://adaptivecards.io/schemas/adaptive-card.json"
	registerVersion = "1.5"
)

// HandleRegister validates a card, inserts it, and adds it to the cache so it
// is servable at once. 400 on field/format, 409 on not-highest/duplicate.
func (h *CardHandler) HandleRegister(c *gin.Context) {
	ctx := reqCtx(c)

	var doc cardDoc
	if err := c.ShouldBindJSON(&doc); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid card JSON"))
		return
	}
	if err := validateRegister(&doc); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	// This check and the insert aren't serialized: concurrent same-path registers
	// can both insert different versions; only an exact (path, cardVersion) dupe fails. Accepted.
	versions, err := h.store.ListVersions(ctx, doc.Path)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list card versions: %w", err))
		return
	}
	if !isHighest(doc.CardVersion, versions) {
		errhttp.Write(ctx, c, errcode.Conflict("cardVersion must be the highest for this path"))
		return
	}
	if err := h.store.InsertCard(ctx, &doc); err != nil {
		// Reachable only under a concurrent same-version register; otherwise
		// isHighest already rejects an equal version.
		if errors.Is(err, ErrDuplicateCard) {
			errhttp.Write(ctx, c, errcode.Conflict("card already exists for this (path, cardVersion)"))
			return
		}
		errhttp.Write(ctx, c, err)
		return
	}

	// Add to the cache so the card is servable at once; a failure here is logged,
	// not surfaced (it is persisted and appears on the next refresh).
	if cd, ok, err := h.store.GetCard(ctx, doc.Path, doc.CardVersion); err != nil || !ok {
		slog.Warn("card registered but not cached",
			"path", doc.Path, "cardVersion", doc.CardVersion, "error", err)
	} else {
		h.cache.Add(cd)
	}
	c.JSON(http.StatusCreated, gin.H{"success": true})
}

// validateRegister runs the field/format checks: required fields, path safety,
// semver cardVersion, pinned type/schema/version, and a non-empty array body.
func validateRegister(doc *cardDoc) error {
	switch {
	case doc.Path == "":
		return errcode.BadRequest("path is required")
	case doc.CardVersion == "":
		return errcode.BadRequest("cardVersion is required")
	case doc.Type == "":
		return errcode.BadRequest("type is required")
	case doc.Schema == "":
		return errcode.BadRequest("schema is required")
	case doc.Version == "":
		return errcode.BadRequest("version is required")
	}
	if strings.Contains(doc.Path, "/") {
		return errcode.BadRequest("path must not contain '/'")
	}
	if _, ok := parseSemver(doc.CardVersion); !ok {
		return errcode.BadRequest("cardVersion must be a semantic version a.b.c")
	}
	if doc.Type != registerType {
		return errcode.BadRequest(`type must be "AdaptiveCard"`)
	}
	if doc.Schema != registerSchema {
		return errcode.BadRequest("schema must be " + registerSchema)
	}
	if doc.Version != registerVersion {
		return errcode.BadRequest(`version must be "1.5"`)
	}
	var body []json.RawMessage
	if err := json.Unmarshal(doc.Body, &body); err != nil || len(body) == 0 {
		return errcode.BadRequest("body must be a non-empty array")
	}
	return nil
}
