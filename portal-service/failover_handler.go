package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// FailoverHandler serves the operator control surface over the internal
// listener. It reads/writes FailoverState directly (operators need fresh state,
// not a cached view). now is overridable in tests.
type FailoverHandler struct {
	store FailoverStore
	now   func() time.Time
}

func NewFailoverHandler(store FailoverStore) *FailoverHandler {
	return &FailoverHandler{store: store, now: time.Now}
}

// failoverActionRequest is the POST body: the operator-requested transition,
// who requested it, and why (both required for the audit trail).
type failoverActionRequest struct {
	Action   FailoverAction `json:"action"`
	Operator string         `json:"operator"`
	Reason   string         `json:"reason"`
}

// failoverStateResponse is the wire shape, with the derived servingTarget added.
type failoverStateResponse struct {
	SiteID        string         `json:"siteId"`
	Status        FailoverStatus `json:"status"`
	ServingTarget ServingTarget  `json:"servingTarget"`
	Reason        string         `json:"reason"`
	Operator      string         `json:"operator"`
	Since         int64          `json:"since"`
	Version       int64          `json:"version"`
	Timestamp     int64          `json:"timestamp"`
}

func toResponse(s *FailoverState) failoverStateResponse {
	return failoverStateResponse{
		SiteID:        s.SiteID,
		Status:        s.Status,
		ServingTarget: s.ServingTarget(),
		Reason:        s.Reason,
		Operator:      s.Operator,
		Since:         s.Since,
		Version:       s.Version,
		Timestamp:     s.Timestamp,
	}
}

// List returns every site's failover state.
func (h *FailoverHandler) List(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	states, err := h.store.List(ctx)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list failover states: %w", err))
		return
	}
	out := make([]failoverStateResponse, 0, len(states))
	for i := range states {
		out = append(out, toResponse(&states[i]))
	}
	c.JSON(http.StatusOK, out)
}

// Get returns one site's failover state (a healthy default for an unknown site).
func (h *FailoverHandler) Get(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	siteID := c.Param("siteId")
	st, err := h.store.Get(ctx, siteID)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get failover state: %w", err))
		return
	}
	c.JSON(http.StatusOK, toResponse(&st))
}

// Post applies an operator transition to a site's failover state.
func (h *FailoverHandler) Post(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	siteID := c.Param("siteId")

	var req failoverActionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	if req.Operator == "" || req.Reason == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("operator and reason are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	if !isKnownAction(req.Action) {
		errhttp.Write(ctx, c, errcode.BadRequest(fmt.Sprintf("unknown action %q", req.Action)))
		return
	}

	cur, err := h.store.Get(ctx, siteID)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get failover state: %w", err))
		return
	}

	nowMs := h.now().UTC().UnixMilli()
	next, err := applyAction(&cur, req.Action, req.Operator, req.Reason, nowMs)
	if err != nil {
		errhttp.Write(ctx, c, errcode.Conflict(
			fmt.Sprintf("action %q not allowed from status %q", req.Action, cur.Status),
			errcode.WithReason(errcode.PortalFailoverIllegalTransition)))
		return
	}

	if err := h.store.Transition(ctx, &next); err != nil {
		if errors.Is(err, errFailoverVersionConflict) {
			errhttp.Write(ctx, c, errcode.Conflict("failover state changed concurrently, retry",
				errcode.WithReason(errcode.PortalFailoverVersionConflict)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("transition failover state: %w", err))
		return
	}

	slog.InfoContext(ctx, "failover transition",
		"siteId", siteID, "from", cur.Status, "to", next.Status,
		"operator", req.Operator, "reason", req.Reason, "version", next.Version)
	c.JSON(http.StatusOK, toResponse(&next))
}

// registerFailoverRoutes mounts the control surface behind the ops-token gate.
func registerFailoverRoutes(r *gin.Engine, h *FailoverHandler, opsToken string) {
	grp := r.Group("/internal/v1/failover", requireOps(opsToken))
	grp.GET("", h.List)
	grp.GET("/:siteId", h.Get)
	grp.POST("/:siteId", h.Post)
}
