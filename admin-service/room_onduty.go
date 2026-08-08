package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// roomRequester issues a synchronous request/reply against room-service. Shaped
// around messages so *o11ynats.Conn satisfies it and carries headers outbound.
type roomRequester interface {
	RequestMsg(ctx context.Context, msg *nats.Msg, timeout time.Duration) (*nats.Msg, error)
}

// roomOnDutyRequest is the duty-toggle body; OnDuty is a pointer so an absent
// field is rejected while an explicit false is accepted.
type roomOnDutyRequest struct {
	OnDuty       *bool  `json:"onDuty" binding:"required"`
	OwnerAccount string `json:"ownerAccount"`
}

// setRoomOnDuty maps the boolean onto the room's restricted + externalAccess
// flags via room-service. The room is told nothing — see roomRestricted.
func (h *Handler) setRoomOnDuty(c *gin.Context) {
	// Seed the correlation id so Classify's log line for a failure carries it —
	// with no audit row, the logs are the only trace of a duty switch.
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	if h.roomRPC == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("room service client not configured"))
		return
	}

	var req roomOnDutyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	onDuty := *req.OnDuty
	// Trim before the guard, or a whitespace-only account clears it and comes back
	// from room-service as "not a member" instead of a precise 400.
	ownerAccount := strings.TrimSpace(req.OwnerAccount)

	// room-service rejects this anyway; failing here names the missing field.
	if onDuty && ownerAccount == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("ownerAccount is required when turning duty on",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	roomID := c.Param("roomId")
	owner := ""
	if onDuty {
		owner = ownerAccount
	}

	// Timestamp is left zero on purpose: room-service stamps it on acceptance and
	// overwrites whatever arrives, so setting it here would only look meaningful.
	payload, err := json.Marshal(model.RoomRestrictedRequest{
		RoomID:         roomID,
		Restricted:     onDuty,
		ExternalAccess: onDuty,
		OwnerAccount:   owner,
		Account:        principalFrom(c).Account,
	})
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("marshal room restricted request: %w", err))
		return
	}

	// NewMsg carries X-Request-ID from ctx so the correlation id survives the hop.
	msg := natsutil.NewMsg(ctx, subject.RoomRestricted(h.cfg.SiteID), payload)
	reply, err := h.roomRPC.RequestMsg(ctx, msg, h.cfg.RoomRPCTimeout)
	if err != nil {
		// room-service has no deadline of its own, so a timeout may still have
		// applied the write: 503 invites a retry of an idempotent call. Canceled
		// belongs here too — a client hang-up is not an internal fault and must
		// not log at ERROR.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) {
			errhttp.Write(ctx, c, errcode.Unavailable("room service unavailable", errcode.WithCause(err)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("room restricted rpc: %w", err))
		return
	}
	if reply == nil {
		errhttp.Write(ctx, c, fmt.Errorf("room restricted rpc returned no reply"))
		return
	}

	if remote, ok := errcode.Parse(reply.Data); ok {
		// Parse does not validate Code, so a foreign envelope collapses to internal.
		if !remote.Code.Valid() {
			errhttp.Write(ctx, c, fmt.Errorf("room restricted rpc returned unknown code %q", remote.Code))
			return
		}
		errhttp.Write(ctx, c, remote)
		return
	}

	// Parse also returns false for an empty or truncated body, which is not success.
	var status model.StatusWithRequestReply
	if err := json.Unmarshal(reply.Data, &status); err != nil || status.Status != "ok" {
		errhttp.Write(ctx, c, fmt.Errorf("room restricted rpc returned an unrecognized reply"))
		return
	}

	// No h.audit call, unlike every other mutating handler here: the operation is
	// specified as leaving no record under its own name. room-service logs the
	// change (actor, room, flags, owner) and that is the intended trail.
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
