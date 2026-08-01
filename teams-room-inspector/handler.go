package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// maxChatIDsPerRequest bounds one verify call. The caller batches at 200 by
// default; the cap leaves headroom while keeping the $in lists sane.
const maxChatIDsPerRequest = 500

// Handler serves the read-only verification endpoint for this site.
type Handler struct {
	store  RoomStore
	siteID string
}

func NewHandler(store RoomStore, siteID string) *Handler {
	return &Handler{store: store, siteID: siteID}
}

// HandleHealth is the liveness probe.
func (h *Handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HandleVerify reports, per requested Teams chat id, whether this site holds
// the room and how many subscriptions point at it. Room ids are derived with
// the same idgen.DeterministicID room-worker used to create them, so the caller
// never has to know the mapping.
func (h *Handler) HandleVerify(c *gin.Context) {
	ctx := c.Request.Context()

	var req model.TeamsRoomVerifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("decode verify request"))
		return
	}
	if len(req.ChatIDs) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("chatIds must not be empty"))
		return
	}
	if len(req.ChatIDs) > maxChatIDsPerRequest {
		errhttp.Write(ctx, c, errcode.BadRequest(
			fmt.Sprintf("chatIds exceeds the per-request limit of %d", maxChatIDsPerRequest)))
		return
	}

	roomIDs := make([]string, 0, len(req.ChatIDs))
	for _, chatID := range req.ChatIDs {
		roomIDs = append(roomIDs, idgen.DeterministicID([]byte(chatID)))
	}

	states, err := h.store.RoomStates(ctx, roomIDs)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("read room states: %w", err))
		return
	}

	resp := model.TeamsRoomVerifyResponse{
		SiteID:         h.siteID,
		RequestedCount: len(req.ChatIDs),
		Chats:          make([]model.TeamsRoomVerifyResult, 0, len(req.ChatIDs)),
	}
	for i, chatID := range req.ChatIDs {
		roomID := roomIDs[i]
		st := states[roomID] // zero value when absent: no room, no subscriptions
		if st.Exists {
			resp.FoundCount++
		}
		resp.Chats = append(resp.Chats, model.TeamsRoomVerifyResult{
			ChatID:            chatID,
			RoomID:            roomID,
			RoomExists:        st.Exists,
			SubscriptionCount: st.SubscriptionCount,
			RoomUserCount:     st.UserCount,
		})
	}
	c.JSON(http.StatusOK, resp)
}
