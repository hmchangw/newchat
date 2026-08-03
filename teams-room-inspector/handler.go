package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// maxChatIDsPerRequest bounds one verify call. Shared with the caller, whose
// config validation refuses a batch size above it.
const maxChatIDsPerRequest = model.TeamsRoomVerifyMaxChatIDs

// verifyRequestBodyMaxBytes caps the request body read by ShouldBindJSON,
// which otherwise buffers the whole body before the maxChatIDsPerRequest
// check runs. The endpoint is unauthenticated (cluster-internal by design),
// so this is the only guard against an unbounded body. Sized generously for
// maxChatIDsPerRequest Graph chat ids (each well under 256 bytes as a quoted
// JSON string) plus envelope overhead.
const verifyRequestBodyMaxBytes = maxChatIDsPerRequest*256 + 4*1024

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
// the room and how many subscriptions point at it. Room ids come from the same
// teamsmigrate.RoomIDFromChatID room-worker used to create them, so the caller
// never has to know the mapping.
func (h *Handler) HandleVerify(c *gin.Context) {
	ctx := c.Request.Context()

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, verifyRequestBodyMaxBytes)
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
		roomIDs = append(roomIDs, teamsmigrate.RoomIDFromChatID(chatID))
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
