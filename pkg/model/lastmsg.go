package model

import (
	"encoding/json"
	"time"
)

// LastMessagePreviewMaxRunes caps a preview body — untrimmed snippets would be denormalized
// into the hot rooms collection and every delete event. Keep in sync with history-service's rooms.get cap.
const LastMessagePreviewMaxRunes = 256

// TrimPreview bounds a message body to LastMessagePreviewMaxRunes runes, on rune boundaries.
func TrimPreview(msg string) string {
	if len(msg) <= LastMessagePreviewMaxRunes {
		return msg // bytes ≤ cap ⇒ runes ≤ cap; no alloc on the common short case
	}
	// Byte length exceeds the cap but the rune count may not (multibyte). Walk runes and cut at
	// the byte offset of the (cap)-th rune — bounds the work to the cap and allocates only the
	// result, instead of materialising the whole body as []rune on the fan-out hot path.
	count := 0
	for i := range msg {
		if count == LastMessagePreviewMaxRunes {
			return msg[:i]
		}
		count++
	}
	return msg // fewer than cap runes despite the byte length
}

// LastMessagePreview is the denormalized preview of a room's newest non-deleted, non-system
// message (embedded so clients refresh without a history fetch). CreatedAt/EditedAt are domain times, not event times.
type LastMessagePreview struct {
	MessageID     string `json:"messageId" bson:"messageId"`
	Type          string `json:"type,omitempty" bson:"type,omitempty"`
	SenderAccount string `json:"senderAccount" bson:"senderAccount"`
	SenderName    string `json:"senderName,omitempty" bson:"senderName,omitempty"`
	Msg           string `json:"msg,omitempty" bson:"msg,omitempty"`
	// EncMsg is the roomcrypto envelope for encrypted channel rooms (same envelope clients decrypt
	// for EditRoomEvent.EncryptedNewContent). At most one of Msg/EncMsg is set (both empty for content-less/attachment-only).
	EncMsg          json.RawMessage `json:"encMsg,omitempty" bson:"encMsg,omitempty"`
	CreatedAt       time.Time       `json:"createdAt" bson:"createdAt"`
	EditedAt        *time.Time      `json:"editedAt,omitempty" bson:"editedAt,omitempty"`
	AttachmentCount int             `json:"attachmentCount,omitempty" bson:"attachmentCount,omitempty"`
}

// LastMessagePointer is the room's newest surviving message of ANY type (system included;
// deleted/removed-parent excluded) that rooms.{lastMsgId,lastMsgAt} track for sorting — distinct from the non-system preview.
type LastMessagePointer struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
}

// LastRoomMessageRequest is the NATS request body for the last-room-message RPC (subject.MsgRoomLast).
// Before (unix ms) widens the walk ceiling past a coalescer-lagged rooms.lastMsgAt so unflushed survivors stay in-window; zero trusts the stored lastMsgAt.
type LastRoomMessageRequest struct {
	RoomID string `json:"roomId"`
	Before int64  `json:"before,omitempty"`
}

// LastRoomMessageResponse is the reply for the last-room-message RPC; all answers are bounded by the server's lookback (row budget, bucket cap, history floor).
// Nil LastMessage = no non-system survivor found; nil Pointer = no survivor of any kind (callers self-heal via next event/room-list); Pointer without LastMessage = only system messages survive; LastMessage without Pointer = pre-Pointer server, derive pointer from the preview.
type LastRoomMessageResponse struct {
	LastMessage *LastMessagePreview `json:"lastMessage,omitempty"`
	Pointer     *LastMessagePointer `json:"pointer,omitempty"`
}
