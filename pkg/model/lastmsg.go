package model

import (
	"encoding/json"
	"time"
)

// LastMessagePreviewMaxRunes caps a preview body: room-list snippets never
// need full message content, and untrimmed bodies would otherwise be
// denormalized into the hot rooms collection and every delete event. Keep in
// sync with history-service's rooms.get snippet cap.
const LastMessagePreviewMaxRunes = 256

// TrimPreview bounds a message body to LastMessagePreviewMaxRunes runes,
// cutting on rune boundaries.
func TrimPreview(msg string) string {
	if len(msg) <= LastMessagePreviewMaxRunes {
		return msg // bytes ≤ cap ⇒ runes ≤ cap; no alloc on the common short case
	}
	r := []rune(msg)
	if len(r) <= LastMessagePreviewMaxRunes {
		return msg
	}
	return string(r[:LastMessagePreviewMaxRunes])
}

// LastMessagePreview is the denormalized preview of a room's newest
// non-deleted, non-system message, embedded in room snapshots and delete
// events so clients can refresh the room-list preview without a history
// fetch. CreatedAt/EditedAt are domain times of the previewed message, not
// event times.
type LastMessagePreview struct {
	MessageID     string `json:"messageId" bson:"messageId"`
	Type          string `json:"type,omitempty" bson:"type,omitempty"`
	SenderAccount string `json:"senderAccount" bson:"senderAccount"`
	SenderName    string `json:"senderName,omitempty" bson:"senderName,omitempty"`
	Msg           string `json:"msg,omitempty" bson:"msg,omitempty"`
	// EncMsg is the roomcrypto envelope of the content for encrypted channel
	// rooms — the same envelope clients decrypt for
	// EditRoomEvent.EncryptedNewContent. Exactly one of Msg/EncMsg is set
	// (both empty for content-less messages, e.g. attachment-only).
	EncMsg          json.RawMessage `json:"encMsg,omitempty" bson:"encMsg,omitempty"`
	CreatedAt       time.Time       `json:"createdAt" bson:"createdAt"`
	EditedAt        *time.Time      `json:"editedAt,omitempty" bson:"editedAt,omitempty"`
	AttachmentCount int             `json:"attachmentCount,omitempty" bson:"attachmentCount,omitempty"`
}

// LastMessagePointer identifies the room's newest surviving message of ANY
// type — system notices included, removed-parent placeholders and deleted
// rows excluded. It is the value rooms.{lastMsgId,lastMsgAt} track for room
// sorting, distinct from the non-system preview (a system message can own
// the pointer while an older user message owns the preview).
type LastMessagePointer struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
}

// LastRoomMessageRequest is the NATS request body for the last-room-message
// RPC (subject.MsgRoomLast).
//
// Before (unix ms, optional) is a caller-supplied walk ceiling: the
// denormalized rooms.lastMsgAt can lag behind coalesced create writes, so the
// delete fan-out passes the delete-event time to guarantee buffered-but-
// unflushed survivors are still inside the walk window. Zero means "trust
// the stored lastMsgAt" (pre-Before callers).
type LastRoomMessageRequest struct {
	RoomID string `json:"roomId"`
	Before int64  `json:"before,omitempty"`
}

// LastRoomMessageResponse is the reply for the last-room-message RPC.
// LastMessage is nil when the room has no remaining non-deleted, non-system
// message. Pointer is nil only when the room has no surviving message of any
// kind; it can be non-nil with a nil LastMessage when only system messages
// survive. Pointer absent with LastMessage present means the reply came from
// a pre-Pointer server (rolling deploy) — callers derive the pointer from
// the preview.
type LastRoomMessageResponse struct {
	LastMessage *LastMessagePreview `json:"lastMessage,omitempty"`
	Pointer     *LastMessagePointer `json:"pointer,omitempty"`
}
