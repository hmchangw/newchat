package model

import (
	"encoding/json"
	"time"
)

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

// LastRoomMessageRequest is the NATS request body for the last-room-message
// RPC (subject.MsgRoomLast).
type LastRoomMessageRequest struct {
	RoomID string `json:"roomId"`
}

// LastRoomMessageResponse is the reply for the last-room-message RPC.
// LastMessage is nil when the room has no remaining non-deleted,
// non-system message.
type LastRoomMessageResponse struct {
	LastMessage *LastMessagePreview `json:"lastMessage,omitempty"`
}
