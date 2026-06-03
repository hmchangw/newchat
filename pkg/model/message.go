package model

import (
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

type Message struct {
	ID          string `json:"id"                                     bson:"_id"`
	RoomID      string `json:"roomId"                                 bson:"roomId"`
	UserID      string `json:"userId"                                 bson:"userId"`
	UserAccount string `json:"userAccount"                            bson:"userAccount"`
	// UserDisplayName is the render-ready sender name, composed once at canonical-message
	// write time by message-gatekeeper via pkg/displayfmt.CombineWithFallback(engName,
	// chineseName, account) — the same helper used by room-worker/sysmsg.go and
	// pkg/model/cassandra/reactions.go so display formatting stays uniform system-wide.
	// Downstream consumers (notification-worker, future search-sync-worker) read this
	// verbatim; omitempty keeps pre-rollout canonical messages decoding cleanly (consumers
	// fall back to UserAccount when the field is empty).
	UserDisplayName              string                         `json:"userDisplayName,omitempty"              bson:"userDisplayName,omitempty"`
	Content                      string                         `json:"content"                                bson:"content"`
	Attachments                  [][]byte                       `json:"attachments,omitempty"                  bson:"attachments,omitempty"`
	Card                         *cassandra.Card                `json:"card,omitempty"                         bson:"card,omitempty"`
	CardAction                   *cassandra.CardAction          `json:"cardAction,omitempty"                   bson:"cardAction,omitempty"`
	File                         *cassandra.File                `json:"file,omitempty"                         bson:"file,omitempty"`
	Mentions                     []Participant                  `json:"mentions,omitempty"                     bson:"mentions,omitempty"`
	CreatedAt                    time.Time                      `json:"createdAt"                              bson:"createdAt"`
	EditedAt                     *time.Time                     `json:"editedAt,omitempty"                     bson:"editedAt,omitempty"`
	UpdatedAt                    *time.Time                     `json:"updatedAt,omitempty"                    bson:"updatedAt,omitempty"`
	ThreadParentMessageID        string                         `json:"threadParentMessageId,omitempty"        bson:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *time.Time                     `json:"threadParentMessageCreatedAt,omitempty" bson:"threadParentMessageCreatedAt,omitempty"`
	TShow                        bool                           `json:"tshow,omitempty"                        bson:"tshow,omitempty"`
	Type                         string                         `json:"type,omitempty"                         bson:"type,omitempty"`
	SysMsgData                   []byte                         `json:"sysMsgData,omitempty"                   bson:"sysMsgData,omitempty"`
	QuotedParentMessage          *cassandra.QuotedParentMessage `json:"quotedParentMessage,omitempty"          bson:"quotedParentMessage,omitempty"`
	PinnedAt                     *time.Time                     `json:"pinnedAt,omitempty"                     bson:"pinnedAt,omitempty"`
	PinnedBy                     *Participant                   `json:"pinnedBy,omitempty"                     bson:"pinnedBy,omitempty"`
}

// RoomRenamedSysData is the JSON payload stored in Message.SysMsgData
// for a room_renamed system message.
type RoomRenamedSysData struct {
	NewName   string `json:"newName"   bson:"newName"`
	ByAccount string `json:"byAccount" bson:"byAccount"`
}

// RoomRestrictedSysData is the JSON payload stored in Message.SysMsgData
// for a room_restricted system message — emitted when room.restricted or
// room.externalAccess flip.
type RoomRestrictedSysData struct {
	Restricted     bool   `json:"restricted"             bson:"restricted"`
	ExternalAccess bool   `json:"externalAccess"         bson:"externalAccess"`
	ByAccount      string `json:"byAccount"              bson:"byAccount"`
	OwnerAccount   string `json:"ownerAccount,omitempty" bson:"ownerAccount,omitempty"`
}

type SendMessageRequest struct {
	ID                           string `json:"id"`
	Content                      string `json:"content"`
	RequestID                    string `json:"requestId"`
	ThreadParentMessageID        string `json:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *int64 `json:"threadParentMessageCreatedAt,omitempty"`
	QuotedParentMessageID        string `json:"quotedParentMessageId,omitempty"`
}

// SenderDisplayName returns the canonical render-ready name for the message's
// sender: UserDisplayName when populated (the message-gatekeeper-composed value
// described on the field), UserAccount otherwise. The fallback handles legacy
// in-flight canonical messages that predate UserDisplayName.
func (m *Message) SenderDisplayName() string {
	if m.UserDisplayName != "" {
		return m.UserDisplayName
	}
	return m.UserAccount
}
