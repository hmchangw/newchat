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

// TeamsMeetStartedSysData is the JSON payload stored in Message.SysMsgData for
// a teams_meet_started system message — emitted when a Microsoft Teams online
// meeting is created for a room. It is also the read-back source the meetings
// RPC uses for per-room idempotency.
type TeamsMeetStartedSysData struct {
	MeetingID string `json:"meetingId" bson:"meetingId"`
	JoinURL   string `json:"joinUrl"   bson:"joinUrl"`
}

type SendMessageRequest struct {
	ID                    string `json:"id"`
	Content               string `json:"content"`
	RequestID             string `json:"requestId"`
	ThreadParentMessageID string `json:"threadParentMessageId,omitempty"`
	QuotedParentMessageID string `json:"quotedParentMessageId,omitempty"`
	// TShow requests that a thread reply also appear in the parent room's
	// channel timeline (the "Also send to channel" option). Only meaningful
	// when ThreadParentMessageID is set — message-gatekeeper normalizes it to
	// false on non-thread sends. Maps onto Message.TShow; same wire name as
	// the persisted message field the reply echoes back.
	TShow bool `json:"tshow,omitempty"`
	// Attachments carries render-ready attachment blobs produced by upload-service
	// (one JSON object per element). message-gatekeeper validates and copies them
	// onto the canonical Message.
	Attachments [][]byte `json:"attachments,omitempty"`
	// Type is the optional client-settable message type. The only accepted value is
	// MessageTypeImportant; message-gatekeeper rejects any system type or unknown
	// value so a client can't inject a system event. Empty = a normal message.
	Type string `json:"type,omitempty"`
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

// RoomTimeHint is an optional caller-supplied walk-bounds hint (UTC millis) for a
// single room, letting history-service skip its own per-room room-times read.
type RoomTimeHint struct {
	LastMsgAt *int64 `json:"lastMsgAt,omitempty"`
	CreatedAt *int64 `json:"createdAt,omitempty"`
}

// RoomsGetRequest is the request body for the rooms.get batch RPC: the rooms whose
// last message the caller wants. The site is taken from the subject.
type RoomsGetRequest struct {
	RoomIDs []string `json:"roomIds"`
	// Hints is an optional per-room (keyed by roomID) walk-bounds hint that lets
	// history-service skip its per-room room-times read. Backward compatible:
	// old callers omit it and send only RoomIDs.
	Hints map[string]RoomTimeHint `json:"hints,omitempty"`
}

// PreviewMessage is a room's most-recent eligible message, enriched for the
// room-list preview. Content is a snippet capped at preview.MaxContentRunes
// (500 runes) — no longer the full body. Sender/mentions carry render-ready
// wire Participants (a bot sender's displayName is its app name). Purely a wire
// type: history-service's rooms.get RPC produces it and user-service's
// subscription.list embeds it. It is never stored — the room doc holds the
// split PreviewMeta + sealed body instead.
type PreviewMessage struct {
	MessageID   string                 `json:"messageId"`
	Sender      Participant            `json:"sender"`
	Content     string                 `json:"content"`
	CreatedAt   time.Time              `json:"createdAt"`
	Attachments []cassandra.Attachment `json:"attachments,omitempty"`
	Mentions    []Participant          `json:"mentions,omitempty"`
	// VisibleTo is surfaced now; its write-path (populating the column) is a separate
	// follow-up, so it's empty until that lands.
	VisibleTo string `json:"visibleTo,omitempty"`
	// TODO(#106): forwardSource — wired after the Forwarded snapshot merges.
}

// PreviewMeta is the plaintext half of a stored room preview: precisely the fields
// Cassandra leaves unencrypted, so persisting them makes no new classification. The
// user-authored half is sealed into Room.PreviewCiphertext. Storage-only — clients
// always receive a PreviewMessage.
type PreviewMeta struct {
	MessageID string        `json:"messageId"           bson:"messageId"`
	Sender    Participant   `json:"sender"              bson:"sender"`
	CreatedAt time.Time     `json:"createdAt"           bson:"createdAt"`
	Mentions  []Participant `json:"mentions,omitempty"  bson:"mentions,omitempty"`
	VisibleTo string        `json:"visibleTo,omitempty" bson:"visibleTo,omitempty"`
}

// RoomsGetResponse maps each requested roomId that has a resolvable last message to
// it. Rooms with no eligible message, or that degraded, are omitted (best-effort)
// rather than failing the whole batch.
type RoomsGetResponse struct {
	Rooms map[string]PreviewMessage `json:"rooms"`
}
