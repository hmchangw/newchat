package model

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventCreated EventType = "created"
	EventUpdated EventType = "updated"
	EventDeleted EventType = "deleted"
)

type MessageEvent struct {
	Event     EventType `json:"event,omitempty" bson:"event,omitempty"`
	Message   Message   `json:"message"`
	SiteID    string    `json:"siteId"`
	Timestamp int64     `json:"timestamp" bson:"timestamp"`
}

type RoomMetadataUpdateEvent struct {
	RoomID        string    `json:"roomId"`
	Name          string    `json:"name"`
	UserCount     int       `json:"userCount"`
	LastMessageAt time.Time `json:"lastMessageAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	Timestamp     int64     `json:"timestamp" bson:"timestamp"`
}

type SubscriptionUpdateEvent struct {
	UserID       string       `json:"userId"`
	Subscription Subscription `json:"subscription"`
	Action       string       `json:"action"` // "added" | "removed"
	Timestamp    int64        `json:"timestamp" bson:"timestamp"`
}

type UpdateRoleRequest struct {
	RoomID  string `json:"roomId"  bson:"roomId"`
	Account string `json:"account" bson:"account"`
	NewRole Role   `json:"newRole" bson:"newRole"`
	// Set by room-service at acceptance; stable seed for room-worker's Nats-Msg-Id.
	Timestamp int64 `json:"timestamp" bson:"timestamp"`
}

// InboxMemberEvent is the payload of an OutboxEvent{Type: "member_added" |
// "member_removed"} carried on the INBOX stream for local consumers like
// search-sync-worker. One event represents a bulk add/remove of N Accounts
// against a single room; downstream consumers fan out per-account.
//
// HistorySharedSince == nil means the entire bulk is unrestricted; non-nil
// means all Accounts in the bulk are restricted from that timestamp. The
// user-room collection routes these into restrictedRooms{}; the spotlight
// collection skips non-nil events entirely for MVP. Publishers MUST emit nil
// for unrestricted rooms — never &0 and never a non-positive timestamp — so
// the Go↔painless boundary sentinel (hss <= 0 → unrestricted) stays sound.
// JoinedAt is only meaningful on add events and omitted on removes.
type InboxMemberEvent struct {
	RoomID             string   `json:"roomId"`
	RoomName           string   `json:"roomName"`
	RoomType           RoomType `json:"roomType"`
	SiteID             string   `json:"siteId"`
	Accounts           []string `json:"accounts"`
	HistorySharedSince *int64   `json:"historySharedSince,omitempty"`
	JoinedAt           int64    `json:"joinedAt,omitempty"`
	Timestamp          int64    `json:"timestamp" bson:"timestamp"`
}

type NotificationEvent struct {
	Type      string  `json:"type"` // "new_message"
	RoomID    string  `json:"roomId"`
	Message   Message `json:"message"`
	Timestamp int64   `json:"timestamp" bson:"timestamp"`
}

// OutboxEventType is the type tag on an OutboxEvent used to route it to the
// correct handler on the destination site.
type OutboxEventType = string

const (
	OutboxMemberAdded                OutboxEventType = "member_added"
	OutboxMemberRemoved              OutboxEventType = "member_removed"
	OutboxSubscriptionRead           OutboxEventType = "subscription_read"
	OutboxThreadSubscriptionUpserted OutboxEventType = "thread_subscription_upserted"
)

// SubscriptionReadEvent is the OutboxEvent.Payload for type
// "subscription_read". Sent from a room's home site to the user's home site
// when a user marks the room as read; the destination updates its local
// subscription cache. LastSeenAt is UnixMilli (UTC) for cross-language wire
// safety; Timestamp is the publish time.
type SubscriptionReadEvent struct {
	Account    string `json:"account"    bson:"account"`
	RoomID     string `json:"roomId"     bson:"roomId"`
	LastSeenAt int64  `json:"lastSeenAt" bson:"lastSeenAt"`
	Alert      bool   `json:"alert"      bson:"alert"`
	Timestamp  int64  `json:"timestamp"  bson:"timestamp"`
}

type OutboxEvent struct {
	Type       OutboxEventType `json:"type"`
	SiteID     string          `json:"siteId"`
	DestSiteID string          `json:"destSiteId"`
	Payload    []byte          `json:"payload"` // JSON-encoded inner event
	Timestamp  int64           `json:"timestamp" bson:"timestamp"`
}

type MemberAddEvent struct {
	Type               string   `json:"type"               bson:"type"`
	RoomID             string   `json:"roomId"             bson:"roomId"`
	RoomName           string   `json:"roomName"           bson:"roomName"`
	Accounts           []string `json:"accounts"           bson:"accounts"`
	SiteID             string   `json:"siteId"             bson:"siteId"`
	JoinedAt           int64    `json:"joinedAt"           bson:"joinedAt"`
	HistorySharedSince *int64   `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`
	Timestamp          int64    `json:"timestamp"          bson:"timestamp"`
}

// Participant represents a user with display name info for client rendering.
type Participant struct {
	UserID      string `json:"userId,omitempty" bson:"userId,omitempty"`
	Account     string `json:"account" bson:"account"`
	SiteID      string `json:"siteId,omitempty" bson:"siteId,omitempty"`
	ChineseName string `json:"chineseName" bson:"chineseName"`
	EngName     string `json:"engName" bson:"engName"`
}

// ClientMessage wraps Message with enriched sender info for client consumption.
type ClientMessage struct {
	Message `json:",inline" bson:",inline"`
	Sender  *Participant `json:"sender,omitempty"`
}

type RoomEventType string

const (
	RoomEventNewMessage RoomEventType = "new_message"
)

type RoomEvent struct {
	Type      RoomEventType `json:"type"`
	RoomID    string        `json:"roomId"`
	Timestamp int64         `json:"timestamp" bson:"timestamp"`

	RoomName  string    `json:"roomName"`
	RoomType  RoomType  `json:"roomType"`
	SiteID    string    `json:"siteId"`
	UserCount int       `json:"userCount"`
	LastMsgAt time.Time `json:"lastMsgAt"`
	LastMsgID string    `json:"lastMsgId"`

	Mentions   []Participant `json:"mentions,omitempty"`
	MentionAll bool          `json:"mentionAll,omitempty"`

	HasMention bool `json:"hasMention,omitempty"`

	Message          *ClientMessage  `json:"message,omitempty"`
	EncryptedMessage json.RawMessage `json:"encryptedMessage,omitempty"`
}

type RoomKeyEvent struct {
	RoomID     string `json:"roomId"`
	Version    int    `json:"version"`
	PublicKey  []byte `json:"publicKey"`
	PrivateKey []byte `json:"privateKey"`
	Timestamp  int64  `json:"timestamp" bson:"timestamp"`
}

type MemberRemoveEvent struct {
	Type      string   `json:"type"            bson:"type"`
	RoomID    string   `json:"roomId"          bson:"roomId"`
	Accounts  []string `json:"accounts"        bson:"accounts"`
	SiteID    string   `json:"siteId"          bson:"siteId"`
	OrgID     string   `json:"orgId,omitempty" bson:"orgId,omitempty"`
	Timestamp int64    `json:"timestamp"       bson:"timestamp"`
}

// AsyncJobResult signals to the requester's client that an async room-worker job has completed.
type AsyncJobResult struct {
	RequestID string `json:"requestId"`
	Operation string `json:"operation"`
	Status    string `json:"status"`
	RoomID    string `json:"roomId,omitempty"`
	Error     string `json:"error,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

const (
	AsyncJobOpRoomCreate           = "room.create"
	AsyncJobOpRoomMemberAdd        = "room.member.add"
	AsyncJobOpRoomMemberRemove     = "room.member.remove"
	AsyncJobOpRoomMemberRemoveOrg  = "room.member.remove_org"
	AsyncJobOpRoomMemberRoleUpdate = "room.member.role_update"
)

const (
	// MessageTypeRoomCreated is the system-message type emitted on room creation (channels only).
	MessageTypeRoomCreated = "room_created"
	// MessageTypeMembersAdded is the system-message type emitted when members are added.
	MessageTypeMembersAdded = "members_added"
)

const (
	// OutboxTypeRoomCreated is the cross-site outbox event type emitted when a room is created.
	// Distinct from MessageTypeRoomCreated (system-message type) so destination sites can
	// route on event semantics without collision.
	OutboxTypeRoomCreated = "room_created"
)

const (
	// AsyncJobStatusOK indicates a successful async job result.
	AsyncJobStatusOK = "ok"
	// AsyncJobStatusError indicates a failed async job result.
	AsyncJobStatusError = "error"
)

// CreateRoomReply is the sync NATS reply returned after publishing the canonical create event.
type CreateRoomReply struct {
	Status   string `json:"status"`
	RoomID   string `json:"roomId"`
	RoomType string `json:"roomType"`
}

// CreateRoomReplyAccepted means validated + queued; persistence happens later in room-worker.
const CreateRoomReplyAccepted = "accepted"

// RoomCreatedOutbox is the cross-site payload (wrapped in OutboxEvent) when a remote member exists.
type RoomCreatedOutbox struct {
	RoomID           string   `json:"roomId"`
	RoomType         RoomType `json:"roomType"`
	RoomName         string   `json:"roomName"`
	HomeSiteID       string   `json:"homeSiteId"`
	Accounts         []string `json:"accounts"`
	RequesterAccount string   `json:"requesterAccount"`
	Timestamp        int64    `json:"timestamp"`
}
