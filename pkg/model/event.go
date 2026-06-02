package model

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventCreated  EventType = "created"
	EventUpdated  EventType = "updated"
	EventDeleted  EventType = "deleted"
	EventPinned   EventType = "pinned"
	EventUnpinned EventType = "unpinned"
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
	Action       string       `json:"action"` // "added" | "removed" | "role_updated" | "mute_toggled"
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
	OutboxSubscriptionMuteToggled    OutboxEventType = "subscription_mute_toggled"
	OutboxThreadSubscriptionUpserted OutboxEventType = "thread_subscription_upserted"
	OutboxThreadRead                 OutboxEventType = "thread_read"
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

// ThreadReadEvent is the OutboxEvent.Payload for type "thread_read". The source site
// ships the authoritative NewThreadUnread+Alert; the destination applies them as-is.
type ThreadReadEvent struct {
	Account         string   `json:"account"`
	RoomID          string   `json:"roomId"`
	ThreadRoomID    string   `json:"threadRoomId"`
	ParentMessageID string   `json:"parentMessageId"`
	NewThreadUnread []string `json:"newThreadUnread"`
	Alert           bool     `json:"alert"`
	LastSeenAt      int64    `json:"lastSeenAt"`
	Timestamp       int64    `json:"timestamp"`
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
	RoomType           RoomType `json:"roomType,omitempty" bson:"roomType,omitempty"`
	Accounts           []string `json:"accounts"           bson:"accounts"`
	SiteID             string   `json:"siteId"             bson:"siteId"`
	RequesterAccount   string   `json:"requesterAccount,omitempty" bson:"requesterAccount,omitempty"`
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
	RoomEventNewMessage      RoomEventType = "new_message"
	RoomEventMessageEdited   RoomEventType = "message_edited"
	RoomEventMessageDeleted  RoomEventType = "message_deleted"
	RoomEventMessagePinned   RoomEventType = "message_pinned"
	RoomEventMessageUnpinned RoomEventType = "message_unpinned"
)

// RoomEvent is the live fan-out event for a newly created message
// (RoomEventNewMessage). Edits, deletes, pins, and unpins use the flattened
// EditRoomEvent / DeleteRoomEvent / PinRoomEvent / UnpinRoomEvent so clients
// are not handed zero-valued base fields.
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

// EditRoomEvent is the live event published when a message is edited. Fields are
// flat (no zero-valued RoomEvent base fields). For encrypted channel rooms
// NewContent is empty and EncryptedNewContent carries the ciphertext; otherwise
// NewContent holds the plaintext edit.
type EditRoomEvent struct {
	Type                RoomEventType   `json:"type" bson:"type"`
	RoomID              string          `json:"roomId" bson:"roomId"`
	SiteID              string          `json:"siteId" bson:"siteId"`
	Timestamp           int64           `json:"timestamp" bson:"timestamp"`
	MessageID           string          `json:"messageId" bson:"messageId"`
	NewContent          string          `json:"newContent,omitempty" bson:"newContent,omitempty"`
	EncryptedNewContent json.RawMessage `json:"encryptedNewContent,omitempty" bson:"encryptedNewContent,omitempty"`
	EditedBy            string          `json:"editedBy" bson:"editedBy"`
	EditedAt            time.Time       `json:"editedAt" bson:"editedAt"`
	UpdatedAt           time.Time       `json:"updatedAt" bson:"updatedAt"`
}

// DeleteRoomEvent is the live event published when a message is deleted. Fields
// are flat (no zero-valued RoomEvent base fields).
type DeleteRoomEvent struct {
	Type      RoomEventType `json:"type" bson:"type"`
	RoomID    string        `json:"roomId" bson:"roomId"`
	SiteID    string        `json:"siteId" bson:"siteId"`
	Timestamp int64         `json:"timestamp" bson:"timestamp"`
	MessageID string        `json:"messageId" bson:"messageId"`
	DeletedBy string        `json:"deletedBy" bson:"deletedBy"`
	DeletedAt time.Time     `json:"deletedAt" bson:"deletedAt"`
	UpdatedAt time.Time     `json:"updatedAt" bson:"updatedAt"`
}

// PinRoomEvent is the live event published when a message is pinned. Fields
// are flat (no zero-valued RoomEvent base fields). Mirrors the
// EditRoomEvent / DeleteRoomEvent pattern.
type PinRoomEvent struct {
	Type      RoomEventType `json:"type" bson:"type"`
	RoomID    string        `json:"roomId" bson:"roomId"`
	SiteID    string        `json:"siteId" bson:"siteId"`
	Timestamp int64         `json:"timestamp" bson:"timestamp"`
	MessageID string        `json:"messageId" bson:"messageId"`
	PinnedBy  *Participant  `json:"pinnedBy,omitempty" bson:"pinnedBy,omitempty"`
	PinnedAt  time.Time     `json:"pinnedAt" bson:"pinnedAt"`
}

// UnpinRoomEvent is the live event published when a message is unpinned.
type UnpinRoomEvent struct {
	Type       RoomEventType `json:"type" bson:"type"`
	RoomID     string        `json:"roomId" bson:"roomId"`
	SiteID     string        `json:"siteId" bson:"siteId"`
	Timestamp  int64         `json:"timestamp" bson:"timestamp"`
	MessageID  string        `json:"messageId" bson:"messageId"`
	UnpinnedBy *Participant  `json:"unpinnedBy,omitempty" bson:"unpinnedBy,omitempty"`
	UnpinnedAt time.Time     `json:"unpinnedAt" bson:"unpinnedAt"`
}

// RemovedSubscriptionRef is the minimal subscription identity carried on a
// "removed" subscription.update event — only the fields a client needs to drop
// the room from its sidebar. Used instead of an embedded full Subscription so
// the wire payload carries no zero-valued fields (roles, name, joinedAt, etc.).
type RemovedSubscriptionRef struct {
	RoomID   string           `json:"roomId" bson:"roomId"`
	RoomType RoomType         `json:"roomType" bson:"roomType"`
	U        SubscriptionUser `json:"u" bson:"u"`
}

// SubscriptionRemovedEvent is the subscription.update payload published when a
// member (individual or via org removal) loses a subscription. It mirrors
// SubscriptionUpdateEvent's envelope (userId / subscription / action /
// timestamp) but embeds a lean RemovedSubscriptionRef rather than a full
// Subscription, so removals do not ship zero-valued Subscription fields.
type SubscriptionRemovedEvent struct {
	UserID       string                 `json:"userId,omitempty" bson:"userId,omitempty"`
	Subscription RemovedSubscriptionRef `json:"subscription" bson:"subscription"`
	Action       string                 `json:"action" bson:"action"` // always "removed"
	Timestamp    int64                  `json:"timestamp" bson:"timestamp"`
}

type RoomKeyEvent struct {
	RoomID     string `json:"roomId"`
	Version    int    `json:"version"`
	PrivateKey []byte `json:"privateKey"`
	Timestamp  int64  `json:"timestamp" bson:"timestamp"`
}

// RoomKeyEnsureRequest is the payload for the room key ensure RPC.
type RoomKeyEnsureRequest struct {
	RoomID string `json:"roomId"`
}

// RoomKeyEnsureResponse is the reply from the room key ensure RPC. It confirms
// the room has a key pair in Valkey at the given Version. Key bytes are not
// returned — the caller only needs to know the key exists.
type RoomKeyEnsureResponse struct {
	RoomID  string `json:"roomId"`
	Version int    `json:"version"`
}

// RoomKeyGetRequest is the payload for the client-callable room key get RPC.
// Version is optional: when nil the server returns the current key; when set
// the server returns the historical key at that version (subject to the
// roomkeystore previous-key grace window).
type RoomKeyGetRequest struct {
	Version *int `json:"version,omitempty"`
}

// RoomKeyGetResponse is the reply from the room key get RPC. PrivateKey is
// the 32-byte room secret used directly as the AES-256-GCM key by clients.
// []byte marshals to standard base64 in JSON (same shape as RoomKeyEvent).
type RoomKeyGetResponse struct {
	RoomID     string `json:"roomId"`
	Version    int    `json:"version"`
	PrivateKey []byte `json:"privateKey"`
}

// MuteToggleResponse is the sync reply for the mute.toggle RPC.
type MuteToggleResponse struct {
	Status string `json:"status"`
	Muted  bool   `json:"muted"`
}

// SubscriptionMuteToggledEvent is the OutboxEvent.Payload for type "subscription_mute_toggled".
type SubscriptionMuteToggledEvent struct {
	Account   string `json:"account"              bson:"account"`
	RoomID    string `json:"roomId"               bson:"roomId"`
	Muted     bool   `json:"muted" bson:"muted"`
	Timestamp int64  `json:"timestamp"            bson:"timestamp"`
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
	// MessageTypeMemberRemoved is the system-message type emitted when a member is removed.
	MessageTypeMemberRemoved = "member_removed"
	// MessageTypeMemberLeft is the system-message type emitted when a member self-leaves.
	MessageTypeMemberLeft = "member_left"
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
