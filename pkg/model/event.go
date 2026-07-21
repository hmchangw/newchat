package model

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventCreated          EventType = "created"
	EventUpdated          EventType = "updated"
	EventDeleted          EventType = "deleted"
	EventPinned           EventType = "pinned"
	EventUnpinned         EventType = "unpinned"
	EventReacted          EventType = "reacted"
	EventThreadReplyAdded EventType = "thread_reply_added"
)

// UserStatusUpdated is the cross-site inbox event user-service publishes on
// status.set; the remote inbox-worker applies it. Timestamp is the event-level
// time set at publish via time.Now().UTC().UnixMilli().
type UserStatusUpdated struct {
	Account      string `json:"account"                bson:"account"`
	StatusText   string `json:"statusText"             bson:"statusText"`
	StatusIsShow *bool  `json:"statusIsShow,omitempty" bson:"statusIsShow,omitempty"`
	Timestamp    int64  `json:"timestamp"              bson:"timestamp"`
}

type MessageEvent struct {
	Event   EventType `json:"event,omitempty" bson:"event,omitempty"`
	Message Message   `json:"message"`
	SiteID  string    `json:"siteId"`
	// ReactionDelta is set only when Event == EventReacted.
	ReactionDelta *ReactionDelta `json:"reactionDelta,omitempty" bson:"reactionDelta,omitempty"`
	Timestamp     int64          `json:"timestamp"               bson:"timestamp"`
	// NewTCount is the authoritative tcount of the parent message after a thread
	// reply is added (EventThreadReplyAdded) or deleted (EventDeleted with
	// ThreadParentMessageID set). Nil for all other event types.
	// bson tag omits omitempty — zero is a valid count when the last reply is deleted.
	NewTCount *int `json:"newTcount,omitempty" bson:"newTcount"`
	// NewThreadLastMsgAt is the timestamp of the most recent surviving thread reply
	// after this operation (nil when no replies remain).
	NewThreadLastMsgAt *time.Time `json:"newThreadLastMsgAt,omitempty" bson:"newThreadLastMsgAt,omitempty"`
	// QuotedParentUnverified marks Message.QuotedParentMessage as the untrusted
	// degraded placeholder the gatekeeper builds on a transient history outage:
	// message-worker re-projects the authoritative snapshot from Cassandra and
	// clears the flag, or drops the quote when the parent can't be confirmed (not
	// found, or in a different room/thread). Envelope-only (never
	// persisted, never reaches clients); always false on the happy path. The
	// bson:"-" tag enforces the never-persisted contract — MessageEvent carries
	// bson tags on its other fields, so an untagged field would round-trip.
	QuotedParentUnverified bool `json:"quotedParentUnverified,omitempty" bson:"-"`
	// ThreadParentSenderAccount is the thread parent's author, resolved best-effort
	// by the gatekeeper (empty on soft-fail or edit/delete events). Envelope-only
	// like QuotedParentUnverified; lets broadcast/notification workers skip their
	// own parent fetch, falling back when absent.
	ThreadParentSenderAccount string `json:"threadParentSenderAccount,omitempty" bson:"-"`
}

// ReactionAction is the toggle direction on ReactionDelta.Action; defined
// type (not alias) so constants give compile-time safety vs raw strings.
type ReactionAction string

const (
	ReactionActionAdded   ReactionAction = "added"
	ReactionActionRemoved ReactionAction = "removed"
)

// ReactionDelta is the per-toggle reaction payload. Actor produced the toggle;
// the reacted-to message's author is on the enclosing MessageEvent.Message.
type ReactionDelta struct {
	Shortcode string         `json:"shortcode" bson:"shortcode"`
	Action    ReactionAction `json:"action"    bson:"action"`
	Actor     Participant    `json:"actor"     bson:"actor"`
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
	Action       string       `json:"action"` // "added" | "removed" | "role_updated" | "mute_toggled" | "favorite_toggled" | "read"
	RoomName     string       `json:"roomName,omitempty"`
	Timestamp    int64        `json:"timestamp" bson:"timestamp"`
}

// CanonicalMemberEventMuted is the only event type currently published on this stream.
const CanonicalMemberEventMuted = "muted"

// CanonicalMemberEvent is the room-scoped post-mutation event for roomsubcache invalidation (mute-only today).
type CanonicalMemberEvent struct {
	Type      string `json:"type"`
	RoomID    string `json:"roomId"`
	Account   string `json:"account"`
	Muted     bool   `json:"muted"` // post-toggle state; false is a valid (unmuted) value, so no omitempty.
	Timestamp int64  `json:"timestamp"`
}

type UpdateRoleRequest struct {
	RoomID  string `json:"roomId"  bson:"roomId"`
	Account string `json:"account" bson:"account"`
	NewRole Role   `json:"newRole" bson:"newRole"`
	// Set by room-service at acceptance; stable seed for room-worker's Nats-Msg-Id.
	Timestamp int64 `json:"timestamp" bson:"timestamp"`
}

// InboxMemberEvent is the payload of an InboxEvent{Type: "member_added" |
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

// NotificationEvent is the per-user reaction notification published on
// chat.user.{account}.notification when someone reacts to a message.
// Distinct from PushNotificationEvent — push is the batched mobile pipeline;
// this is the legacy single-user envelope the FE already listens on.
type NotificationEvent struct {
	Type          string         `json:"type"` // "reaction"
	RoomID        string         `json:"roomId"`
	RoomType      RoomType       `json:"roomType"`
	Message       Message        `json:"message"`
	ReactionDelta *ReactionDelta `json:"reactionDelta,omitempty" bson:"reactionDelta,omitempty"`
	Timestamp     int64          `json:"timestamp"               bson:"timestamp"`
}

// InboxEventType is the type tag on an InboxEvent used to route it to the
// correct handler on the destination site.
type InboxEventType = string

const (
	InboxMemberAdded                 InboxEventType = "member_added"
	InboxMemberRemoved               InboxEventType = "member_removed"
	InboxRoleUpdated                 InboxEventType = "role_updated"
	InboxSubscriptionRead            InboxEventType = "subscription_read"
	InboxSubscriptionMuteToggled     InboxEventType = "subscription_mute_toggled"
	InboxSubscriptionFavoriteToggled InboxEventType = "subscription_favorite_toggled"
	InboxThreadSubscriptionUpserted  InboxEventType = "thread_subscription_upserted"
	InboxThreadRead                  InboxEventType = "thread_read"
	InboxThreadReadAll               InboxEventType = "thread_read_all"
	InboxRoomRenamed                 InboxEventType = "room_renamed"
	InboxRoomRestricted              InboxEventType = "room_restricted"
	InboxUserStatusUpdated           InboxEventType = "user_status_updated"
	InboxUserSettingsUpdated         InboxEventType = "user_settings_updated"
)

// UserSettingsUpdated is the cross-site inbox event user-service publishes on
// settings.set; the remote inbox-worker applies it so the local notification
// worker can read the user's notification preferences. Settings is the full
// post-update sub-document — the receiver replaces, never merges.
type UserSettingsUpdated struct {
	Account   string       `json:"account"   bson:"account"`
	Settings  UserSettings `json:"settings"  bson:"settings"`
	Timestamp int64        `json:"timestamp" bson:"timestamp"`
}

// SubscriptionReadEvent is the InboxEvent.Payload for type
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

// ThreadReadEvent is the InboxEvent.Payload for type "thread_read". The source site
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

// ThreadReadAllEvent is the InboxEvent.Payload for type "thread_read_all" — the
// federated "mark all threads read" for a user. One event carries the whole
// bulk dismiss (no per-thread list): the destination inbox-worker advances every
// one of the account's thread subscriptions to LastSeenAt under a high-water-mark
// guard (clearing hasMention) and clears every subscription's threadUnread/alert
// on the user's home replica.
type ThreadReadAllEvent struct {
	Account    string `json:"account"`
	LastSeenAt int64  `json:"lastSeenAt"`
	Timestamp  int64  `json:"timestamp"`
}

type InboxEvent struct {
	Type       InboxEventType `json:"type"`
	SiteID     string         `json:"siteId"`
	DestSiteID string         `json:"destSiteId"`
	Payload    []byte         `json:"payload"` // JSON-encoded inner event
	Timestamp  int64          `json:"timestamp" bson:"timestamp"`
}

// OutboxEvent is the federation relay published by room-service and
// room-worker onto the OUTBOX stream
// (chat.outbox.{originSiteID}.{destSiteID}.{eventType}); outbox-worker
// forwards Envelope to the destination site's INBOX with at-least-once retry.
// Destination and event type ride the subject (so a per-destination consumer can
// filter/pause on a single peer); the body carries only the pre-marshaled
// InboxEvent (byte-identical to a direct publish) and the forward's Nats-Msg-Id.
// Envelope is json.RawMessage, not []byte, so the already-JSON envelope embeds
// verbatim instead of being base64-encoded a second time into a durable,
// replicated stream body.
type OutboxEvent struct {
	RoomID    string          `json:"roomId"    bson:"roomId"`
	Envelope  json.RawMessage `json:"envelope"  bson:"envelope"`
	DedupID   string          `json:"dedupId"   bson:"dedupId"`
	Timestamp int64           `json:"timestamp" bson:"timestamp"`
}

type MemberAddEvent struct {
	Type     string   `json:"type"               bson:"type"`
	RoomID   string   `json:"roomId"             bson:"roomId"`
	RoomName string   `json:"roomName"           bson:"roomName"`
	RoomType RoomType `json:"roomType,omitempty" bson:"roomType,omitempty"`
	// Accounts is stripped from the room-scoped (frontend) copy — the client renders from Members —
	// but kept on the INBOX/search and cross-site copies, which carry no Members and subscribe/index by account.
	Accounts           []string `json:"accounts,omitempty" bson:"accounts,omitempty"`
	SiteID             string   `json:"siteId"             bson:"siteId"`
	RequesterAccount   string   `json:"requesterAccount,omitempty" bson:"requesterAccount,omitempty"`
	JoinedAt           int64    `json:"joinedAt"           bson:"joinedAt"`
	HistorySharedSince *int64   `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`
	// Members carries the member.list (enrich=true) display entries; org-expanded accounts ride Accounts only.
	// Room-scoped event only — INBOX copies omit it (remote sites re-resolve display data).
	Members   []RoomMemberEntry `json:"members,omitempty" bson:"members,omitempty"`
	Timestamp int64             `json:"timestamp"         bson:"timestamp"`
}

// Participant represents a user with display name info for client rendering.
// DisplayName is the render-ready composed name (see pkg/displayfmt.CombineWithFallback)
// and is the field push-service uses to render notifications; it is populated only
// where pre-composition is meaningful (push event senders), left empty in
// fan-out shapes that carry raw EngName/ChineseName (mentions, ClientMessage).
type Participant struct {
	UserID      string `json:"userId,omitempty"      bson:"userId,omitempty"`
	Account     string `json:"account"               bson:"account"`
	SiteID      string `json:"siteId,omitempty"      bson:"siteId,omitempty"`
	ChineseName string `json:"chineseName,omitempty" bson:"chineseName"`
	EngName     string `json:"engName,omitempty"     bson:"engName"`
	DisplayName string `json:"displayName,omitempty" bson:"displayName,omitempty"`
}

// ClientMessage wraps Message with enriched sender info for client consumption.
type ClientMessage struct {
	Message `json:",inline" bson:",inline"`
	Sender  *Participant `json:"sender,omitempty"`
	// Attachments is the sole client-facing attachments representation. It
	// deliberately shadows the embedded raw Message.Attachments ([][]byte) under
	// the same "attachments" JSON key: Go's promotion rule emits this (shallower)
	// field and suppresses the embedded one. buildClientMessage also nils the
	// embedded raw so only one representation exists in memory.
	Attachments []Attachment `json:"attachments,omitempty"`
}

type RoomEventType string

const (
	RoomEventNewMessage            RoomEventType = "new_message"
	RoomEventMessageEdited         RoomEventType = "message_edited"
	RoomEventMessageDeleted        RoomEventType = "message_deleted"
	RoomEventMessagePinned         RoomEventType = "message_pinned"
	RoomEventMessageUnpinned       RoomEventType = "message_unpinned"
	RoomEventRoomRenamed           RoomEventType = "room_renamed"
	RoomEventRoomRestricted        RoomEventType = "room_restricted"
	RoomEventMessageReacted        RoomEventType = "message_reacted"
	RoomEventThreadMetadataUpdated RoomEventType = "thread_metadata_updated"
	RoomEventMessageRead           RoomEventType = "message_read"
	RoomEventThreadMessageRead     RoomEventType = "thread_message_read"
)

// ThreadAction identifies what operation triggered a ThreadMetadataUpdatedEvent.
type ThreadAction string

const (
	ThreadActionReplyAdded   ThreadAction = "reply_added"
	ThreadActionReplyDeleted ThreadAction = "reply_deleted"
)

// RoomEvent is the live fan-out event for a newly created message
// (RoomEventNewMessage). Edits, deletes, pins, and unpins use the flattened
// EditRoomEvent / DeleteRoomEvent / PinStateRoomEvent so clients are not
// handed zero-valued base fields.
type RoomEvent struct {
	Type           RoomEventType `json:"type"`
	RoomID         string        `json:"roomId"`
	Timestamp      int64         `json:"timestamp" bson:"timestamp"`
	EventTimestamp int64         `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`

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

// MessageReadEvent is the live event published when a room's read floor
// (minUserLastSeenAt) advances after a member marks the room read. Channel
// rooms receive it once on the room event subject; DM rooms receive it per
// member on their user event subject. MinUserLastSeenAt is nil when any member
// is still fully unread (the floor cannot be established), in which case the
// field is omitted. No siteId: consumers key rooms by roomId and already cache
// the room's origin site, and this event never reaches a client for a room it
// is not already subscribed to.
type MessageReadEvent struct {
	Type              RoomEventType `json:"type" bson:"type"`
	RoomID            string        `json:"roomId" bson:"roomId"`
	MinUserLastSeenAt *time.Time    `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	Timestamp         int64         `json:"timestamp" bson:"timestamp"`
}

// ThreadMessageReadEvent is the thread equivalent of MessageReadEvent: published
// when a thread's read floor advances. It is routed by the parent room's type
// (channel → room subject, dm → per-member user subject) and carries both the
// parent RoomID (for client scoping) and the ThreadRoomID that advanced.
// MinUserLastSeenAt is omitted when a member is still fully unread.
type ThreadMessageReadEvent struct {
	Type              RoomEventType `json:"type" bson:"type"`
	RoomID            string        `json:"roomId" bson:"roomId"`
	ThreadRoomID      string        `json:"threadRoomId" bson:"threadRoomId"`
	MinUserLastSeenAt *time.Time    `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	Timestamp         int64         `json:"timestamp" bson:"timestamp"`
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
	EventTimestamp      int64           `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`
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
	Type           RoomEventType `json:"type" bson:"type"`
	RoomID         string        `json:"roomId" bson:"roomId"`
	SiteID         string        `json:"siteId" bson:"siteId"`
	Timestamp      int64         `json:"timestamp" bson:"timestamp"`
	EventTimestamp int64         `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`
	MessageID      string        `json:"messageId" bson:"messageId"`
	DeletedBy      string        `json:"deletedBy" bson:"deletedBy"`
	DeletedAt      time.Time     `json:"deletedAt" bson:"deletedAt"`
	UpdatedAt      time.Time     `json:"updatedAt" bson:"updatedAt"`
}

// PinStateRoomEvent is the live event published when a message is pinned or
// unpinned. Fields are flat (no zero-valued RoomEvent base fields). Mirrors
// the EditRoomEvent / DeleteRoomEvent pattern. Pinned carries the resulting
// pin state; By and At name the actor and domain time of the change. Type
// stays the discriminator (RoomEventMessagePinned / RoomEventMessageUnpinned).
type PinStateRoomEvent struct {
	Type           RoomEventType `json:"type" bson:"type"`
	RoomID         string        `json:"roomId" bson:"roomId"`
	SiteID         string        `json:"siteId" bson:"siteId"`
	Timestamp      int64         `json:"timestamp" bson:"timestamp"`
	EventTimestamp int64         `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`
	MessageID      string        `json:"messageId" bson:"messageId"`
	Pinned         bool          `json:"pinned" bson:"pinned"`
	By             *Participant  `json:"by,omitempty" bson:"by,omitempty"`
	At             time.Time     `json:"at" bson:"at"`
}

// ThreadMetadataUpdatedEvent is published on the per-user NATS subject when a
// thread reply is added or deleted, so clients can update the reply-count badge
// on the parent message without re-fetching the full message.
type ThreadMetadataUpdatedEvent struct {
	Type               RoomEventType `json:"type" bson:"type"`
	RoomID             string        `json:"roomId" bson:"roomId"`
	SiteID             string        `json:"siteId" bson:"siteId"`
	Timestamp          int64         `json:"timestamp" bson:"timestamp"`
	EventTimestamp     int64         `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`
	ParentMessageID    string        `json:"parentMessageId" bson:"parentMessageId"`
	ReplyMessageID     string        `json:"replyMessageId" bson:"replyMessageId"`
	NewTCount          int           `json:"newTcount" bson:"newTcount"`
	NewThreadLastMsgAt *time.Time    `json:"newThreadLastMsgAt,omitempty" bson:"newThreadLastMsgAt,omitempty"`
	Action             ThreadAction  `json:"action" bson:"action"`
}

// RoomRenamedRoomEvent is the live event published when a channel is renamed.
// Flat shape (same convention as EditRoomEvent / DeleteRoomEvent) — no
// zero-valued RoomEvent base fields shipped to clients. Drives the client's
// local subscription `name` update; no separate `subscription.update` fan-out
// fires for renames.
type RoomRenamedRoomEvent struct {
	Type      RoomEventType `json:"type" bson:"type"`
	RoomID    string        `json:"roomId" bson:"roomId"`
	SiteID    string        `json:"siteId" bson:"siteId"`
	Timestamp int64         `json:"timestamp" bson:"timestamp"`
	NewName   string        `json:"newName" bson:"newName"`
	ByAccount string        `json:"byAccount" bson:"byAccount"`
	RenamedAt time.Time     `json:"renamedAt" bson:"renamedAt"`
}

// RoomRestrictedRoomEvent is the live event published when a channel's
// restricted / externalAccess flags change. Flat shape. `OwnerAccount` is
// non-empty only on the unrestricted→restricted transition (when an owner is
// designated). Drives the client's local subscription `restricted` /
// `externalAccess` / `roles` update.
type RoomRestrictedRoomEvent struct {
	Type           RoomEventType `json:"type" bson:"type"`
	RoomID         string        `json:"roomId" bson:"roomId"`
	SiteID         string        `json:"siteId" bson:"siteId"`
	Timestamp      int64         `json:"timestamp" bson:"timestamp"`
	Restricted     bool          `json:"restricted" bson:"restricted"`
	ExternalAccess bool          `json:"externalAccess" bson:"externalAccess"`
	OwnerAccount   string        `json:"ownerAccount,omitempty" bson:"ownerAccount,omitempty"`
	ByAccount      string        `json:"byAccount" bson:"byAccount"`
	ChangedAt      time.Time     `json:"changedAt" bson:"changedAt"`
}

// ReactRoomEvent is the live event published when a reaction is toggled.
// Actor carries the full Participant so clients can render display names without a side lookup.
type ReactRoomEvent struct {
	Type           RoomEventType  `json:"type" bson:"type"`
	RoomID         string         `json:"roomId" bson:"roomId"`
	SiteID         string         `json:"siteId" bson:"siteId"`
	Timestamp      int64          `json:"timestamp" bson:"timestamp"`
	EventTimestamp int64          `json:"eventTimestamp,omitempty" bson:"eventTimestamp,omitempty"`
	MessageID      string         `json:"messageId" bson:"messageId"`
	Shortcode      string         `json:"shortcode" bson:"shortcode"`
	Action         ReactionAction `json:"action"    bson:"action"`
	Actor          Participant    `json:"actor"     bson:"actor"`
	ReactedAt      time.Time      `json:"reactedAt" bson:"reactedAt"`
	UpdatedAt      time.Time      `json:"updatedAt" bson:"updatedAt"`
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

// SubscriptionMuteToggledEvent is the InboxEvent.Payload for type "subscription_mute_toggled".
type SubscriptionMuteToggledEvent struct {
	Account   string `json:"account"              bson:"account"`
	RoomID    string `json:"roomId"               bson:"roomId"`
	Muted     bool   `json:"muted" bson:"muted"`
	Timestamp int64  `json:"timestamp"            bson:"timestamp"`
}

// FavoriteToggleResponse is the sync reply for the favorite.toggle RPC.
type FavoriteToggleResponse struct {
	Status   string `json:"status"`
	Favorite bool   `json:"favorite"`
}

// SubscriptionFavoriteToggledEvent is the InboxEvent.Payload for type "subscription_favorite_toggled".
type SubscriptionFavoriteToggledEvent struct {
	Account   string `json:"account"              bson:"account"`
	RoomID    string `json:"roomId"               bson:"roomId"`
	Favorite  bool   `json:"favorite"             bson:"favorite"`
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
	// Code and Reason mirror the errcode envelope; typed as string so pkg/model
	// does not import pkg/errcode.
	Code      string `json:"code,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

const (
	AsyncJobOpRoomCreate           = "room.create"
	AsyncJobOpRoomMemberAdd        = "room.member.add"
	AsyncJobOpRoomMemberRemove     = "room.member.remove"
	AsyncJobOpRoomMemberRemoveOrg  = "room.member.remove_org"
	AsyncJobOpRoomMemberRoleUpdate = "room.member.role_update"
	AsyncJobOpRoomRename           = "room.rename"
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
	// MessageTypeRoomRenamed is the system-message type emitted when a channel is renamed.
	MessageTypeRoomRenamed = "room_renamed"
	// MessageTypeRoomRestricted is the system-message type emitted when a
	// channel's Restricted/ExternalAccess flags change.
	MessageTypeRoomRestricted = "room_restricted"
	// MessageTypeTeamsMeetStarted is the system-message type emitted when a
	// Microsoft Teams online meeting is created for a room. Its SysMsgData
	// carries the meeting ID + join URL (TeamsMeetStartedSysData) and is read
	// back per-room to make the meetings RPC idempotent.
	MessageTypeTeamsMeetStarted = "teams_meet_started"
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

// CreateRoomStatusExists indicates the requested DM already existed; RoomID is
// the existing room. Clients treat it as success and open that room.
const CreateRoomStatusExists = "exists"

// RoomRenamedInboxPayload is wrapped in InboxEvent.Payload for InboxRoomRenamed.
type RoomRenamedInboxPayload struct {
	RoomID    string `json:"roomId"    bson:"roomId"`
	NewName   string `json:"newName"   bson:"newName"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// RoomRestrictedInboxPayload is wrapped in InboxEvent.Payload for
// InboxRoomRestricted. When OwnerAccount is non-empty AND Restricted is
// true, the destination $cond promotes that account to sole owner.
type RoomRestrictedInboxPayload struct {
	RoomID         string `json:"roomId"                 bson:"roomId"`
	Restricted     bool   `json:"restricted"             bson:"restricted"`
	ExternalAccess bool   `json:"externalAccess"         bson:"externalAccess"`
	OwnerAccount   string `json:"ownerAccount,omitempty" bson:"ownerAccount,omitempty"`
	Timestamp      int64  `json:"timestamp"              bson:"timestamp"`
}

// StatusReply is the response for fire-and-forget RPCs that only confirm
// acceptance. Status is "ok" or "accepted" depending on the endpoint.
type StatusReply struct {
	Status string `json:"status"`
}

// StatusWithRequestReply is StatusReply plus the echoed request ID, for RPCs
// whose clients correlate the async result by request ID (rename, restricted).
type StatusWithRequestReply struct {
	Status    string `json:"status"`
	RequestID string `json:"requestId"`
}

// RoomRenameRequest is the rename RPC body. NewName-only: roomID is taken from
// the subject, never the body.
type RoomRenameRequest struct {
	NewName string `json:"newName"`
}
