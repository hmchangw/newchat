package model

import (
	"errors"
	"time"
)

// ErrSubscriptionNotFound is returned when a subscription lookup finds no matching document.
var ErrSubscriptionNotFound = errors.New("subscription not found")

type Role string

const (
	RoleOwner Role = "owner"
	// RoleAdmin is recognized by message-gatekeeper's large-room bypass but not yet assignable via role-update RPC.
	RoleAdmin  Role = "admin"
	RoleMember Role = "member"
)

type SubscriptionUser struct {
	ID      string `json:"id" bson:"_id"`
	Account string `json:"account" bson:"account"`
	IsBot   bool   `json:"isBot" bson:"isBot"`
}

type Subscription struct {
	ID                 string           `json:"id" bson:"_id"`
	User               SubscriptionUser `json:"u" bson:"u"`
	RoomID             string           `json:"roomId" bson:"roomId"`
	SiteID             string           `json:"siteId" bson:"siteId"`
	Roles              []Role           `json:"roles" bson:"roles"`
	Name               string           `json:"name"                    bson:"name"`
	RoomType           RoomType         `json:"roomType"                bson:"roomType"`
	IsSubscribed       bool             `json:"isSubscribed,omitempty"  bson:"isSubscribed,omitempty"`
	HistorySharedSince *time.Time       `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`
	JoinedAt           time.Time        `json:"joinedAt" bson:"joinedAt"`
	LastSeenAt         *time.Time       `json:"lastSeenAt,omitempty" bson:"lastSeenAt,omitempty"`
	HasMention         bool             `json:"hasMention" bson:"hasMention"`
	ThreadUnread       []string         `json:"threadUnread,omitempty" bson:"threadUnread,omitempty"`
	Alert              bool             `json:"alert" bson:"alert"`
	Muted              bool             `json:"muted" bson:"muted"`
	Favorite           bool             `json:"favorite" bson:"favorite"`
	// Denormalized from Room.{Restricted,ExternalAccess}; the only place remote sites carry restricted state
	// (cross-site inbox mirrors subscriptions, not Room docs). Treat missing as false.
	Restricted     bool `json:"restricted,omitempty"     bson:"restricted,omitempty"`
	ExternalAccess bool `json:"externalAccess,omitempty" bson:"externalAccess,omitempty"`

	// HasUnread and HasGroupMention are NOT persisted (bson:"-"); subscription.list
	// enrichment computes them at read time from the room's LastMsgAt /
	// LastMentionAllAt (carried on EnrichedSubscription) vs the subscription's LastSeenAt.
	HasUnread       bool `json:"hasUnread"       bson:"-"`
	HasGroupMention bool `json:"hasGroupMention" bson:"-"`

	// Room carries all room-derived fields, populated at read time from room-service's
	// RoomsInfoBatch RPC (baseline $lookup values when the RPC degrades). Never persisted.
	Room *SubscriptionRoom `json:"room,omitempty" bson:"-"`

	// Per-attribute last-change timestamps persisted on the Mongo subscriptions
	// document, stamped by the write path as order-safety guards and surfaced to
	// clients. Each is a nullable pointer, nil until the first guarded write, so a
	// creating writer that doesn't stamp it never persists a zero-time placeholder.
	// bson keys match what the writers store. RestrictUpdatedAt was formerly
	// `visibilityUpdatedAt`.
	FavoriteUpdatedAt *time.Time `json:"favoriteUpdatedAt,omitempty" bson:"favoriteUpdatedAt,omitempty"`
	MuteUpdatedAt     *time.Time `json:"muteUpdatedAt,omitempty"     bson:"muteUpdatedAt,omitempty"`
	RolesUpdatedAt    *time.Time `json:"rolesUpdatedAt,omitempty"    bson:"rolesUpdatedAt,omitempty"`
	NameUpdatedAt     *time.Time `json:"nameUpdatedAt,omitempty"     bson:"nameUpdatedAt,omitempty"`
	RestrictUpdatedAt *time.Time `json:"restrictUpdatedAt,omitempty" bson:"restrictUpdatedAt,omitempty"`
}

// EnrichedSubscription is the decode target for the subscription-list aggregation:
// a stored Subscription (inlined) plus the read-time room baseline projected by the
// rooms $lookup/$addFields. The baseline lives HERE, not on Subscription, so a plain
// Subscription can never persist stale room data — the INSERT type carries no room
// fields. All baseline fields are internal (json:"-"): they build sub.Room for LOCAL
// subs (cross-site subs carry zero values) and are never serialized to the client.
type EnrichedSubscription struct {
	Subscription `bson:",inline"`

	// Room metadata copied from the joined room doc by the $addFields stage.
	UserCount         int        `json:"-" bson:"userCount,omitempty"`
	LastMsgAt         *time.Time `json:"-" bson:"lastMsgAt,omitempty"`
	LastMsgID         string     `json:"-" bson:"lastMsgId,omitempty"`
	LastMentionAllAt  *time.Time `json:"-" bson:"lastMentionAllAt,omitempty"`
	MinUserLastSeenAt *time.Time `json:"-" bson:"minUserLastSeenAt,omitempty"`
	AppCount          int        `json:"-" bson:"appCount,omitempty"`
	RoomName          string     `json:"-" bson:"roomName,omitempty"` // room canonical name (distinct from the sub's display Name)
	// Room E2E key baseline projected from the room's encKey sub-document by the rooms
	// $lookup (current-slot priv/ver only); used to build sub.Room.PrivateKey/KeyVersion
	// for LOCAL subs without a second key read. Cross-site subs carry zero values (the
	// key arrives via the GetRoomsInfo RPC). The room key is never written into the
	// subscriptions collection — it lives only on this read-time aggregation result.
	RoomKeyPriv []byte `json:"-" bson:"encKeyPriv,omitempty"`
	RoomKeyVer  int    `json:"-" bson:"encKeyVer,omitempty"`
}

// SubscriptionRoom is the room-derived view nested on an enriched subscription.
// Name is the room's canonical name — the subscription's own Name (counterpart
// account for DMs, app display name for botDMs) is never overwritten by it.
type SubscriptionRoom struct {
	SiteID string `json:"siteId,omitempty" bson:"-"`
	Name   string `json:"name,omitempty" bson:"-"`
	// UserCount/AppCount/LastMsgID mirror the room-service room document (model.Room).
	UserCount int `json:"userCount,omitempty" bson:"-"`
	AppCount  int `json:"appCount,omitempty" bson:"-"`
	// LastMsgAt/LastMentionAllAt/MinUserLastSeenAt are returned to the client as
	// RFC3339 timestamps (*time.Time). The room-service RPC delivers them as epoch
	// millis; enrichment converts at the seam (the local $lookup baseline already
	// carries *time.Time).
	LastMsgAt        *time.Time `json:"lastMsgAt,omitempty" bson:"-"`
	LastMsgID        string     `json:"lastMsgId,omitempty" bson:"-"`
	LastMentionAllAt *time.Time `json:"lastMentionAllAt,omitempty" bson:"-"`
	// MinUserLastSeenAt is the oldest read position across the room's members (the
	// room-wide "everyone has read up to here" floor), mirrored from the room doc.
	MinUserLastSeenAt *time.Time `json:"minUserLastSeenAt,omitempty" bson:"-"`
	// Room E2E key delivered to authorized members for initial key bootstrap
	// on subscription.list (same payload as the room.key.get RPC).
	PrivateKey *string `json:"privateKey,omitempty" bson:"-"`
	KeyVersion *int    `json:"keyVersion,omitempty" bson:"-"`
	// LastMessage is resolved at read time via history-service's rooms.get RPC
	// (A2 — no denormalized write path). Omitted when the room has no message,
	// the enriching site RPC degraded, or the room is soft-deleted (Room==nil).
	PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"-"`
}

// SubscriptionHRInfo carries the counterpart's HR-directory record on a DM subscription for sidebar/header rendering.
type SubscriptionHRInfo struct {
	Account string `json:"account" bson:"account"`
	Name    string `json:"name"    bson:"name"`
	EngName string `json:"engName" bson:"engName"`
}

// SubscriptionItem is the client-facing response shape for one subscription row.
// Each concrete type embeds the base *Subscription (common fields flatten at JSON
// marshal time) and adds its room-type-specific fields:
//   - ChannelSubscription → base only
//   - DMSubscription      → base + hrInfo
//   - BotDMSubscription   → base + a nested app object
//
// It is a read/response model only — kept distinct from the base Subscription that
// is persisted to MongoDB so the heterogeneous list can be modeled per room type.
type SubscriptionItem interface {
	// Base returns the embedded base subscription carrying the common fields.
	Base() *Subscription
	isSubscriptionItem()
}

// ChannelSubscription is the channel-room response row: just the base subscription.
type ChannelSubscription struct {
	*Subscription
}

// DMSubscription is the wire/storage shape for DM subscriptions: base Subscription plus counterpart HRInfo.
// The embedded pointer flattens at JSON marshal time; only emitted for RoomTypeDM.
type DMSubscription struct {
	*Subscription `bson:",inline"`
	HRInfo        *SubscriptionHRInfo `json:"hrInfo,omitempty" bson:"hrInfo,omitempty"`
}

// EnrichedDMSubscription is the decode target for the GetDMSubscription aggregation:
// an EnrichedSubscription (inlined, carrying the read-time room baseline) plus the
// counterpart's HRInfo. It is a read-time repo type only — the wire/storage shape
// returned to clients is DMSubscription (a plain *Subscription + HRInfo), built by
// the service after enrichment.
type EnrichedDMSubscription struct {
	EnrichedSubscription `bson:",inline"`
	HRInfo               *SubscriptionHRInfo `json:"-" bson:"hrInfo,omitempty"`
}

// BotDMSubscription is the botDM response row: base Subscription plus a nested app object.
type BotDMSubscription struct {
	*Subscription
	App *AppSubscription `json:"app,omitempty"`
}

func (s *ChannelSubscription) Base() *Subscription { return s.Subscription }
func (s *ChannelSubscription) isSubscriptionItem() {}
func (s *DMSubscription) Base() *Subscription      { return s.Subscription }
func (s *DMSubscription) isSubscriptionItem()      {}
func (s *BotDMSubscription) Base() *Subscription   { return s.Subscription }
func (s *BotDMSubscription) isSubscriptionItem()   {}

// IsRoomMember reports whether sub represents an active membership; returns false for nil.
func IsRoomMember(sub *Subscription) bool {
	return sub != nil
}

// MessageThreadReadRequest is the body of the message.thread.read RPC.
type MessageThreadReadRequest struct {
	ThreadID string `json:"threadId"`
}
