package models

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// SubscriptionListRequest is the body of subscription.list.
// Type ∈ {current, rooms, apps}. UpdatedWithinDays nil ⇒ no age filter.
// Offset/Limit page the result; omitted (0) ⇒ first page at the server default size.
// IncludeLastMessage nil/absent ⇒ include (backward-compatible); false ⇒ skip the
// last-message enrichment.
type SubscriptionListRequest struct {
	Type               string `json:"type"`
	Favorite           *bool  `json:"favorite,omitempty"`
	UpdatedWithinDays  *int   `json:"updatedWithinDays,omitempty"`
	IncludeLastMessage *bool  `json:"includeLastMessage,omitempty"`
	Offset             int    `json:"offset,omitempty"`
	Limit              int    `json:"limit,omitempty"`
}

// SubscriptionListResponse is returned by subscription.getByRoomID (a 0-or-1 point
// lookup, so it is not paginated).
// Subscriptions is a heterogeneous slice of per-room-type rows ([model.SubscriptionItem]):
// channel ([model.ChannelSubscription]), dm ([model.DMSubscription], adds hrInfo), and
// botDM ([model.BotDMSubscription], adds a nested app object).
type SubscriptionListResponse struct {
	Subscriptions []model.SubscriptionItem `json:"subscriptions"`
	Total         int                      `json:"total"`
}

// PagedSubscriptionListResponse is returned by subscription.list and
// subscription.getChannels: one page of rows plus hasMore, which is true when the
// next page would return at least one more row (the server over-fetches by one).
type PagedSubscriptionListResponse struct {
	Subscriptions []model.SubscriptionItem `json:"subscriptions"`
	HasMore       bool                     `json:"hasMore"`
}

// GetChannelsRequest is the body of subscription.getChannels (exactly one of
// membersContain/accountNames set). Offset/Limit page the result; omitted (0) ⇒
// first page at the server default size.
type GetChannelsRequest struct {
	MembersContain string   `json:"membersContain,omitempty"`
	AccountNames   []string `json:"accountNames,omitempty"`
	Offset         int      `json:"offset,omitempty"`
	Limit          int      `json:"limit,omitempty"`
}

// GetDMRequest is the body of subscription.getDM.
type GetDMRequest struct {
	AccountName string `json:"accountName"`
}

// DMResponse wraps the enriched DM subscription returned by subscription.getDM.
type DMResponse struct {
	Subscription model.DMSubscription `json:"subscription"`
}

// GetByRoomIDRequest is the body of subscription.getByRoomID.
type GetByRoomIDRequest struct {
	RoomID string `json:"roomId"`
}

// CountRequest is the body of subscription.count (Unread nil/false ⇒ total).
type CountRequest struct {
	Unread *bool `json:"unread,omitempty"`
}

// CountResponse is returned by subscription.count.
type CountResponse struct {
	Count int `json:"count"`
}

// ActiveSubscription is the badge path's row: the five fields unreadRooms reads.
// A field absent here is a field GetActiveSubscriptions does not fetch.
type ActiveSubscription struct {
	RoomID       string     `json:"roomId"                 bson:"roomId"`
	SiteID       string     `json:"siteId"                 bson:"siteId"`
	LastSeenAt   *time.Time `json:"lastSeenAt,omitempty"   bson:"lastSeenAt,omitempty"`
	ThreadUnread []string   `json:"threadUnread,omitempty" bson:"threadUnread,omitempty"`
	// Joined from the room; nil for a cross-site sub or a room with no messages.
	LastMsgAt *time.Time `json:"lastMsgAt,omitempty" bson:"lastMsgAt,omitempty"`
	// Joined from the room like LastMsgAt; the unread reference is
	// LastUserMsgAt ?? LastMsgAt.
	LastUserMsgAt *time.Time `json:"lastUserMsgAt,omitempty" bson:"lastUserMsgAt,omitempty"`
}
