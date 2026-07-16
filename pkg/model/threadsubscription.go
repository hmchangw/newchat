package model

import (
	"errors"
	"time"
)

var ErrThreadSubscriptionNotFound = errors.New("thread subscription not found")

type ThreadSubscription struct {
	ID              string `json:"id"              bson:"_id"`
	ParentMessageID string `json:"parentMessageId" bson:"parentMessageId"`
	RoomID          string `json:"roomId"          bson:"roomId"`
	ThreadRoomID    string `json:"threadRoomId"    bson:"threadRoomId"`
	UserID          string `json:"userId"          bson:"userId"`
	UserAccount     string `json:"userAccount"     bson:"userAccount"`
	// SiteID is the home site of the room that contains this thread — same
	// semantic as Subscription.SiteID. Across cross-site federation it stays
	// constant: every replica of a given subscription has the same SiteID
	// regardless of which site stores the document.
	SiteID string `json:"siteId" bson:"siteId"`
	// Never add omitempty: unreadThreadsPipeline relies on BSON encoding nil as explicit null, not a missing field.
	LastSeenAt *time.Time `json:"lastSeenAt"  bson:"lastSeenAt"`
	HasMention bool       `json:"hasMention"  bson:"hasMention"`
	CreatedAt  time.Time  `json:"createdAt"   bson:"createdAt"`
	UpdatedAt  time.Time  `json:"updatedAt"   bson:"updatedAt"`
}

// ThreadUnreadRow is an account's thread-sub joined with its subscription: the
// badge fields plus the room type. Only rows the account can still access
// survive the join; RoomType feeds the DM tally.
type ThreadUnreadRow struct {
	ThreadRoomID string     `json:"threadRoomId" bson:"threadRoomId"`
	SiteID       string     `json:"siteId"       bson:"siteId"`
	RoomType     RoomType   `json:"roomType"     bson:"roomType"`
	LastSeenAt   *time.Time `json:"lastSeenAt"   bson:"lastSeenAt"`
	HasMention   bool       `json:"hasMention"   bson:"hasMention"`
}

// ThreadUnreadSummaryRequest is the client-facing thread-unread badge request. The
// account rides the subject; no body fields.
type ThreadUnreadSummaryRequest struct{}

// ThreadUnreadSummaryResponse is the cross-site thread-unread badge. Booleans are ORed
// and LastMessageAt is maxed over the responding sites. UnavailableSites lists
// sites whose RPC failed so a client can distinguish degraded from authoritative.
type ThreadUnreadSummaryResponse struct {
	Unread              bool     `json:"unread"`
	UnreadDirectMessage bool     `json:"unreadDirectMessage"`
	UnreadMention       bool     `json:"unreadMention"`
	LastMessageAt       *int64   `json:"lastMessageAt,omitempty"` // UnixMilli
	UnavailableSites    []string `json:"unavailableSites,omitempty"`
}

// ThreadReadAllRequest is the client-facing clear-all-thread-unread request. The
// account rides the subject; no body fields.
type ThreadReadAllRequest struct{}

// ThreadReadAllResponse is the cross-site clear-all-thread-unread result.
// ClearedThreads sums the thread subscriptions cleared on each responding site.
// UnavailableSites lists sites whose bulk-clear RPC failed (their threads may
// remain unread); the overall call still succeeds.
type ThreadReadAllResponse struct {
	ClearedThreads   int      `json:"clearedThreads"`
	UnavailableSites []string `json:"unavailableSites,omitempty"`
}

// RoomThreadReadAllRequest is the server-to-server request user-service sends to a
// site's room-service to clear all of an account's thread-unread state.
type RoomThreadReadAllRequest struct {
	Account string `json:"account"`
}

// RoomThreadReadAllResponse reports how many thread subscriptions the site cleared.
type RoomThreadReadAllResponse struct {
	ClearedThreads int `json:"clearedThreads"`
}

// ThreadRoomInfoBatchRequest asks room-service for a batch of thread rooms' info.
type ThreadRoomInfoBatchRequest struct {
	ThreadRoomIDs []string `json:"threadRoomIds"`
}

// ThreadRoomInfo is one thread room's last-activity time. Found=false means the
// thread room does not exist (LastMsgAt is 0).
type ThreadRoomInfo struct {
	ThreadRoomID string `json:"threadRoomId"`
	Found        bool   `json:"found"`
	LastMsgAt    int64  `json:"lastMsgAt"` // UnixMilli; 0 when Found=false
}

// ThreadRoomInfoBatchResponse is the batch reply, one entry per requested id.
type ThreadRoomInfoBatchResponse struct {
	Threads []ThreadRoomInfo `json:"threads"`
}
