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
