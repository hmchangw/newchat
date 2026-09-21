package model

import "time"

type ThreadRoom struct {
	ID                    string     `json:"id"                    bson:"_id"`
	ParentMessageID       string     `json:"parentMessageId"       bson:"parentMessageId"`
	ThreadParentCreatedAt time.Time  `json:"threadParentCreatedAt" bson:"threadParentCreatedAt"`
	RoomID                string     `json:"roomId"                bson:"roomId"`
	SiteID                string     `json:"siteId"                bson:"siteId"`
	LastMsgAt             time.Time  `json:"lastMsgAt"             bson:"lastMsgAt"`
	LastMsgID             string     `json:"lastMsgId"             bson:"lastMsgId"`
	ReplyAccounts         []string   `json:"replyAccounts"         bson:"replyAccounts"`
	CreatedAt             time.Time  `json:"createdAt"             bson:"createdAt"`
	UpdatedAt             time.Time  `json:"updatedAt"             bson:"updatedAt"`
	MinUserLastSeenAt     *time.Time `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	// ParentStamped records that the parent message's thread_room_id has been
	// written. Internal bookkeeping, never serialized to clients: the first reply
	// sets it after a successful stamp, and a subsequent reply re-stamps while it
	// is false. Absent on rooms created before this field, which reads as false,
	// so their next reply repairs a stamp that may never have landed.
	ParentStamped bool `json:"-" bson:"parentStamped,omitempty"`
}
