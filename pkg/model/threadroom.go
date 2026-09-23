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
	// ParentStamped is true once the parent message's thread_room_id has been
	// written. Server-side only, never sent to clients. Replies keep re-writing the
	// stamp while this is false. Rooms created before this field have no value,
	// which reads as false, so their next reply repairs them.
	ParentStamped bool `json:"-" bson:"parentStamped,omitempty"`
}
