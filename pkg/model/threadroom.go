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
}
