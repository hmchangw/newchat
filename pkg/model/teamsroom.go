package model

import "time"

// TeamsRoomCreateEvent is the batch envelope published by teams-room-creation
// to the room-canonical subject chat.room.canonical.{siteID}.teams.create.
// One event carries up to N chats that all share SiteID. Consumed by the room
// materialization worker (out of scope here).
type TeamsRoomCreateEvent struct {
	SiteID    string                `json:"siteId" bson:"siteId"`
	Chats     []TeamsRoomCreateChat `json:"chats" bson:"chats"`
	Timestamp int64                 `json:"timestamp" bson:"timestamp"` // publish time, UnixMilli UTC
}

// TeamsRoomCreateChat is one chat's room-creation input.
type TeamsRoomCreateChat struct {
	ID              string                  `json:"id" bson:"id"`
	Name            string                  `json:"name" bson:"name"`
	Members         []TeamsRoomCreateMember `json:"members" bson:"members"`
	CreatedDateTime time.Time               `json:"createdDateTime" bson:"createdDateTime"`
}

// TeamsRoomCreateMember is one member reference in a room-creation event: only
// the account and history-visibility cutoff are carried (the Graph member id is
// intentionally dropped).
type TeamsRoomCreateMember struct {
	Account                     string    `json:"account" bson:"account"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}
