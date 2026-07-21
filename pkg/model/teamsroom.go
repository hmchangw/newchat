package model

import "time"

// TeamsRoomCreateEvent is the batch envelope published by teams-room-creation
// to the room-canonical subject chat.room.canonical.{siteID}.teams.create.
// One event carries up to N chats that all share a site — the site is carried
// on the subject, not in the payload. Consumed by the room materialization
// worker (out of scope here).
type TeamsRoomCreateEvent struct {
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

// TeamsRoomCreateMember is one member reference in a room-creation event: the
// member's user id, account, and history-visibility cutoff.
type TeamsRoomCreateMember struct {
	ID                          string    `json:"id" bson:"id"` // member's AAD user object id (the teams_user _id)
	Account                     string    `json:"account" bson:"account"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}
