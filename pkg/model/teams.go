package model

import "time"

// Request/reply contracts for the Microsoft Teams integration RPCs handled by
// room-service. Two endpoints (room call, user call) build a Teams deep link
// with no external I/O; the meetings endpoint creates a Graph onlineMeeting and
// is idempotent per room.

// TeamsRoomCallRequest is the request body for the room-call deep-link RPC.
// The room is carried on the NATS subject, so the body may be empty; the field
// is accepted for clients that prefer to pass it explicitly.
type TeamsRoomCallRequest struct {
	// RoomID is optional — the authoritative room is the subject's {roomID}.
	RoomID string `json:"roomId,omitempty"`
}

// TeamsUserCallRequest is the request body for the 1:1 user-call deep-link RPC.
type TeamsUserCallRequest struct {
	// AccountName is the target user's account; its email is derived as
	// account@TEAMS_EMAIL_DOMAIN.
	AccountName string `json:"accountName"`
}

// TeamsCallReply is the reply for both deep-link RPCs (calls/room, calls/user).
type TeamsCallReply struct {
	JoinURL string `json:"joinUrl"`
}

// TeamsMeetingRequest is the request body for the meetings RPC. The room is
// carried on the NATS subject; the body may be empty.
type TeamsMeetingRequest struct {
	// RoomID is optional — the authoritative room is the subject's {roomID}.
	RoomID string `json:"roomId,omitempty"`
}

// TeamsMeetingReply is the reply for the meetings RPC: the Graph onlineMeeting
// ID and its join URL.
type TeamsMeetingReply struct {
	ID      string `json:"id"`
	JoinURL string `json:"joinUrl"`
}

// TeamsMeetingRecord is the first-class persisted record of a room's Teams
// meeting in the teams_meetings collection. A UNIQUE index on (roomId, siteId)
// makes the meetings RPC retry-safe: a concurrent second create hits a
// duplicate-key error, and the loser reads back the winner's record instead of
// creating a duplicate system message. This is the same unique-index +
// IsDuplicateKeyError idempotency convention room-service already uses for
// room_members and subscriptions.
type TeamsMeetingRecord struct {
	RoomID    string `bson:"roomId"`
	SiteID    string `bson:"siteId"`
	MeetingID string `bson:"meetingId"`
	JoinURL   string `bson:"joinUrl"`
	CreatedAt int64  `bson:"createdAt"`
}

// TeamsChatTypeOneOnOne is Graph's chatType for 1:1 chats. A oneOnOne chat
// never changes after creation, so the sync inserts it once and never updates.
const TeamsChatTypeOneOnOne = "oneOnOne"

// TeamsChatMember is one member of a synced Teams chat. Account is empty when
// the member is not in teams_user (guests / users outside the system).
type TeamsChatMember struct {
	ID                          string    `json:"id" bson:"id"`
	Account                     string    `json:"account" bson:"account"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}

// TeamsChat is one document of the teams_chat collection, owned by
// teams-chat-sync. SiteID is the member-majority site, written only on insert
// ($setOnInsert) and never changed afterwards.
type TeamsChat struct {
	ID                  string            `json:"id" bson:"_id"` // Graph chat id
	Name                string            `json:"name" bson:"name"`
	ChatType            string            `json:"chatType" bson:"chatType"` // oneOnOne | group | meeting
	CreatedDateTime     time.Time         `json:"createdDateTime" bson:"createdDateTime"`
	LastUpdatedDateTime time.Time         `json:"lastUpdatedDateTime" bson:"lastUpdatedDateTime"`
	Members             []TeamsChatMember `json:"members" bson:"members"`
	SiteID              string            `json:"siteId" bson:"siteId"`
	UpdatedAt           time.Time         `json:"updatedAt" bson:"updatedAt"` // stamped now at build time; written on every upsert
	NeedMemberSync      bool              `json:"needMemberSync" bson:"needMemberSync"`
	NeedCreateRoom      bool              `json:"needCreateRoom" bson:"needCreateRoom"` // set true by teams-chat-member-sync; consumed by room creation
}
