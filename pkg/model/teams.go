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

// TeamsChatMember is one member of a synced Teams chat. Account and
// DisplayName are both resolved from teams_user, so both are empty when the
// member is not in teams_user (guests / users outside the system).
//
// DisplayName mirrors teams_user.displayName — Graph's raw value, NOT the
// render-ready name other services compose via displayfmt.CombineWithFallback.
// A consumer that needs the composed form must build it from the user's
// engName/chineseName rather than rendering this field directly.
type TeamsChatMember struct {
	ID                          string    `json:"id" bson:"id"`
	Account                     string    `json:"account" bson:"account"`
	DisplayName                 string    `json:"displayName" bson:"displayName"`
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
	NeedCreateRoom      bool              `json:"needCreateRoom" bson:"needCreateRoom"` // set true by teams-chat-sync (oneOnOne) or teams-chat-member-sync (group); consumed by room creation
	// NeedVerify is set true by teams-room-creation in the same write that clears
	// NeedCreateRoom, so it means "a creation event for this chat was durably
	// published". teams-room-verify audits these chats against their site and
	// clears the flag only for chats whose room and subscriptions converged.
	NeedVerify bool `json:"needVerify" bson:"needVerify"`
}

// Wire contracts for the room-creation verification lane: teams-room-verify
// (global CronJob) asks each site's teams-room-inspector what its Mongo holds
// for a batch of Teams chat ids. Service-to-service HTTP — not a client-facing
// RPC — so these are absent from docs/client-api.md by design.

// TeamsRoomVerifyMaxChatIDs bounds one verify request. It lives beside the wire
// contract because both ends depend on it: the inspector rejects a larger batch,
// and teams-room-verify's config validation refuses a VERIFY_BATCH_SIZE above it.
// Without the shared constant, raising that env var past the cap would make every
// batch 400 and leave the chats flagged forever, which reads exactly like a
// healthy no-op run.
const TeamsRoomVerifyMaxChatIDs = 500

// TeamsRoomVerifyRequest is the body of POST /internal/teams/rooms/verify.
// Chat ids are Graph ids (…@thread.v2 / …@unq.gbl.spaces); the inspector maps
// each to its room id itself, so caller and callee speak one vocabulary.
type TeamsRoomVerifyRequest struct {
	ChatIDs []string `json:"chatIds" bson:"chatIds"`
}

// TeamsRoomVerifyResult is one chat's state at the answering site.
// SubscriptionCount is the live count of subscription documents for the room
// and is what teams-room-verify's pass/fail comparison uses. RoomUserCount is
// the room's denormalized counter; it plays no part in that comparison and is
// carried purely as extra context in the mismatch log, for diagnosing why a
// chat didn't converge. Both are zero when RoomExists is false.
type TeamsRoomVerifyResult struct {
	ChatID            string `json:"chatId" bson:"chatId"`
	RoomID            string `json:"roomId" bson:"roomId"`
	RoomExists        bool   `json:"roomExists" bson:"roomExists"`
	SubscriptionCount int    `json:"subscriptionCount" bson:"subscriptionCount"`
	RoomUserCount     int    `json:"roomUserCount" bson:"roomUserCount"`
}

// TeamsRoomVerifyResponse is the inspector's reply. Chats carries exactly one
// result per requested chat id, in request order; FoundCount is how many of
// them have a room.
type TeamsRoomVerifyResponse struct {
	SiteID         string                  `json:"siteId" bson:"siteId"`
	RequestedCount int                     `json:"requestedCount" bson:"requestedCount"`
	FoundCount     int                     `json:"foundCount" bson:"foundCount"`
	Chats          []TeamsRoomVerifyResult `json:"chats" bson:"chats"`
}
