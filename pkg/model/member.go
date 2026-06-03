package model

import "time"

type RoomMemberType string

const (
	RoomMemberIndividual RoomMemberType = "individual"
	RoomMemberOrg        RoomMemberType = "org"
)

type HistoryMode string

const (
	HistoryModeNone HistoryMode = "none"
	HistoryModeAll  HistoryMode = "all"
)

type HistoryConfig struct {
	Mode HistoryMode `json:"mode" bson:"mode"`
}

// ChannelRef identifies a source channel by room + its home site. Used by add-member
// to expand cross-site source channels via the remote site's member.list endpoint.
type ChannelRef struct {
	RoomID string `json:"roomId" bson:"roomId"`
	SiteID string `json:"siteId" bson:"siteId"`
}

// AddMembersRequest is the event published by room-service when a user requests to add members to a room.
type AddMembersRequest struct {
	RoomID           string        `json:"roomId"           bson:"roomId"`
	Users            []string      `json:"users"            bson:"users"`
	Orgs             []string      `json:"orgs"             bson:"orgs"`
	Channels         []ChannelRef  `json:"channels"         bson:"channels"`
	History          HistoryConfig `json:"history"          bson:"history"`
	RequesterID      string        `json:"requesterId"      bson:"requesterId"`
	RequesterAccount string        `json:"requesterAccount" bson:"requesterAccount"`
	Timestamp        int64         `json:"timestamp"        bson:"timestamp"`
}

type RoomMember struct {
	ID     string          `json:"id"     bson:"_id"`
	RoomID string          `json:"rid"    bson:"rid"`
	Ts     time.Time       `json:"ts"     bson:"ts"`
	Member RoomMemberEntry `json:"member" bson:"member"`
}

type RoomMemberEntry struct {
	ID      string         `json:"id"                bson:"id"`
	Type    RoomMemberType `json:"type"              bson:"type"`
	Account string         `json:"account,omitempty" bson:"account,omitempty"`

	// Display fields — never persisted (bson:"-"); populated only when
	// ListRoomMembers is called with enrich=true. Elided from JSON when zero.
	EngName     string `json:"engName,omitempty"     bson:"-"`
	ChineseName string `json:"chineseName,omitempty" bson:"-"`
	// Name is the app's display name for bot members (account matching
	// the ".bot" suffix). For humans, EngName/ChineseName are populated
	// and Name stays empty. Caller chooses display: name ?? engName ?? account.
	Name        string `json:"name,omitempty"        bson:"-"`
	IsOwner     bool   `json:"isOwner,omitempty"     bson:"-"`
	OrgName     string `json:"orgName,omitempty"     bson:"-"`
	MemberCount int    `json:"memberCount,omitempty" bson:"-"`
}

type RemoveMemberRequest struct {
	RoomID    string `json:"roomId"             bson:"roomId"`
	Requester string `json:"requester"          bson:"requester"`
	Account   string `json:"account,omitempty"  bson:"account,omitempty"`
	OrgID     string `json:"orgId,omitempty"    bson:"orgId,omitempty"`
	// Set by room-service at acceptance; stable seed for Message.ID + Nats-Msg-Id.
	Timestamp int64 `json:"timestamp" bson:"timestamp"`
	// Set by room-service after the GetRoom check; carried to room-worker to avoid a redundant Mongo round-trip.
	RoomType RoomType `json:"roomType,omitempty" bson:"roomType,omitempty"`
}

type SysMsgUser struct {
	Account     string `json:"account"`
	EngName     string `json:"engName"`
	ChineseName string `json:"chineseName"`
}

type MemberLeft struct {
	User SysMsgUser `json:"user"`
}

type MemberRemoved struct {
	User              *SysMsgUser `json:"user,omitempty"`
	OrgID             string      `json:"orgId,omitempty"`
	SectName          string      `json:"sectName,omitempty"`
	RemovedUsersCount int         `json:"removedUsersCount"`
}

// MembersAdded describes the members that were added to a room, including individuals, organizations, and channel sources.
type MembersAdded struct {
	Individuals     []string     `json:"individuals"`
	Orgs            []string     `json:"orgs"`
	Channels        []ChannelRef `json:"channels"`
	AddedUsersCount int          `json:"addedUsersCount"`
}

// RoomCreated is the sys-message payload emitted on channel creation.
type RoomCreated struct {
	Name            string       `json:"name"`
	Users           []string     `json:"users"`
	Orgs            []string     `json:"orgs"`
	Channels        []ChannelRef `json:"channels"`
	AddedUsersCount int          `json:"addedUsersCount"`
}

type ListRoomMembersRequest struct {
	Limit  *int `json:"limit,omitempty"`
	Offset *int `json:"offset,omitempty"`
	Enrich bool `json:"enrich,omitempty"`
}

type ListRoomMembersResponse struct {
	Members []RoomMember `json:"members"`
}

// OrgMember is the wire projection returned by the list-org-members endpoint.
// Only fields the UI actually renders are included — EmployeeID, SectID, and
// SectName are intentionally omitted (redundant or irrelevant for the caller,
// who already knows which orgId they asked about).
type OrgMember struct {
	ID          string `json:"id"          bson:"_id"`
	Account     string `json:"account"     bson:"account"`
	EngName     string `json:"engName"     bson:"engName"`
	ChineseName string `json:"chineseName" bson:"chineseName"`
	SiteID      string `json:"siteId"      bson:"siteId"`
}

type ListOrgMembersResponse struct {
	Members []OrgMember `json:"members"`
}

// CreateRoomRequest is the canonical event payload (X-Request-ID rides on the NATS header).
// Users/Orgs/Channels are the literal client request; ResolvedUsers/ResolvedOrgs carry the
// post-expansion (channel-ref-merged, requester-stripped, dedup'd) sets the worker uses for
// member materialization. Sys-message payloads use the literal lists.
type CreateRoomRequest struct {
	Name     string       `json:"name"     bson:"name"`
	Users    []string     `json:"users"    bson:"users"`
	Orgs     []string     `json:"orgs"     bson:"orgs"`
	Channels []ChannelRef `json:"channels" bson:"channels"`

	ResolvedUsers []string `json:"resolvedUsers,omitempty" bson:"resolvedUsers,omitempty"`
	ResolvedOrgs  []string `json:"resolvedOrgs,omitempty"  bson:"resolvedOrgs,omitempty"`

	RoomID           string `json:"roomId"            bson:"roomId"`
	RequesterID      string `json:"requesterId"       bson:"requesterId"`
	RequesterAccount string `json:"requesterAccount"  bson:"requesterAccount"`
	Timestamp        int64  `json:"timestamp"         bson:"timestamp"`
}

// SyncCreateDMRequest is the request payload for chat.server.request.room.{siteID}.create.dm.
// Caller (user-service) is responsible for all data-integrity validation before issuing.
type SyncCreateDMRequest struct {
	RoomType         RoomType `json:"roomType"         bson:"roomType"`
	RequesterAccount string   `json:"requesterAccount" bson:"requesterAccount"`
	OtherAccount     string   `json:"otherAccount"     bson:"otherAccount"`
}

// SyncCreateDMReply is the success reply; errors flow via errnats.Reply (pkg/errcode envelope) instead.
type SyncCreateDMReply struct {
	Success      bool         `json:"success"      bson:"success"`
	Subscription Subscription `json:"subscription" bson:"subscription"`
}
