package model

import "time"

type RoomType string

const (
	RoomTypeChannel    RoomType = "channel"
	RoomTypeDM         RoomType = "dm"
	RoomTypeBotDM      RoomType = "botDM"
	RoomTypeDiscussion RoomType = "discussion"
)

type Room struct {
	ID                string     `json:"id" bson:"_id"`
	Name              string     `json:"name" bson:"name"`
	Type              RoomType   `json:"type" bson:"type"`
	SiteID            string     `json:"siteId" bson:"siteId"`
	UserCount         int        `json:"userCount" bson:"userCount"`
	AppCount          int        `json:"appCount" bson:"appCount"`
	LastMsgAt         *time.Time `json:"lastMsgAt,omitempty" bson:"lastMsgAt,omitempty"`
	LastMsgID         string     `json:"lastMsgId" bson:"lastMsgId"`
	LastMentionAllAt  *time.Time `json:"lastMentionAllAt,omitempty" bson:"lastMentionAllAt,omitempty"`
	MinUserLastSeenAt *time.Time `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	CreatedAt         time.Time  `json:"createdAt" bson:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt" bson:"updatedAt"`
	Restricted        bool       `json:"restricted,omitempty" bson:"restricted,omitempty"`
	ExternalAccess    bool       `json:"externalAccess,omitempty" bson:"externalAccess,omitempty"`
	UIDs              []string   `json:"uids,omitempty" bson:"uids,omitempty"`
	Accounts          []string   `json:"accounts,omitempty" bson:"accounts,omitempty"`
}

// RoomsInfoBatchRequest is the NATS request body for the batch room info RPC.
type RoomsInfoBatchRequest struct {
	RoomIDs []string `json:"roomIds"`
}

// RoomInfo is a single aggregated room record: Mongo metadata + room key.
type RoomInfo struct {
	RoomID            string  `json:"roomId"`
	Found             bool    `json:"found"`
	SiteID            string  `json:"siteId,omitempty"`
	Name              string  `json:"name,omitempty"`
	UserCount         int     `json:"userCount,omitempty"`
	AppCount          int     `json:"appCount,omitempty"`
	LastMsgAt         *int64  `json:"lastMsgAt,omitempty"`
	LastMsgID         string  `json:"lastMsgId,omitempty"`
	LastMentionAllAt  *int64  `json:"lastMentionAllAt,omitempty"`
	MinUserLastSeenAt *int64  `json:"minUserLastSeenAt,omitempty"`
	PrivateKey        *string `json:"privateKey,omitempty"`
	KeyVersion        *int    `json:"keyVersion,omitempty"`
	Error             string  `json:"error,omitempty"`
}

// RoomsInfoBatchResponse contains one entry per requested roomID, in input order.
type RoomsInfoBatchResponse struct {
	Rooms []RoomInfo `json:"rooms"`
}

// BuildDMParticipants returns sorted-by-UID, paired-by-index participant
// lists for a dm or botDM room. UIDs[i] and Accounts[i] always describe
// the same user. Callers must pass exactly two distinct *User values;
// upstream (room-service capacity check + room-worker counterpart fetch)
// already enforces this invariant.
func BuildDMParticipants(a, b *User) (uids, accounts []string) {
	if a.ID < b.ID {
		return []string{a.ID, b.ID}, []string{a.Account, b.Account}
	}
	return []string{b.ID, a.ID}, []string{b.Account, a.Account}
}

// RenameRoomRequest is the canonical event for renaming a channel room.
// Clients send only NewName; RoomID and Account are derived from the request
// subject by room-service and Timestamp is stamped at acceptance — those three
// fields are server-set before the canonical event is published, never trusted
// from the wire body.
type RenameRoomRequest struct {
	RoomID    string `json:"roomId"    bson:"roomId"`
	NewName   string `json:"newName"   bson:"newName"`
	Account   string `json:"account"   bson:"account"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// RoomRestrictedRequest is the request body for the sync chat.server.> RPC
// that sets Restricted + ExternalAccess on a channel room. When
// Restricted=true and OwnerAccount is non-empty, that account becomes sole
// owner regardless of prior role. Account identifies the admin caller for
// audit / sys-message authorship.
type RoomRestrictedRequest struct {
	RoomID         string `json:"roomId"                 bson:"roomId"`
	Restricted     bool   `json:"restricted"             bson:"restricted"`
	ExternalAccess bool   `json:"externalAccess"         bson:"externalAccess"`
	OwnerAccount   string `json:"ownerAccount,omitempty" bson:"ownerAccount,omitempty"`
	Account        string `json:"account"                bson:"account"`
	Timestamp      int64  `json:"timestamp"              bson:"timestamp"`
}
