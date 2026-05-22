package model

import (
	"errors"
	"time"
)

// ErrSubscriptionNotFound is returned when a subscription lookup finds no matching document.
var ErrSubscriptionNotFound = errors.New("subscription not found")

type Role string

const (
	RoleOwner Role = "owner"
	// RoleAdmin is recognized by message-gatekeeper's large-room post bypass,
	// but is not yet assignable: room-service's role-update RPC still rejects
	// "admin" via errInvalidRole. Wiring assignment is owned separately.
	RoleAdmin  Role = "admin"
	RoleMember Role = "member"
)

type SubscriptionUser struct {
	ID      string `json:"id" bson:"_id"`
	Account string `json:"account" bson:"account"`
	IsBot   bool   `json:"isBot" bson:"isBot"`
}

type Subscription struct {
	ID                 string           `json:"id" bson:"_id"`
	User               SubscriptionUser `json:"u" bson:"u"`
	RoomID             string           `json:"roomId" bson:"roomId"`
	SiteID             string           `json:"siteId" bson:"siteId"`
	Roles              []Role           `json:"roles" bson:"roles"`
	Name               string           `json:"name"                    bson:"name"`
	RoomType           RoomType         `json:"roomType"                bson:"roomType"`
	IsSubscribed       bool             `json:"isSubscribed,omitempty"  bson:"isSubscribed,omitempty"`
	HistorySharedSince *time.Time       `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`
	JoinedAt           time.Time        `json:"joinedAt" bson:"joinedAt"`
	LastSeenAt         *time.Time       `json:"lastSeenAt,omitempty" bson:"lastSeenAt,omitempty"`
	HasMention         bool             `json:"hasMention" bson:"hasMention"`
	ThreadUnread       []string         `json:"threadUnread,omitempty" bson:"threadUnread,omitempty"`
	Alert              bool             `json:"alert" bson:"alert"`
	Muted              bool             `json:"muted" bson:"muted"`
}

// SubscriptionHRInfo carries the counterpart's HR-directory record on a
// DM subscription. Used to render the DM-room display label
// (engName + name) on the sidebar/header. All three fields are always
// populated when the parent pointer is present.
type SubscriptionHRInfo struct {
	Account string `json:"account" bson:"account"`
	Name    string `json:"name"    bson:"name"`
	EngName string `json:"engName" bson:"engName"`
}

// DMSubscription is the wire/storage shape for DM-type subscriptions:
// the base Subscription record plus the counterpart's HRInfo. The
// embedded pointer flattens at JSON marshal time, so a DMSubscription
// on the wire is a Subscription with one extra top-level `hrInfo` field.
//
// Backend emits this wrapper only for `RoomType == RoomTypeDM`
// subscriptions; channels, botDMs, and discussions ship plain
// Subscription (no hrInfo). Frontend mirrors this split in
// chat-frontend/src/api/types.ts.
type DMSubscription struct {
	*Subscription
	HRInfo *SubscriptionHRInfo `json:"hrInfo,omitempty" bson:"hrInfo,omitempty"`
}
