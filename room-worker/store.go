package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . SubscriptionStore

// UserWithMembership is the result of the GetUserWithMembership aggregation pipeline.
// It carries the target user along with a flag indicating whether an org-sourced
// membership covers them in the room, and the roles on their subscription — so
// the dual-membership branch in room-worker can demote owners without an extra
// database round trip.
type UserWithMembership struct {
	model.User       `bson:",inline"`
	HasOrgMembership bool         `bson:"hasOrgMembership"`
	Roles            []model.Role `bson:"roles"`
}

// OrgMemberStatus is one element returned by GetOrgMembersWithIndividualStatus.
type OrgMemberStatus struct {
	Account                 string `bson:"account"`
	SiteID                  string `bson:"siteId"`
	SectName                string `bson:"sectName"`
	HasIndividualMembership bool   `bson:"hasIndividualMembership"`
}

type SubscriptionStore interface {
	// --- existing methods (invite flow) ---
	CreateSubscription(ctx context.Context, sub *model.Subscription) error
	BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error
	ListByRoom(ctx context.Context, roomID string) ([]model.Subscription, error)
	// ReconcileUserCount sets rooms.userCount to the current subscription count
	// in one atomic $set. Idempotent under JetStream redelivery — unlike $inc,
	// repeated calls converge to the correct value even if an earlier write
	// succeeded but the ack was lost.
	ReconcileUserCount(ctx context.Context, roomID string) error
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetUser(ctx context.Context, account string) (*model.User, error)
	AddRole(ctx context.Context, account, roomID string, role model.Role) error
	RemoveRole(ctx context.Context, account, roomID string, role model.Role) error

	// --- aggregation pipelines (remove flow) ---
	GetUserWithMembership(ctx context.Context, roomID, account string) (*UserWithMembership, error)
	GetOrgMembersWithIndividualStatus(ctx context.Context, roomID, orgID string) ([]OrgMemberStatus, error)

	// --- write operations (remove flow) ---
	DeleteSubscription(ctx context.Context, roomID, account string) (int64, error)
	DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) (int64, error)
	DeleteRoomMember(ctx context.Context, roomID string, memberType model.RoomMemberType, memberID string) error

	// --- add-member flow ---
	CreateRoomMember(ctx context.Context, member *model.RoomMember) error
	BulkCreateRoomMembers(ctx context.Context, members []*model.RoomMember) error
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
	HasOrgRoomMembers(ctx context.Context, roomID string) (bool, error)
	GetSubscriptionAccounts(ctx context.Context, roomID string) ([]string, error)
	// ListNewMembers returns the unique, non-bot accounts that would be added
	// to roomID for a given (orgIDs, directAccounts) tuple — i.e. the union
	// minus already-subscribed accounts. Used by processAddMembers to expand
	// the room-service-supplied (orgs, users) into the actual write list.
	// Delegates to pkg/pipelines.GetNewMembersPipeline + a $group/$addToSet
	// terminal stage.
	ListNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]string, error)
}
