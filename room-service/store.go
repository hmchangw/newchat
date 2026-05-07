package main

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

var (
	ErrUserNotFound = errors.New("user not found") // GetUser: no matching account
	ErrAppNotFound  = errors.New("app not found")  // GetApp: no matching bot account
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// SubscriptionWithMembership is the result of the GetSubscriptionWithMembership
// aggregation — the target's subscription joined with both the individual and
// org membership sources so the handler can decide whether the target is
// removable individually.
type SubscriptionWithMembership struct {
	Subscription            *model.Subscription
	HasIndividualMembership bool
	HasOrgMembership        bool
}

// RoomCounts is the result of CountMembersAndOwners — member and owner counts
// for a single room, computed in one aggregation.
type RoomCounts struct {
	MemberCount int
	OwnerCount  int
}

type RoomStore interface {
	CreateRoom(ctx context.Context, room *model.Room) error
	GetRoom(ctx context.Context, id string) (*model.Room, error)
	ListRooms(ctx context.Context) ([]model.Room, error)
	ListRoomsByIDs(ctx context.Context, ids []string) ([]model.Room, error)
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	CreateSubscription(ctx context.Context, sub *model.Subscription) error
	GetSubscriptionWithMembership(ctx context.Context, roomID, account string) (*SubscriptionWithMembership, error)
	CountMembersAndOwners(ctx context.Context, roomID string) (*RoomCounts, error)
	CountOwners(ctx context.Context, roomID string) (int, error)
	// CountNewMembers returns the count of unique, non-bot, not-already-subscribed users
	// that an add-members request would add to roomID for a given (orgIDs, directAccounts) tuple.
	// excludeAccount is empty string to disable, or an account that must be
	// dropped from the candidate set. create-channel passes the requester's
	// account so an org-expanded requester is not double-counted against the
	// cap (the requester is added separately as the owner).
	// Used by handleAddMembers and handleCreateRoomChannel for capacity validation.
	// Delegates to pkg/pipelines.GetNewMembersPipeline + a $count terminal stage.
	CountNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID, excludeAccount string) (int, error)
	// ListRoomMembers returns the members of roomID. When enrich=true, the
	// returned RoomMember.Member entries carry display fields populated via
	// $lookup stages against users and subscriptions. When enrich=false,
	// display fields are left zero.
	ListRoomMembers(ctx context.Context, roomID string, limit, offset *int, enrich bool) ([]model.RoomMember, error)
	// ListOrgMembers returns all users whose sectId equals orgID, projected
	// as OrgMember rows sorted by account ascending. Returns errInvalidOrg
	// when no users match (treated as "orgId is not valid").
	ListOrgMembers(ctx context.Context, orgID string) ([]model.OrgMember, error)
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Returns model.ErrSubscriptionNotFound
	// (wrapped) when no subscription matches.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
	// GetUserSiteID returns the home site of a user looked up by account.
	// Returns ("", nil) when the user is not found locally; callers treat
	// that as "skip cross-site outbox".
	GetUserSiteID(ctx context.Context, account string) (string, error)
	// MinSubscriptionLastSeenByRoomID returns the minimum lastSeenAt across
	// the room's subscriptions, considering only subscriptions that have a
	// non-nil, non-zero lastSeenAt. Subscriptions whose lastSeenAt has never
	// been written (e.g. the user was invited but has never opened the room)
	// are excluded entirely. Returns nil when no subscription has a usable
	// lastSeenAt.
	MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error)
	// UpdateRoomMinUserLastSeenAt writes rooms.minUserLastSeenAt for roomID.
	// A nil value clears the field via $unset; a non-nil value writes via $set.
	UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error

	// GetUser returns the user by account, or ErrUserNotFound.
	GetUser(ctx context.Context, account string) (*model.User, error)
	// GetApp returns the app whose Assistant.Name == botAccount, or ErrAppNotFound.
	GetApp(ctx context.Context, botAccount string) (*model.App, error)
	// FindDMSubscription returns the requester's existing dm/botDM sub with Name == targetName, filtered by RoomType.
	FindDMSubscription(ctx context.Context, account, targetName string) (*model.Subscription, error)
}

// RoomKeyStore is the consumer-side interface for room encryption key lookups.
// Only the methods room-service needs are declared here.
type RoomKeyStore interface {
	GetMany(ctx context.Context, roomIDs []string) (map[string]*roomkeystore.VersionedKeyPair, error)
}
