package main

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// ErrUserNotFound is returned by GetUser when the account does not exist.
var ErrUserNotFound = errors.New("user not found")

var (
	ErrRoomNotFound       = errors.New("room not found")
	ErrNotChannelRoom     = errors.New("not a channel room")
	ErrOwnerNotSubscribed = errors.New("owner account is no longer subscribed")
)

//go:generate mockgen -destination=mock_store_test.go -package=main . SubscriptionStore,RoomKeyStore

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
	Name                    string `bson:"name"`
	TCName                  string `bson:"tcName"`
	IsDept                  bool   `bson:"isDept"`
	HasIndividualMembership bool   `bson:"hasIndividualMembership"`
	// HasOtherOrgMembership is true when the user is still reachable via
	// ANOTHER org row in the same room (one whose member.id matches the
	// user's sectId or deptId), excluding the org being removed.
	// processRemoveOrg uses this to avoid deleting subs of users who remain
	// covered by a sibling org — relevant since this PR's dept-aware match
	// makes the same user potentially reachable via two org rows
	// concurrently (sectId-org + deptId-org).
	HasOtherOrgMembership bool `bson:"hasOtherOrgMembership"`
}

// AddMemberCandidate is one element returned by ListAddMemberCandidates.
type AddMemberCandidate struct {
	Account                 string `bson:"account"`
	HasSubscription         bool   `bson:"hasSubscription"`
	HasIndividualRoomMember bool   `bson:"hasIndividualRoomMember"`
}

type SubscriptionStore interface {
	// BulkCreateSubscriptions upserts each sub keyed on (roomId, u.account)
	// via $setOnInsert; collisions (e.g. JetStream redelivery) are a Mongo
	// no-op so the persisted sub is preserved unchanged. Used by every
	// membership write path (channel, DM, botDM, add-member); the
	// re-subscribe semantic for botDM is owned by user-service.
	BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error
	// ReconcileMemberCounts recomputes Room.UserCount (non-bot subs) and
	// Room.AppCount (bot subs) via index-backed counts on the denormalized
	// u.isBot flag, then writes both back to the rooms collection in a single
	// update.
	ReconcileMemberCounts(ctx context.Context, roomID string) error
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetUser(ctx context.Context, account string) (*model.User, error)
	// FindDMSubscriptionPair returns both subs of a DM/botDM room in a
	// single query. The first return value is the sub owned by
	// requesterAccount, the second is the counterpart's. Returns
	// ErrSubscriptionNotFound if the room does not have exactly two
	// matching subs or if requesterAccount is not among them.
	FindDMSubscriptionPair(ctx context.Context, roomID, requesterAccount string) (*model.Subscription, *model.Subscription, error)
	RemoveRole(ctx context.Context, account, roomID string, role model.Role) error

	// --- aggregation pipelines (remove flow) ---
	GetUserWithMembership(ctx context.Context, roomID, account string) (*UserWithMembership, error)
	GetOrgMembersWithIndividualStatus(ctx context.Context, roomID, orgID string) ([]OrgMemberStatus, error)

	// --- write operations (remove flow) ---
	DeleteSubscription(ctx context.Context, roomID, account string) (int64, error)
	DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) (int64, error)
	DeleteRoomMember(ctx context.Context, roomID string, memberType model.RoomMemberType, memberID string) error

	// --- add-member flow ---
	BulkCreateRoomMembers(ctx context.Context, members []*model.RoomMember) error
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
	HasOrgRoomMembers(ctx context.Context, roomID string) (bool, error)
	GetSubscriptionAccounts(ctx context.Context, roomID string) ([]string, error)

	// ListAddMemberCandidates: per-user {hasSub, hasIndividualRow} flags so the worker splits into needSub vs needIRM (org→individual upgrade).
	ListAddMemberCandidates(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]AddMemberCandidate, error)

	// CreateRoom inserts the room doc. Returns mongo.ErrDuplicateKey
	// when the _id collides; the handler's idempotency logic handles
	// matching-existing-room as success-on-redelivery.
	CreateRoom(ctx context.Context, room *model.Room) error

	// ListNewMembersForNewRoom is the empty-roomID variant of the
	// ListAddMemberCandidates candidate resolution — same dedup + bot filter,
	// no "already-subscribed" pruning since the room doesn't exist yet.
	// excludeAccount drops one account from the candidate set; create-channel
	// passes the requester's account so they aren't materialized as a regular
	// member in addition to being added separately as the owner.
	ListNewMembersForNewRoom(ctx context.Context, orgIDs, accounts []string, excludeAccount string) ([]string, error)

	// Rename operations. (Restricted moved to room-service as a sync RPC.)

	// UpdateRoomName sets {name, updatedAt} on the channel-typed room doc.
	// Returns ErrRoomNotFound; ErrNotChannelRoom is no longer returned since
	// room-service validates type upstream.
	UpdateRoomName(ctx context.Context, roomID, newName string) error

	// UpdateSubscriptionNamesForRoom updateMany on subscriptions matching {roomId: roomID}.
	// Stamps nameUpdatedAt so the origin doc carries the same high-water mark the
	// federated rename event publishes (inbox-worker guards remote applies against it).
	UpdateSubscriptionNamesForRoom(ctx context.Context, roomID, newName string, nameUpdatedAt time.Time) error

	// ListByRoom returns all subscriptions for roomID across every site.
	// Used by the rename processor to bucket accounts by remote site for
	// outbox fan-out.
	ListByRoom(ctx context.Context, roomID string) ([]model.Subscription, error)
}

// Key store used by room-worker: reads for fan-out, writes for rotation.
type RoomKeyStore interface {
	Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)
	// Set writes a fresh keypair at version 0 — used when seeding a brand-new room.
	Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)
	// SetWithVersion writes pair at an explicit version. Used by the rotate
	// fallback when Rotate finds no current key but fan-out already committed
	// to predictedVersion = currentPair.Version + 1.
	SetWithVersion(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair, version int) error
	// Rotate atomically increments version and writes newPair as current.
	Rotate(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair) (int, error)
}

// DEKProvisioner eagerly provisions a room's at-rest data encryption key at
// creation time. Satisfied by *atrest.Cipher; nil when ATREST_ENABLED=false.
type DEKProvisioner interface {
	EnsureDEK(ctx context.Context, roomID string) error
}
