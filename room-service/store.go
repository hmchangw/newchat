package main

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

var (
	ErrUserNotFound       = errors.New("user not found")                        // GetUser: no matching account
	ErrAppNotFound        = errors.New("app not found")                         // GetApp: no matching bot account
	ErrRoomNotFound       = errors.New("room not found")                        // UpdateRoomVisibility: no matching room
	ErrOwnerNotSubscribed = errors.New("owner account is no longer subscribed") // ApplySubscriptionVisibility: owner left
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

type ReadReceiptRow struct {
	UserID      string `bson:"_id"`
	Account     string `bson:"account"`
	ChineseName string `bson:"chineseName"`
	EngName     string `bson:"engName"`
}

// RoomBotAppEntry pairs an assistant's bot account with its owning
// app name — the joined output of ListRoomBotApps.
type RoomBotAppEntry struct {
	AssistantName string `bson:"assistantName"`
	AppName       string `bson:"appName"`
}

type RoomStore interface {
	GetRoom(ctx context.Context, id string) (*model.Room, error)
	ListRoomsByIDs(ctx context.Context, ids []string) ([]model.Room, error)
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	// ListMemberStatuses returns up to `limit` members of roomID, each
	// projected from the corresponding users document as {account, engName,
	// chineseName, statusIsShow, statusText}. Subscriptions whose user
	// document is missing are dropped. Caller is responsible for the limit
	// cap (handler enforces > 0 and <= room.UserCount).
	ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error)
	// ListMentionableSubscriptions returns up to `limit` mentionable members
	// of roomID (users + apps), excluding excludeAccount, whose searchable
	// keyword matches escapedFilter (case-insensitive substring). escapedFilter
	// must already be regex-escaped (the handler runs regexp.QuoteMeta).
	// Empty escapedFilter matches everything.
	ListMentionableSubscriptions(ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int) ([]model.MentionableSubscription, error)
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
	// ListOrgMembers returns all users whose sectId OR deptId equals orgID,
	// projected as OrgMember rows sorted by account ascending. Returns a
	// RoomInvalidOrg-reason errcode when no users match (treated as "orgId is
	// not valid").
	ListOrgMembers(ctx context.Context, orgID string) ([]model.OrgMember, error)
	// FindExistingOrgIDs returns the subset of orgIDs that match at least
	// one user via sectId or deptId. Used by handleAddMembers and
	// handleCreateRoomChannel to reject requests carrying phantom org IDs
	// before they reach the canonical stream — without this gate the
	// worker would write a room_members row and fan out a "members added"
	// system message for an org with zero backing users.
	FindExistingOrgIDs(ctx context.Context, orgIDs []string) ([]string, error)
	// FindExistingAccounts returns the subset of accounts that have a
	// matching user document. Same shape and motivation as
	// FindExistingOrgIDs but at the user dimension — without this gate, a
	// typo'd or fake account in req.Users is silently dropped by the
	// candidates pipeline and the async job reports success despite the
	// requested user never being added.
	FindExistingAccounts(ctx context.Context, accounts []string) ([]string, error)
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Returns model.ErrSubscriptionNotFound
	// (wrapped) when no subscription matches.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
	// ToggleSubscriptionMute atomically flips muted via a single FindOneAndUpdate.
	// Returns the post-flip subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	ToggleSubscriptionMute(ctx context.Context, roomID, account string) (*model.Subscription, error)
	// ToggleSubscriptionFavorite atomically flips favorite via a single FindOneAndUpdate.
	// Returns the post-flip subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	ToggleSubscriptionFavorite(ctx context.Context, roomID, account string) (*model.Subscription, error)
	// SetOwnerRole atomically grants (makeOwner=true) or revokes (makeOwner=false)
	// the owner role on the subscription keyed by (roomID, account) via a single
	// FindOneAndUpdate. Other roles (e.g. member) are retained. Returns the
	// updated subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	SetOwnerRole(ctx context.Context, roomID, account string, makeOwner bool) (*model.Subscription, error)
	// GetUserSiteID returns the home site of a user looked up by account.
	// Returns ("", nil) when the user is not found locally; callers treat
	// that as "skip cross-site outbox".
	GetUserSiteID(ctx context.Context, account string) (string, error)
	// MinSubscriptionLastSeenByRoomID returns the room's strict read floor:
	// the MIN(lastSeenAt) across ALL of the room's subscriptions, but only
	// when every subscription has a usable lastSeenAt (> zero). Returns nil if
	// any subscription has never been read (missing/null/zero lastSeenAt) or if
	// the room has no subscriptions. Bots are counted, so a botDM room always
	// resolves to nil.
	MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error)
	// UpdateRoomMinUserLastSeenAt writes rooms.minUserLastSeenAt for roomID.
	// A nil value clears the field via $unset; a non-nil value writes via $set.
	UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error

	ListReadReceipts(ctx context.Context, roomID string, since time.Time, excludeAccount string, limit int) ([]ReadReceiptRow, error)

	// GetUser returns the user by account, or ErrUserNotFound.
	GetUser(ctx context.Context, account string) (*model.User, error)
	// GetApp returns the app whose Assistant.Name == botAccount, or ErrAppNotFound.
	GetApp(ctx context.Context, botAccount string) (*model.App, error)
	// ListDefaultChannelTabApps returns apps whose channelTab.enabled AND
	// channelTab.default are both true, sorted by channelTab.name asc.
	// Projection: _id, avatarUrl, assistant, channelTab. Empty result is
	// ([], nil).
	ListDefaultChannelTabApps(ctx context.Context) ([]model.App, error)
	// ListRoomBotApps returns one entry per bot subscribed to roomID,
	// joined with the owning app via assistant.name == u.account. Only
	// apps with assistant.enabled=true are emitted. Empty result is
	// ([], nil); result order is assistantName asc.
	ListRoomBotApps(ctx context.Context, roomID string) ([]RoomBotAppEntry, error)
	// ListActiveCmdMenus returns bot_cmd_menu documents where
	// activeStatus is true AND name IN assistantNames, sorted by name asc.
	// Returns ([], nil) when assistantNames is empty (skips the query).
	ListActiveCmdMenus(ctx context.Context, assistantNames []string) ([]model.BotCmdMenu, error)
	// FindDMSubscription returns the requester's existing dm/botDM sub with Name == targetName, filtered by RoomType.
	FindDMSubscription(ctx context.Context, account, targetName string) (*model.Subscription, error)

	// GetThreadSubscriptionByParent enforces (parentMessageID, account, roomID); the roomID
	// filter rejects a threadId that belongs to a different room than the request subject.
	GetThreadSubscriptionByParent(ctx context.Context, account, parentMessageID, roomID string) (*model.ThreadSubscription, error)

	// UpdateSubscriptionThreadRead overwrites threadUnread + alert; empty threadUnread is $unset.
	UpdateSubscriptionThreadRead(ctx context.Context, roomID, account string, threadUnread []string, alert bool) error

	UpdateThreadSubscriptionRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error

	// UpdateRoomVisibility sets rooms.{restricted, externalAccess, updatedAt}.
	// Returns ErrRoomNotFound when no room matches.
	UpdateRoomVisibility(ctx context.Context, roomID string, restricted, externalAccess bool) error
	// ApplySubscriptionVisibility writes the {restricted, externalAccess} denorm
	// flags to every subscription of the room. When restricted=true and
	// ownerAccount is non-empty, an aggregation-pipeline $cond also rewrites
	// roles so only ownerAccount holds RoleOwner. Returns ErrOwnerNotSubscribed
	// when ownerAccount has no active subscription in the room (the rewrite
	// would leave zero owners).
	ApplySubscriptionVisibility(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string) error
	// ListSubscriptionsByRoom returns every subscription in the room. Used to
	// drive cross-site outbox fan-out (one event per remote site).
	ListSubscriptionsByRoom(ctx context.Context, roomID string) ([]model.Subscription, error)
	// FindUsersByAccounts returns the User docs for the supplied accounts. Used
	// to bucket subscriptions by home site for cross-site fan-out.
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
}

// RoomKeyStore is the consumer-side interface for room encryption key lookups.
// Only the methods room-service needs are declared here.
type RoomKeyStore interface {
	GetMany(ctx context.Context, roomIDs []string) (map[string]*roomkeystore.VersionedKeyPair, error)
	// Get returns the current key for roomID, or (nil, nil) when absent.
	Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)
	// GetByVersion returns the key pair for the given (roomID, version). Returns
	// (nil, nil) when the version isn't held (rolled past the previous-key
	// grace window or never existed).
	GetByVersion(ctx context.Context, roomID string, version int) (*roomkeystore.RoomKeyPair, error)
	// Set writes a fresh keypair as the room's current key (version 0).
	Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)
}

// DEKProvisioner eagerly provisions a room's at-rest data encryption key at
// room-creation time. Satisfied by *atrest.Cipher; nil when ATREST_ENABLED=false.
type DEKProvisioner interface {
	EnsureDEK(ctx context.Context, roomID string) error
}

// MessageReader looks up a message by ID. found=false with err=nil means no row matched.
type MessageReader interface {
	GetMessageRoomAndCreatedAt(ctx context.Context, messageID string) (
		roomID string, createdAt time.Time, senderAccount string, found bool, err error,
	)
}
