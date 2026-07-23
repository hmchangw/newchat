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
	ErrOwnerNotSubscribed = errors.New("owner account is no longer subscribed") // ApplySubscriptionRestriction: owner left
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

// RoomCounts is the result of CountMembersAndOwners. HumanCount excludes bot
// subs (u.isBot) so the last-member guard blocks removing the last human even
// when bots remain; docs missing the flag count as human.
type RoomCounts struct {
	MemberCount int
	HumanCount  int
	OwnerCount  int
}

// ThreadRoomInfoRow is one thread room's last-activity time, the result of
// GetThreadRoomInfos.
type ThreadRoomInfoRow struct {
	ThreadRoomID string
	LastMsgAt    time.Time
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
	// CheckMembership verifies (account, roomID) has a subscription without
	// decoding the document — an {_id:1}-projected existence check for the
	// many call sites that only need the membership gate, not the sub's fields.
	// Returns nil when subscribed, model.ErrSubscriptionNotFound (wrapped) when
	// not, or a wrapped infra error otherwise. Same error contract as
	// GetSubscription so callers branch identically on errors.Is.
	CheckMembership(ctx context.Context, account, roomID string) error
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
	// Used by addMembers and handleCreateRoomChannel for capacity validation.
	// Resolves candidates via pkg/pipelines.MatchCandidatesFilter, then (for a
	// non-empty roomID) subtracts already-subscribed accounts via an indexed read.
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
	// one user via sectId or deptId. Used by addMembers and
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
	// ToggleSubscriptionMute atomically flips muted via a single FindOneAndUpdate,
	// stamping muteUpdatedAt so the origin doc carries the same high-water mark the
	// federated event publishes (inbox-worker guards remote applies against it).
	// Returns the post-flip subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	ToggleSubscriptionMute(ctx context.Context, roomID, account string, muteUpdatedAt time.Time) (*model.Subscription, error)
	// ToggleSubscriptionFavorite atomically flips favorite via a single FindOneAndUpdate,
	// stamping favoriteUpdatedAt so the origin doc carries the same high-water mark the
	// federated event publishes (inbox-worker guards remote applies against it).
	// Returns the post-flip subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	ToggleSubscriptionFavorite(ctx context.Context, roomID, account string, favoriteUpdatedAt time.Time) (*model.Subscription, error)
	// SetOwnerRole atomically grants (makeOwner=true) or revokes (makeOwner=false)
	// the owner role on the subscription keyed by (roomID, account) via a single
	// FindOneAndUpdate. Other roles (e.g. member) are retained. Stamps rolesUpdatedAt
	// so the origin doc carries the same high-water mark the federated event publishes
	// (inbox-worker guards remote applies against it). Returns the updated
	// subscription, or model.ErrSubscriptionNotFound (wrapped) when no match.
	SetOwnerRole(ctx context.Context, roomID, account string, makeOwner bool, rolesUpdatedAt time.Time) (*model.Subscription, error)
	// GetUserSiteID returns the home site of a user looked up by account.
	// Returns ("", nil) when the user is not found locally; callers treat
	// that as "skip cross-site inbox".
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

	// ListThreadReadReceipts is the thread-scoped counterpart of ListReadReceipts:
	// readers are thread subscribers whose thread lastSeenAt passed the message,
	// not room members whose channel read-position did. Used for thread-only
	// replies, which never appear in the channel (see #443).
	ListThreadReadReceipts(ctx context.Context, threadRoomID string, since time.Time, excludeAccount string, limit int) ([]ReadReceiptRow, error)

	// GetUser returns the user by account, or ErrUserNotFound.
	GetUser(ctx context.Context, account string) (*model.User, error)
	// GetApp returns the app whose Assistant.Name == botAccount, or ErrAppNotFound.
	GetApp(ctx context.Context, botAccount string) (*model.App, error)
	// ListDefaultChannelTabApps returns apps whose channelTab.enabled AND
	// channelTab.default are both true, sorted by channelTab.name asc.
	// Projection: _id, assistant, channelTab. Empty result is
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

	// UpdateSubscriptionThreadRead atomically removes threadID from threadUnread and returns
	// the updated slice (nil when empty) and the updated alert flag.
	UpdateSubscriptionThreadRead(ctx context.Context, roomID, account, threadID string) (newThreadUnread []string, newAlert bool, err error)

	UpdateThreadSubscriptionRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error

	// ClearThreadSubscriptionsForAccount marks every one of account's thread
	// subscriptions on this site as read (lastSeenAt=now, updatedAt=now,
	// hasMention=false) in a single account-scoped bulk update. The cross-site
	// convergence rides one thread_read_all event, so no per-row snapshot is
	// returned.
	ClearThreadSubscriptionsForAccount(ctx context.Context, account string, now time.Time) error

	// ClearSubscriptionThreadUnreadForAccount clears thread-unread state on every
	// one of account's subscriptions that currently has unread threads: removes
	// threadUnread and sets alert=false. Subscriptions without unread threads are
	// left untouched so a non-thread alert source is preserved.
	ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error

	// GetThreadRoomByID returns the thread room document for threadRoomID.
	// Returns (nil, nil) when no document matches.
	GetThreadRoomByID(ctx context.Context, threadRoomID string) (*model.ThreadRoom, error)
	// MinThreadSubscriptionLastSeenByThreadRoomID returns the thread room's strict
	// read floor: MIN(lastSeenAt) across ALL thread_subscriptions for threadRoomID,
	// but only when every subscriber has a usable lastSeenAt (> zero). Returns nil
	// if any subscriber has never read or if there are no subscriptions.
	// Bots are counted as ordinary subscribers — a bot subscriber pins the floor to
	// nil since bots never call Mark Thread as Read.
	MinThreadSubscriptionLastSeenByThreadRoomID(ctx context.Context, threadRoomID string) (*time.Time, error)
	// UpdateThreadRoomMinUserLastSeenAt sets or clears thread_rooms.minUserLastSeenAt
	// for threadRoomID. A nil value clears the field via $unset; non-nil writes via $set.
	UpdateThreadRoomMinUserLastSeenAt(ctx context.Context, threadRoomID string, t *time.Time) error
	// GetThreadRoomInfos returns each existing thread room's lastMsgAt. Missing
	// thread rooms are omitted, not an error.
	GetThreadRoomInfos(ctx context.Context, threadRoomIDs []string) ([]ThreadRoomInfoRow, error)

	// UpdateRoomVisibility sets rooms.{restricted, externalAccess, updatedAt}.
	// Returns ErrRoomNotFound when no room matches.
	UpdateRoomVisibility(ctx context.Context, roomID string, restricted, externalAccess bool) error
	// ApplySubscriptionRestriction writes the {restricted, externalAccess} denorm
	// flags to every subscription of the room. When restricted=true and
	// ownerAccount is non-empty, an aggregation-pipeline $cond also rewrites
	// roles so only ownerAccount holds RoleOwner. Returns ErrOwnerNotSubscribed
	// when ownerAccount has no active subscription in the room (the rewrite
	// would leave zero owners). Stamps restrictUpdatedAt so the origin doc
	// carries the same high-water mark the federated event publishes (inbox-worker
	// guards remote applies against it).
	ApplySubscriptionRestriction(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string, restrictUpdatedAt time.Time) error
	// ListSubscriptionsByRoom returns every subscription in the room. Used to
	// drive cross-site inbox fan-out (one event per remote site).
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

// MessageReadMeta is the read-receipt-relevant metadata for a message.
// ThreadOnly marks a reply that lives only in a thread (threadParentId set,
// not tshow) — it never appears in the channel, so its readers must come from
// thread read-state, not the parent-room read-position (see #443).
type MessageReadMeta struct {
	RoomID       string
	CreatedAt    time.Time
	Sender       string
	ThreadRoomID string
	ThreadOnly   bool
}

// MessageReader looks up a message within a room. found=false with err=nil means
// no message matched the (account, roomID, messageID) tuple. The lookup is scoped
// to roomID so a wrong-room message resolves as not-found.
type MessageReader interface {
	GetMessageReadMeta(ctx context.Context, account, roomID, messageID string) (meta MessageReadMeta, found bool, err error)
}

// TeamsMeetingStore is the first-class idempotency record for a room's Teams
// meeting. It replaces the message-bucket marker scan with a dedicated Mongo
// document keyed unique on (roomId, siteId), mirroring the unique-index +
// IsDuplicateKeyError retry-safe-write convention room-service already uses for
// room_members and subscriptions (see store_mongo.go EnsureIndexes).
type TeamsMeetingStore interface {
	// GetTeamsMeeting fast-path reads the room's existing meeting record.
	// found=false with err=nil means the room has no meeting yet.
	GetTeamsMeeting(ctx context.Context, roomID, siteID string) (
		record *model.TeamsMeetingRecord, found bool, err error,
	)
	// InsertTeamsMeeting inserts the meeting record. The (roomId, siteId)
	// unique index makes this the idempotency gate: a concurrent second insert
	// returns a duplicate-key error (mongo.IsDuplicateKeyError), which the
	// handler treats as "a concurrent winner already wrote it" and reads back.
	InsertTeamsMeeting(ctx context.Context, record model.TeamsMeetingRecord) error
}
