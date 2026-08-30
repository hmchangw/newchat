package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/orgdisplay"
	"github.com/hmchangw/chat/pkg/pipelines"
)

// botAccountRegex matches bot/app accounts by the ".bot" suffix only — all "p_"
// accounts have user records and resolve as users here.
const botAccountRegex = `\.bot$`

type MongoStore struct {
	rooms               *mongo.Collection
	subscriptions       *mongo.Collection
	threadSubscriptions *mongo.Collection
	threadRooms         *mongo.Collection
	roomMembers         *mongo.Collection
	users               *mongo.Collection
	apps                *mongo.Collection
	botCmdMenus         *mongo.Collection
	teamsMeetings       *mongo.Collection
	// *Secondary serve only staleness-tolerant display/list reads; every authz,
	// dedup, read-modify-write, and read-after-write read keeps the primary handle.
	roomsSecondary               *mongo.Collection
	subscriptionsSecondary       *mongo.Collection
	threadSubscriptionsSecondary *mongo.Collection
	threadRoomsSecondary         *mongo.Collection
	usersSecondary               *mongo.Collection
	appsSecondary                *mongo.Collection
	botCmdMenusSecondary         *mongo.Collection
}

// StoreOption customizes MongoStore construction.
type StoreOption func(*storeSettings)

type storeSettings struct {
	readPref *readpref.ReadPref
}

// WithReadPreference routes the store's staleness-tolerant reads to rp; a nil rp
// keeps every read on the default.
func WithReadPreference(rp *readpref.ReadPref) StoreOption {
	return func(s *storeSettings) { s.readPref = rp }
}

func NewMongoStore(db *mongo.Database, opts ...StoreOption) *MongoStore {
	var st storeSettings
	for _, o := range opts {
		o(&st)
	}
	secondary := func(coll *mongo.Collection) *mongo.Collection {
		return mongoutil.CollectionWithReadPreference(coll, st.readPref)
	}
	rooms := db.Collection("rooms")
	subscriptions := db.Collection("subscriptions")
	threadSubscriptions := db.Collection("thread_subscriptions")
	threadRooms := db.Collection("thread_rooms")
	users := db.Collection("users")
	apps := db.Collection("apps")
	botCmdMenus := db.Collection("bot_cmd_menu")
	return &MongoStore{
		rooms:               rooms,
		subscriptions:       subscriptions,
		threadSubscriptions: threadSubscriptions,
		threadRooms:         threadRooms,
		roomMembers:         db.Collection("room_members"),
		users:               users,
		apps:                apps,
		botCmdMenus:         botCmdMenus,
		teamsMeetings:       db.Collection("teams_meetings"),

		roomsSecondary:               secondary(rooms),
		subscriptionsSecondary:       secondary(subscriptions),
		threadSubscriptionsSecondary: secondary(threadSubscriptions),
		threadRoomsSecondary:         secondary(threadRooms),
		usersSecondary:               secondary(users),
		appsSecondary:                secondary(apps),
		botCmdMenusSecondary:         secondary(botCmdMenus),
	}
}

// EnsureIndexes creates the indexes that back the read paths in this service
// and the unique constraints required for retry-safe writes by room-worker.
// Must be invoked once at startup. Mongo treats index creation as idempotent
// when the key spec and options match.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	// Each index is attempted independently; a failure is collected (and surfaced via
	// the joined return) but never skips the rest — one transient error must not starve
	// a later unique/correctness index this service solely owns.
	var errs []error
	create := func(coll *mongo.Collection, name string, keys bson.D) {
		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys}); err != nil {
			errs = append(errs, fmt.Errorf("ensure %s index: %w", name, err))
		}
	}
	unique := func(coll *mongo.Collection, name string, keys bson.D) {
		if err := mongoutil.EnsureIndexWithRepair(ctx, coll,
			mongo.IndexModel{Keys: keys, Options: options.Index().SetUnique(true)}); err != nil {
			errs = append(errs, fmt.Errorf("ensure %s unique index: %w", name, err))
		}
	}

	create(s.roomMembers, "room_members (rid)", bson.D{{Key: "rid", Value: 1}})
	// Unique logical key — room-worker upserts room_members with $setOnInsert on
	// (rid, member.type, member.id), so a redelivered member.add no-ops instead of
	// inserting a duplicate under a fresh _id.
	unique(s.roomMembers, "room_members (rid,member.type,member.id)",
		bson.D{{Key: "rid", Value: 1}, {Key: "member.type", Value: 1}, {Key: "member.id", Value: 1}})
	// Unique logical key for subscriptions. Same retry-idempotency rationale.
	unique(s.subscriptions, "subscriptions (roomId,u.account)",
		bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}})
	// users.account + apps.assistant_name_idx are owned by user-service; verify + warn only.
	mongoutil.WarnMissingIndexes(ctx, s.users, "account_1")
	mongoutil.WarnMissingIndexes(ctx, s.apps, "assistant_name_idx")
	create(s.users, "users (sectId,account)", bson.D{{Key: "sectId", Value: 1}, {Key: "account", Value: 1}})
	create(s.users, "users (deptId,account)", bson.D{{Key: "deptId", Value: 1}, {Key: "account", Value: 1}})
	create(s.subscriptions, "subscriptions (roomId,lastSeenAt)", bson.D{{Key: "roomId", Value: 1}, {Key: "lastSeenAt", Value: 1}})
	// Backs ReconcileMemberCounts (bot vs non-bot counts) index-only.
	create(s.subscriptions, "subscriptions (roomId,u.isBot)", bson.D{{Key: "roomId", Value: 1}, {Key: "u.isBot", Value: 1}})
	// Lookup for FindDMSubscription (u.account+name); else a collection scan.
	create(s.subscriptions, "subscriptions (u.account,name)", bson.D{{Key: "u.account", Value: 1}, {Key: "name", Value: 1}})
	// Backs ComputeSectionOrder + section rebalance (filter u.account+sectionId, sort sectionOrder).
	create(s.subscriptions, "subscriptions (u.account,sectionId,sectionOrder)",
		bson.D{{Key: "u.account", Value: 1}, {Key: "sectionId", Value: 1}, {Key: "sectionOrder", Value: 1}})
	// Backs getRoomSubscriptions paginated {joinedAt,_id} sort from the index (avoids the 32MB sort limit).
	create(s.subscriptions, "subscriptions (roomId,joinedAt,_id)",
		bson.D{{Key: "roomId", Value: 1}, {Key: "joinedAt", Value: 1}, {Key: "_id", Value: 1}})
	// Backs CountOwners (roomId+roles) index-only.
	create(s.subscriptions, "subscriptions (roomId,roles)", bson.D{{Key: "roomId", Value: 1}, {Key: "roles", Value: 1}})
	// room-service owns the thread_subscriptions unique key. Create the canonical
	// (threadRoomId, userAccount) index FIRST — it has different keys from the legacy
	// (threadRoomId, userId) index, so they coexist — then drop the legacy only once
	// the replacement exists, so a failed create never leaves the collection without
	// either constraint. The collection exists post-create, so an absent legacy index
	// is IndexNotFound (a no-op); a NamespaceNotFound would mean an external drop and surfaces.
	if cerr := mongoutil.EnsureIndexWithRepair(ctx, s.threadSubscriptions, mongo.IndexModel{
		Keys:    bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); cerr != nil {
		errs = append(errs, fmt.Errorf("ensure thread_subscriptions (threadRoomId,userAccount) unique index: %w", cerr))
	} else if derr := s.threadSubscriptions.Indexes().DropOne(ctx, "threadRoomId_1_userId_1"); derr != nil && !mongoutil.IsIndexNotFound(derr) {
		errs = append(errs, fmt.Errorf("drop legacy thread_subscriptions (threadRoomId,userId) index: %w", derr))
	}
	create(s.threadSubscriptions, "thread_subscriptions (parentMessageId,userAccount)",
		bson.D{{Key: "parentMessageId", Value: 1}, {Key: "userAccount", Value: 1}})
	// Backs MinThreadSubscriptionLastSeenByThreadRoomID (covered seek on threadRoomId,lastSeenAt).
	create(s.threadSubscriptions, "thread_subscriptions (threadRoomId,lastSeenAt)",
		bson.D{{Key: "threadRoomId", Value: 1}, {Key: "lastSeenAt", Value: 1}})
	// Backs per-user, per-site lookups; no other thread_subscriptions index prefixes userAccount.
	create(s.threadSubscriptions, "thread_subscriptions (userAccount,siteId)",
		bson.D{{Key: "userAccount", Value: 1}, {Key: "siteId", Value: 1}})
	create(s.apps, "apps (channelTab.default,enabled,name)",
		bson.D{{Key: "channelTab.default", Value: 1}, {Key: "channelTab.enabled", Value: 1}, {Key: "channelTab.name", Value: 1}})
	create(s.botCmdMenus, "bot_cmd_menu (activeStatus,name)", bson.D{{Key: "activeStatus", Value: 1}, {Key: "name", Value: 1}})
	// Unique per-room idempotency record for the meetings RPC: a concurrent second
	// create reads back the winner instead of publishing a duplicate teams_meet_started.
	unique(s.teamsMeetings, "teams_meetings (roomId,siteId)", bson.D{{Key: "roomId", Value: 1}, {Key: "siteId", Value: 1}})

	return errors.Join(errs...)
}

// GetTeamsMeeting fast-path reads the room's existing Teams meeting record.
// found=false with err=nil means the room has no meeting yet.
func (s *MongoStore) GetTeamsMeeting(ctx context.Context, roomID, siteID string) (*model.TeamsMeetingRecord, bool, error) {
	var rec model.TeamsMeetingRecord
	err := s.teamsMeetings.FindOne(ctx, bson.M{"roomId": roomID, "siteId": siteID}).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get teams meeting for room %q: %w", roomID, err)
	}
	return &rec, true, nil
}

// InsertTeamsMeeting inserts the meeting record. The (roomId, siteId) unique
// index makes this the idempotency gate: a concurrent second insert returns a
// duplicate-key error the handler detects via mongo.IsDuplicateKeyError (which
// unwraps with errors.As) and reads back the winner's record.
func (s *MongoStore) InsertTeamsMeeting(ctx context.Context, record model.TeamsMeetingRecord) error {
	if _, err := s.teamsMeetings.InsertOne(ctx, record); err != nil {
		return fmt.Errorf("insert teams meeting record: %w", err)
	}
	return nil
}

// roomReadProjection is the field set GetRoom returns — the union of every
// Room field read by a handler call site. The full Room doc is never needed on
// these read paths, and projecting trims the BSON decode (a top CPU consumer in
// profiling) plus the wire payload. Keep in sync with the Room field reads in
// handler.go; the projection-field integration test guards drift.
var roomReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "type", Value: 1}, {Key: "name", Value: 1},
	{Key: "userCount", Value: 1}, {Key: "appCount", Value: 1},
	{Key: "restricted", Value: 1}, {Key: "externalAccess", Value: 1},
	{Key: "crossSite", Value: 1}, {Key: "crossSiteAt", Value: 1},
	{Key: "lastMsgAt", Value: 1}, {Key: "minUserLastSeenAt", Value: 1},
	{Key: "lastMentionAllAt", Value: 1},
}

// roomAppReadProjection: only the fields the app-read RPCs consume.
var roomAppReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "type", Value: 1}, {Key: "siteId", Value: 1},
}

// subscriptionReadProjection is the field set GetSubscription returns — the
// union of every Subscription field read by a handler call site. The fat
// Subscription doc (~30 fields incl. byte arrays and time pointers) is never
// needed here; projecting trims the reflection-heavy BSON decode. Keep in sync
// with the Subscription field reads in handler.go; the projection-field
// integration test guards drift.
var subscriptionReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "u", Value: 1}, {Key: "roomId", Value: 1},
	{Key: "siteId", Value: 1}, {Key: "roles", Value: 1}, {Key: "alert", Value: 1},
	{Key: "threadUnread", Value: 1}, {Key: "lastSeenAt", Value: 1},
	{Key: "historySharedSince", Value: 1},
}

func (s *MongoStore) GetRoom(ctx context.Context, id string) (*model.Room, error) {
	var room model.Room
	opts := options.FindOne().SetProjection(roomReadProjection)
	if err := s.rooms.FindOne(ctx, bson.M{"_id": id}, opts).Decode(&room); err != nil {
		return nil, fmt.Errorf("room %q not found: %w", id, err)
	}
	return &room, nil
}

func (s *MongoStore) GetRoomAppRead(ctx context.Context, id string) (*model.Room, error) {
	var room model.Room
	opts := options.FindOne().SetProjection(roomAppReadProjection)
	if err := s.rooms.FindOne(ctx, bson.M{"_id": id}, opts).Decode(&room); err != nil {
		return nil, fmt.Errorf("room %q not found: %w", id, err)
	}
	return &room, nil
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	opts := options.FindOne().SetProjection(subscriptionReadProjection)
	if err := s.subscriptions.FindOne(ctx, filter, opts).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
		}
		return nil, fmt.Errorf("get subscription for %q in room %q: %w", account, roomID, err)
	}
	return &sub, nil
}

// membershipExistsProjection returns only _id so the existence check decodes
// essentially nothing — the cheapest form of GetSubscription for call sites
// that use the result solely as a membership gate.
var membershipExistsProjection = bson.D{{Key: "_id", Value: 1}}

func (s *MongoStore) CheckMembership(ctx context.Context, account, roomID string) error {
	filter := bson.M{"u.account": account, "roomId": roomID}
	opts := options.FindOne().SetProjection(membershipExistsProjection)
	if err := s.subscriptions.FindOne(ctx, filter, opts).Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("%q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
		}
		return fmt.Errorf("check membership for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

// GetSubscriptionWithMembership loads the target subscription joined with their
// individual and org membership sources. Used by the remove-member validation
// flow to decide whether a user can leave or be removed individually.
func (s *MongoStore) GetSubscriptionWithMembership(ctx context.Context, roomID, account string) (*SubscriptionWithMembership, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID, "u.account": account}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$rid", roomID}},
					bson.M{"$eq": bson.A{"$member.type", "individual"}},
					bson.M{"$eq": bson.A{"$member.account", "$$acct"}},
				}}}},
				bson.M{"$limit": 1},
			},
			"as": "individualMembership",
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"sectId": 1, "deptId": 1}},
			},
			"as": "userDoc",
		}}},
		// Dept-aware org-membership lookup: a user added via Orgs:["X"] may
		// match the org by deptId only (no sectId), so the room_members row
		// has member.id = deptId. Checking only sectId would miss that case
		// and report HasOrgMembership=false, leading the remove flow to drop
		// the user's subscription even though they are still org-attached.
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let": bson.M{
				"sectId": bson.M{"$arrayElemAt": bson.A{"$userDoc.sectId", 0}},
				"deptId": bson.M{"$arrayElemAt": bson.A{"$userDoc.deptId", 0}},
			},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$rid", roomID}},
					bson.M{"$eq": bson.A{"$member.type", "org"}},
					bson.M{"$or": bson.A{
						bson.M{"$eq": bson.A{"$member.id", "$$sectId"}},
						bson.M{"$eq": bson.A{"$member.id", "$$deptId"}},
					}},
				}}}},
				bson.M{"$limit": 1},
			},
			"as": "orgMembership",
		}}},
		{{Key: "$addFields", Value: bson.M{
			"hasIndividualMembership": bson.M{"$gt": bson.A{bson.M{"$size": "$individualMembership"}, 0}},
			"hasOrgMembership":        bson.M{"$gt": bson.A{bson.M{"$size": "$orgMembership"}, 0}},
		}}},
		{{Key: "$project", Value: bson.M{"individualMembership": 0, "orgMembership": 0, "userDoc": 0}}},
	}

	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate subscription with membership: %w", err)
	}
	defer cursor.Close(ctx)

	var result struct {
		model.Subscription      `bson:",inline"`
		HasIndividualMembership bool `bson:"hasIndividualMembership"`
		HasOrgMembership        bool `bson:"hasOrgMembership"`
	}
	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return nil, fmt.Errorf("iterate subscription with membership: %w", err)
		}
		return nil, fmt.Errorf("subscription not found for account %q in room %q: %w", account, roomID, mongo.ErrNoDocuments)
	}
	if err := cursor.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode subscription with membership: %w", err)
	}
	sub := result.Subscription
	return &SubscriptionWithMembership{
		Subscription:            &sub,
		HasIndividualMembership: result.HasIndividualMembership,
		HasOrgMembership:        result.HasOrgMembership,
	}, nil
}

// CountMembersAndOwners returns the total and owner-role subscription counts
// for a room in a single aggregation, driving the last-owner and last-member
// guards in remove-member validation.
func (s *MongoStore) CountMembersAndOwners(ctx context.Context, roomID string) (*RoomCounts, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID}}},
		{{Key: "$facet", Value: bson.M{
			"members": bson.A{bson.M{"$count": "count"}},
			// Humans = not a bot AND not the platform-admin pseudo-account, so the
			// last-human guard is correct even for a legacy p_admin sub with isBot unset.
			"humans": bson.A{
				bson.M{"$match": bson.M{
					"u.isBot":   bson.M{"$ne": true},
					"u.account": bson.M{"$not": bson.Regex{Pattern: platformAdminRegex()}},
				}},
				bson.M{"$count": "count"},
			},
			"owners": bson.A{
				bson.M{"$match": bson.M{"roles": model.RoleOwner}},
				bson.M{"$count": "count"},
			},
		}}},
	}
	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate room counts: %w", err)
	}
	defer cursor.Close(ctx)

	var result struct {
		Members []struct {
			Count int `bson:"count"`
		} `bson:"members"`
		Humans []struct {
			Count int `bson:"count"`
		} `bson:"humans"`
		Owners []struct {
			Count int `bson:"count"`
		} `bson:"owners"`
	}
	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return nil, fmt.Errorf("iterate room counts: %w", err)
		}
		return &RoomCounts{}, nil
	}
	if err := cursor.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode room counts: %w", err)
	}
	counts := &RoomCounts{}
	if len(result.Members) > 0 {
		counts.MemberCount = result.Members[0].Count
	}
	if len(result.Humans) > 0 {
		counts.HumanCount = result.Humans[0].Count
	}
	if len(result.Owners) > 0 {
		counts.OwnerCount = result.Owners[0].Count
	}
	return counts, nil
}

func (s *MongoStore) ListRoomsByIDs(ctx context.Context, ids []string) ([]model.Room, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.roomsSecondary.Find(ctx, bson.M{"_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{
			"_id": 1, "siteId": 1, "name": 1, "userCount": 1, "appCount": 1,
			"lastMsgId": 1, "lastMsgAt": 1, "lastUserMsgAt": 1, "lastMentionAllAt": 1, "minUserLastSeenAt": 1, "crossSite": 1,
		}))
	if err != nil {
		return nil, fmt.Errorf("list rooms by ids: %w", err)
	}
	var rooms []model.Room
	if err := cursor.All(ctx, &rooms); err != nil {
		return nil, fmt.Errorf("list rooms by ids: decode: %w", err)
	}
	return rooms, nil
}

func (s *MongoStore) CountOwners(ctx context.Context, roomID string) (int, error) {
	count, err := s.subscriptions.CountDocuments(ctx, bson.M{"roomId": roomID, "roles": model.RoleOwner})
	if err != nil {
		return 0, fmt.Errorf("count owners for room %q: %w", roomID, err)
	}
	return int(count), nil
}

func (s *MongoStore) CountNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID, excludeAccount string) (int, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return 0, nil
	}
	filter := pipelines.MatchCandidatesFilter(orgIDs, directAccounts, excludeAccount)
	// Create path (no room yet): every resolved candidate is a new member, so a
	// plain indexed count suffices — there are no subscriptions to subtract.
	if roomID == "" {
		n, err := s.users.CountDocuments(ctx, filter)
		if err != nil {
			return 0, fmt.Errorf("count new members: %w", err)
		}
		return int(n), nil
	}
	// Add path: resolve candidate accounts, then subtract those already
	// subscribed via one indexed read scoped to the candidate set — instead of
	// a correlated-$lookup aggregation.
	cursor, err := s.users.Find(ctx, filter, options.Find().SetProjection(bson.M{"account": 1, "_id": 0}))
	if err != nil {
		return 0, fmt.Errorf("find candidate accounts: %w", err)
	}
	var rows []struct {
		Account string `bson:"account"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return 0, fmt.Errorf("decode candidate accounts: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	// account is unique in the users collection, so rows carry no duplicates.
	accounts := make([]string, len(rows))
	for i, r := range rows {
		accounts[i] = r.Account
	}
	subbed, err := pipelines.SubscribedAccounts(ctx, s.subscriptions, roomID, accounts)
	if err != nil {
		return 0, fmt.Errorf("resolve subscribed accounts: %w", err)
	}
	// subbed is a subset of the candidate accounts (the $in query bounds it), so
	// the new-member count is just the candidates minus those already subscribed.
	return len(accounts) - len(subbed), nil
}

// ListRoomMembers returns the members of a room. It prefers the room_members
// collection. When no room_members document exists for roomID, it falls back
// to synthesizing RoomMember entries from the subscriptions collection so
// callers always see the same response shape. Sort: orgs first, then
// individuals, each group by ts ascending with _id tiebreaker.
func (s *MongoStore) ListRoomMembers(ctx context.Context, roomID string, limit, offset *int, enrich bool) ([]model.RoomMember, error) {
	// Lightweight existence probe — project only _id to minimize payload.
	err := s.roomMembers.FindOne(ctx, bson.M{"rid": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
	switch {
	case err == nil:
		return s.getRoomMembers(ctx, roomID, limit, offset, enrich)
	case errors.Is(err, mongo.ErrNoDocuments):
		return s.getRoomSubscriptions(ctx, roomID, limit, offset, enrich)
	default:
		return nil, fmt.Errorf("probe room_members for %q: %w", roomID, err)
	}
}

func (s *MongoStore) getRoomMembers(ctx context.Context, roomID string, limit, offset *int, enrich bool) ([]model.RoomMember, error) {
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{"rid": roomID}}},
		bson.D{{Key: "$addFields", Value: bson.M{
			"typeOrder": bson.M{"$cond": bson.A{
				bson.M{"$eq": bson.A{"$member.type", "org"}}, 0, 1,
			}},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{
			{Key: "typeOrder", Value: 1},
			{Key: "ts", Value: 1},
			{Key: "_id", Value: 1},
		}}},
	}
	if offset != nil && *offset > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$skip", Value: int64(*offset)}})
	}
	// Mongo rejects {$limit: 0}; the handler guards against <=0 but we
	// defend here too so the store is robust to direct internal callers.
	if limit != nil && *limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: int64(*limit)}})
	}

	if enrich {
		pipeline = append(pipeline, enrichRoomMembersStages(roomID)...)
	}

	// Drop the helper typeOrder field last so it never leaks into the result.
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.M{"typeOrder": 0}}})

	cursor, err := s.roomMembers.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate room_members for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)

	if !enrich {
		members := []model.RoomMember{}
		if err := cursor.All(ctx, &members); err != nil {
			return nil, fmt.Errorf("decode room_members for %q: %w", roomID, err)
		}
		return members, nil
	}

	// Enriched path: decode into a hybrid row type that carries a parallel
	// `display` sub-document (the aggregation writes individual-member values
	// there to sidestep the bson:"-" tags on RoomMemberEntry's display fields).
	// Org-member display (sectName, memberCount) is resolved separately in a
	// single index-backed batch below — see attachOrgDisplay — rather than via a
	// per-row correlated $lookup that would force a users collection scan per row.
	var rows []roomMemberEnrichedRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode enriched room_members for %q: %w", roomID, err)
	}
	members := make([]model.RoomMember, len(rows))
	var orgIDs, botAccounts []string
	for i := range rows {
		rm := rows[i].RoomMember
		d := rows[i].Display
		rm.Member.EngName = d.EngName
		rm.Member.ChineseName = d.ChineseName
		rm.Member.IsOwner = d.IsOwner
		if rm.Member.Type == model.RoomMemberOrg {
			orgIDs = append(orgIDs, rm.Member.ID)
		} else {
			rm.Member.SectName = d.SectName
			rm.Member.EmployeeID = d.EmployeeID
			// room_members rows carry no isBot flag, so the suffix is the only signal.
			if model.IsBot(rm.Member.Account) {
				botAccounts = append(botAccounts, rm.Member.Account)
			}
		}
		members[i] = rm
	}
	if len(orgIDs) > 0 {
		if err := s.attachOrgDisplay(ctx, roomID, members, orgIDs); err != nil {
			return nil, err
		}
	}
	if err := s.attachAppNames(ctx, roomID, members, botAccounts); err != nil {
		return nil, err
	}
	return members, nil
}

// attachOrgDisplay resolves org-member display names and member counts for the
// org rows in members, then fills SectName (dept-first tiebreak) and MemberCount
// in place. It mirrors attachUserDisplayNames but for the org dimension: a
// single index-backed batch query feeds a Go-side rollup, replacing the prior
// per-row correlated $lookup whose $expr $or could not use an index.
func (s *MongoStore) attachOrgDisplay(ctx context.Context, roomID string, members []model.RoomMember, orgIDs []string) error {
	users, err := pipelines.OrgDisplayUsers(ctx, s.users, orgIDs)
	if err != nil {
		return fmt.Errorf("attach org display for %q: %w", roomID, err)
	}
	agg := orgdisplay.Build(orgIDs, users)
	for i := range members {
		if members[i].Member.Type != model.RoomMemberOrg {
			continue
		}
		id := members[i].Member.ID
		if a := agg[id]; a != nil {
			members[i].Member.MemberCount = a.MemberCount
		}
		members[i].Member.OrgName = orgdisplay.Name(agg[id], id)
		members[i].Member.OrgCode = orgdisplay.Code(agg[id])
		members[i].Member.OrgDescription = orgdisplay.Description(agg[id])
	}
	return nil
}

// roomMemberEnrichedRow is the decode target for the enriched aggregation
// pipeline. It carries the standard RoomMember plus a parallel `display`
// sub-document populated by enrichment stages. This exists because
// RoomMemberEntry's display fields are tagged bson:"-" for persistence
// safety — the pipeline therefore writes enrichment values to a separate
// field that has normal bson tags, and Go-side post-processing copies
// them onto RoomMemberEntry.
type roomMemberEnrichedRow struct {
	model.RoomMember `bson:",inline"`
	Display          roomMemberEnrichedDisplay `bson:"display"`
}

type roomMemberEnrichedDisplay struct {
	EngName     string `bson:"engName,omitempty"`
	ChineseName string `bson:"chineseName,omitempty"`
	IsOwner     bool   `bson:"isOwner,omitempty"`
	SectName    string `bson:"sectName,omitempty"`
	EmployeeID  string `bson:"employeeId,omitempty"`
}

// enrichRoomMembersStages returns the $lookup + $set stages appended to the
// room_members aggregation when enrich=true. These enrich INDIVIDUAL members
// only (engName/chineseName/isOwner) via account-keyed, index-backed lookups.
// Org-member display is resolved separately by attachOrgDisplay — see there for
// why it is not a pipeline $lookup. Enrichment output is written into a
// `display` sub-document so it survives the RoomMemberEntry bson:"-" tags.
func enrichRoomMembersStages(roomID string) []bson.D {
	return []bson.D{
		// Individuals: join users on account → pull engName / chineseName.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let": bson.M{
				"acct": "$member.account",
				"mtyp": "$member.type",
			},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$$mtyp", "individual"}},
					bson.M{"$eq": bson.A{"$account", "$$acct"}},
				}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"engName": 1, "chineseName": 1, "sectName": 1, "employeeId": 1, "_id": 0}},
			},
			"as": "_userMatch",
		}}},
		// Individuals: join subscriptions on (roomId, u.account) → pull roles.
		{{Key: "$lookup", Value: bson.M{
			"from": "subscriptions",
			"let": bson.M{
				"acct": "$member.account",
				"mtyp": "$member.type",
			},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$$mtyp", "individual"}},
					bson.M{"$eq": bson.A{"$roomId", roomID}},
					bson.M{"$eq": bson.A{"$u.account", "$$acct"}},
				}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"roles": 1, "_id": 0}},
			},
			"as": "_subMatch",
		}}},
		// Fold the individual matches into a single `display` sub-document.
		{{Key: "$set", Value: bson.M{
			"display": bson.M{
				"engName":     bson.M{"$arrayElemAt": bson.A{"$_userMatch.engName", 0}},
				"chineseName": bson.M{"$arrayElemAt": bson.A{"$_userMatch.chineseName", 0}},
				"sectName":    bson.M{"$arrayElemAt": bson.A{"$_userMatch.sectName", 0}},
				"employeeId":  bson.M{"$arrayElemAt": bson.A{"$_userMatch.employeeId", 0}},
				"isOwner": bson.M{"$in": bson.A{
					"owner",
					bson.M{"$ifNull": bson.A{
						bson.M{"$arrayElemAt": bson.A{"$_subMatch.roles", 0}},
						bson.A{},
					}},
				}},
			},
		}}},
		// Drop the temporary join arrays.
		{{Key: "$project", Value: bson.M{"_userMatch": 0, "_subMatch": 0}}},
	}
}

// The nested path is u._id, not u.id — SubscriptionUser.ID is bson "_id".
var roomMemberSubProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "u._id", Value: 1}, {Key: "u.account", Value: 1},
	{Key: "u.isBot", Value: 1},
	{Key: "roles", Value: 1}, {Key: "joinedAt", Value: 1},
}

func (s *MongoStore) getRoomSubscriptions(ctx context.Context, roomID string, limit, offset *int, enrich bool) ([]model.RoomMember, error) {
	opts := options.Find().SetSort(bson.D{
		{Key: "joinedAt", Value: 1},
		{Key: "_id", Value: 1},
	})
	if offset != nil && *offset > 0 {
		opts.SetSkip(int64(*offset))
	}
	// SetLimit(0) means "no limit" in the driver, which would silently return
	// unbounded results. Only set when >0 so it matches the aggregation path.
	if limit != nil && *limit > 0 {
		opts.SetLimit(int64(*limit))
	}
	opts.SetProjection(roomMemberSubProjection)
	cursor, err := s.subscriptions.Find(ctx, bson.M{"roomId": roomID}, opts)
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)

	var subs []model.Subscription
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode subscriptions for %q: %w", roomID, err)
	}

	members := make([]model.RoomMember, 0, len(subs))
	var humanAccounts, botAccounts []string
	for i := range subs {
		sub := &subs[i]
		entry := model.RoomMemberEntry{
			ID:      sub.User.ID,
			Type:    model.RoomMemberIndividual,
			Account: sub.User.Account,
		}
		if enrich {
			entry.IsOwner = hasRole(sub.Roles, model.RoleOwner)
			if acct := sub.User.Account; acct != "" {
				// The flag is the authority; the suffix covers subs written before it existed.
				if sub.User.IsBot || model.IsBot(acct) {
					botAccounts = append(botAccounts, acct)
				} else {
					humanAccounts = append(humanAccounts, acct)
				}
			}
		}
		members = append(members, model.RoomMember{
			ID:     sub.ID,
			RoomID: roomID,
			Ts:     sub.JoinedAt,
			Member: entry,
		})
	}

	if enrich && len(members) > 0 {
		if err := s.attachUserDisplayNames(ctx, roomID, members, humanAccounts, botAccounts); err != nil {
			return nil, err
		}
	}
	return members, nil
}

// attachUserDisplayNames batch-loads display fields for the individual members
// in the slice and copies them on in place. Humans resolve from users, bots
// from apps; each query fires only when its partition is non-empty. Callers
// partition the accounts because the two ListRoomMembers paths detect bots
// differently.
func (s *MongoStore) attachUserDisplayNames(ctx context.Context, roomID string, members []model.RoomMember, humanAccounts, botAccounts []string) error {
	if len(humanAccounts) > 0 {
		userByAccount, err := s.findUsersForDisplay(ctx, humanAccounts)
		if err != nil {
			return fmt.Errorf("find users for room %q: %w", roomID, err)
		}
		for i := range members {
			if members[i].Member.Type != model.RoomMemberIndividual {
				continue
			}
			if u, ok := userByAccount[members[i].Member.Account]; ok {
				members[i].Member.EngName = u.EngName
				members[i].Member.ChineseName = u.ChineseName
				members[i].Member.SectName = u.SectName
				members[i].Member.EmployeeID = u.EmployeeID
			}
		}
	}
	return s.attachAppNames(ctx, roomID, members, botAccounts)
}

// attachAppNames resolves bot members' app display names in one indexed batch
// and fills AppName in place. Org members carry no account, so they never key
// into the result.
func (s *MongoStore) attachAppNames(ctx context.Context, roomID string, members []model.RoomMember, botAccounts []string) error {
	if len(botAccounts) == 0 {
		return nil
	}
	appByAssistant, err := s.findAppsForDisplay(ctx, botAccounts)
	if err != nil {
		return fmt.Errorf("find apps for room %q: %w", roomID, err)
	}
	for i := range members {
		if name, ok := appByAssistant[members[i].Member.Account]; ok {
			members[i].Member.AppName = name
		}
	}
	return nil
}

// findUsersForDisplay returns engName/chineseName indexed by account
// for every users document matching one of accounts. The existing
// users.account index covers the $in filter.
func (s *MongoStore) findUsersForDisplay(ctx context.Context, accounts []string) (map[string]*model.User, error) {
	cursor, err := s.users.Find(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "account": 1, "engName": 1, "chineseName": 1, "sectName": 1, "employeeId": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find users for display: %w", err)
	}
	defer cursor.Close(ctx)

	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users for display: %w", err)
	}
	out := make(map[string]*model.User, len(users))
	for i := range users {
		out[users[i].Account] = &users[i]
	}
	return out, nil
}

// findAppsForDisplay returns app.name indexed by assistant.name for
// every apps document whose assistant.name matches one of botAccounts.
// The existing apps (assistant.name) index covers the $in filter.
func (s *MongoStore) findAppsForDisplay(ctx context.Context, botAccounts []string) (map[string]string, error) {
	cursor, err := s.apps.Find(ctx,
		bson.M{"assistant.name": bson.M{"$in": botAccounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "name": 1, "assistant.name": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find apps for display: %w", err)
	}
	defer cursor.Close(ctx)

	type row struct {
		Name      string `bson:"name"`
		Assistant struct {
			Name string `bson:"name"`
		} `bson:"assistant"`
	}
	var rows []row
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode apps for display: %w", err)
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Assistant.Name] = r.Name
	}
	return out, nil
}

// Keeps users.services (bcrypt) off this path. Omits active, so a caller
// adding an IsActive check must add it back — nil reads as active.
var userReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "account", Value: 1},
	{Key: "engName", Value: 1}, {Key: "chineseName", Value: 1},
	{Key: "roles", Value: 1},
}

func (s *MongoStore) GetUser(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	opts := options.FindOne().SetProjection(userReadProjection)
	err := s.users.FindOne(ctx, bson.M{"account": account}, opts).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", account, err)
	}
	return &u, nil
}

// Both call sites read only Assistant.Enabled.
var appAssistantReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "assistant", Value: 1},
}

func (s *MongoStore) GetApp(ctx context.Context, botAccount string) (*model.App, error) {
	var a model.App
	opts := options.FindOne().SetProjection(appAssistantReadProjection)
	err := s.apps.FindOne(ctx, bson.M{"assistant.name": botAccount}, opts).Decode(&a)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrAppNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get app for bot %q: %w", botAccount, err)
	}
	return &a, nil
}

// The open-or-create dedup check returns only the existing RoomID.
var dmDedupProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "roomId", Value: 1},
}

func (s *MongoStore) FindDMSubscription(ctx context.Context, account, targetName string) (*model.Subscription, error) {
	var sub model.Subscription
	opts := options.FindOne().SetProjection(dmDedupProjection)
	err := s.subscriptions.FindOne(ctx, bson.M{
		"u.account": account,
		"name":      targetName,
		"roomType":  bson.M{"$in": []model.RoomType{model.RoomTypeDM, model.RoomTypeBotDM}},
	}, opts).Decode(&sub)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, model.ErrSubscriptionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find dm subscription: %w", err)
	}
	return &sub, nil
}

// ListOrgMembers returns all users whose sectId OR deptId equals orgID,
// projected as OrgMember rows sorted by account ascending. The dept branch
// is symmetric to the membership-lookup pipelines (GetSubscriptionWithMembership,
// GetUserWithMembership): an org added by a dept-only match stores
// member.id = deptId in room_members, so the expansion RPC must look up
// users by deptId too. Both (sectId, account) and (deptId, account) indexes
// exist (see ensureIndexes) so the $or stays index-backed. Returns a
// RoomInvalidOrg-reason errcode when neither branch matches any users.
func (s *MongoStore) ListOrgMembers(ctx context.Context, orgID string) ([]model.OrgMember, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "account", Value: 1}}).
		SetProjection(bson.M{
			"_id":         1,
			"account":     1,
			"engName":     1,
			"chineseName": 1,
			"siteId":      1,
		})
	cursor, err := s.usersSecondary.Find(ctx, bson.M{"$or": []bson.M{
		{"sectId": orgID},
		{"deptId": orgID},
	}}, opts)
	if err != nil {
		return nil, fmt.Errorf("find users for org %q: %w", orgID, err)
	}
	defer cursor.Close(ctx)

	var members []model.OrgMember
	if err := cursor.All(ctx, &members); err != nil {
		return nil, fmt.Errorf("decode users for org %q: %w", orgID, err)
	}
	if len(members) == 0 {
		return nil, errcode.BadRequest(fmt.Sprintf("list org members for %q", orgID), errcode.WithReason(errcode.RoomInvalidOrg))
	}
	return members, nil
}

// FindExistingOrgIDs returns the subset of orgIDs that match at least one
// user via sectId or deptId. Two parallel distinct calls — one on each
// indexed field — keep the query covered by the (sectId, account) and
// (deptId, account) compound indexes; the result of each distinct is
// bounded by len(orgIDs) since the filter is an $in on the same field.
//
// A single $unionWith aggregation was tried (one round-trip instead of
// two) and benchmarked ~8.5% faster end-to-end with the same index
// coverage, but the aggregation form is more complex, ships ~55% more
// Go-side allocations per call, and shifts behavior onto Mongo's
// aggregation framework (slightly different optimizations across
// versions, more surface area in a sharded future). The two-Distinct
// form is simpler, version-agnostic from at least Mongo 4.4 onward, and
// the perf delta is not material at this call rate. Keep it simple.
func (s *MongoStore) FindExistingOrgIDs(ctx context.Context, orgIDs []string) ([]string, error) {
	if len(orgIDs) == 0 {
		return nil, nil
	}
	var sectIDs []string
	if err := s.users.Distinct(ctx, "sectId", bson.M{"sectId": bson.M{"$in": orgIDs}}).Decode(&sectIDs); err != nil {
		return nil, fmt.Errorf("distinct sectIds for org validation: %w", err)
	}
	var deptIDs []string
	if err := s.users.Distinct(ctx, "deptId", bson.M{"deptId": bson.M{"$in": orgIDs}}).Decode(&deptIDs); err != nil {
		return nil, fmt.Errorf("distinct deptIds for org validation: %w", err)
	}
	out := make([]string, 0, len(sectIDs)+len(deptIDs))
	seen := make(map[string]struct{}, len(sectIDs)+len(deptIDs))
	for _, id := range sectIDs {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	for _, id := range deptIDs {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, nil
}

// FindExistingAccounts returns the subset of accounts that have a matching
// user document. Distinct on the indexed `account` field keeps the result
// bounded by len(accounts) regardless of how many users share an org.
func (s *MongoStore) FindExistingAccounts(ctx context.Context, accounts []string) ([]string, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	var out []string
	if err := s.users.Distinct(ctx, "account", bson.M{"account": bson.M{"$in": accounts}}).Decode(&out); err != nil {
		return nil, fmt.Errorf("distinct accounts for user validation: %w", err)
	}
	return out, nil
}

// UpdateSubscriptionRead sets lastSeenAt on the subscription keyed by
// (roomID, account), clearing alert and hasMention, and returns the post-update
// unread-thread count. FindOneAndUpdate rather than UpdateOne: the same single
// round trip also yields the post-state the badge hook needs.
// Returns model.ErrSubscriptionNotFound when no subscription matches.
func (s *MongoStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) (int, error) {
	var out struct {
		ThreadUnread []string `bson:"threadUnread"`
	}
	opts := options.FindOneAndUpdate().
		SetProjection(bson.D{{Key: "threadUnread", Value: 1}}).
		SetReturnDocument(options.After)
	err := s.subscriptions.FindOneAndUpdate(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": false, "hasMention": false}},
		opts,
	).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return len(out.ThreadUnread), nil
}

// ToggleSubscriptionMute atomically flips muted via FindOneAndUpdate.
// findOneAndUpdateSub applies an aggregation-pipeline $set to the subscription
// keyed by (roomID, account) and returns the post-update document. op names the
// operation for error wrapping; mongo.ErrNoDocuments maps to
// model.ErrSubscriptionNotFound.
func (s *MongoStore) findOneAndUpdateSub(ctx context.Context, roomID, account, op string, set bson.M) (*model.Subscription, error) {
	filter := bson.M{"roomId": roomID, "u.account": account}
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: set}}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var result model.Subscription
	if err := s.subscriptions.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s for %q in room %q: %w", op, account, roomID, model.ErrSubscriptionNotFound)
		}
		return nil, fmt.Errorf("%s for %q in room %q: %w", op, account, roomID, err)
	}
	return &result, nil
}

// ToggleSubscriptionMute flips muted. $ifNull treats an absent field as false so
// legacy docs toggle to true on first call. muteUpdatedAt is stamped from the same
// instant the caller publishes as the event timestamp, keeping the origin doc and
// every federated replica on one high-water mark.
func (s *MongoStore) ToggleSubscriptionMute(ctx context.Context, roomID, account string, muteUpdatedAt time.Time) (*model.Subscription, error) {
	return s.findOneAndUpdateSub(ctx, roomID, account, "toggle mute", bson.M{
		"muted":         bson.M{"$not": bson.A{bson.M{"$ifNull": bson.A{"$muted", false}}}},
		"muteUpdatedAt": muteUpdatedAt,
	})
}

// ToggleSubscriptionFavorite flips favorite. $ifNull treats an absent field as
// false so legacy docs toggle to true on first call. favoriteUpdatedAt is stamped
// from the same instant the caller publishes as the event timestamp, keeping the
// origin doc and every federated replica on one high-water mark. Toggling OFF also
// clears sectionId/sectionOrder when they were "favorites" (moveChat's manual
// section membership follows the flag); toggling ON leaves sectionId alone.
func (s *MongoStore) ToggleSubscriptionFavorite(ctx context.Context, roomID, account string, favoriteUpdatedAt time.Time) (*model.Subscription, error) {
	turningOff := bson.M{"$and": bson.A{
		bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$favorite", false}}, true}},
		bson.M{"$eq": bson.A{"$sectionId", model.SectionFavorites}},
	}}
	return s.findOneAndUpdateSub(ctx, roomID, account, "toggle favorite", bson.M{
		"favorite":          bson.M{"$not": bson.A{bson.M{"$ifNull": bson.A{"$favorite", false}}}},
		"favoriteUpdatedAt": favoriteUpdatedAt,
		"sectionId":         bson.M{"$cond": bson.A{turningOff, "$$REMOVE", "$sectionId"}},
		"sectionOrder":      bson.M{"$cond": bson.A{turningOff, "$$REMOVE", "$sectionOrder"}},
	})
}

// MoveSubscriptionSection sets sectionId+sectionOrder (sectionID != nil) or clears
// both (sectionID == nil, a remove) on one subscription, stamping sectionUpdatedAt
// as the high-water mark. Classic update (not the $set pipeline) so a remove can
// $unset. favorite is mirrored alongside: true only when sectionID == "favorites",
// false otherwise (a remove un-favorites too) — see model.SectionFavorites.
// Returns the post-write sub or model.ErrSubscriptionNotFound (wrapped).
func (s *MongoStore) MoveSubscriptionSection(ctx context.Context, roomID, account string, sectionID *string, order float64, sectionUpdatedAt time.Time) (*model.Subscription, error) {
	var update bson.M
	if sectionID == nil {
		update = bson.M{
			"$set":   bson.M{"sectionUpdatedAt": sectionUpdatedAt, "favorite": false, "favoriteUpdatedAt": sectionUpdatedAt},
			"$unset": bson.M{"sectionId": "", "sectionOrder": ""},
		}
	} else {
		update = bson.M{"$set": bson.M{
			"sectionId":         *sectionID,
			"sectionOrder":      order,
			"sectionUpdatedAt":  sectionUpdatedAt,
			"favorite":          *sectionID == model.SectionFavorites,
			"favoriteUpdatedAt": sectionUpdatedAt,
		}}
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var result model.Subscription
	err := s.subscriptions.FindOneAndUpdate(ctx, bson.M{"roomId": roomID, "u.account": account}, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("move section for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
		}
		return nil, fmt.Errorf("move section for %q in room %q: %w", account, roomID, err)
	}
	return &result, nil
}

// ComputeSectionOrder midpoints a new position within (account, sectionID). Two
// small indexed reads in the placed case (the afterRoomID sub, then the next
// higher order); one in the append case (the section max). Both afterRoomID and
// beforeRoomID empty ⇒ append at the section end (max order + 1).
func (s *MongoStore) ComputeSectionOrder(ctx context.Context, account, sectionID, afterRoomID, beforeRoomID string) (float64, bool, error) {
	secFilter := bson.M{"u.account": account, "sectionId": sectionID}
	if beforeRoomID != "" {
		// Place just before beforeRoomID (top-insertion) — mirror of the after
		// case. Next = beforeRoomID's order; Prev = the largest order strictly
		// less than it. Stale ref → append; no prev (it's the head) → next-1.
		var before model.Subscription
		err := s.subscriptions.FindOne(ctx, bson.M{"roomId": beforeRoomID, "u.account": account, "sectionId": sectionID},
			options.FindOne().SetProjection(bson.M{"roomId": 1, "sectionOrder": 1})).Decode(&before)
		if errors.Is(err, mongo.ErrNoDocuments) {
			max, mErr := s.sectionOrderExtreme(ctx, secFilter, -1)
			if mErr != nil {
				return 0, false, mErr
			}
			return max + 1, false, nil
		}
		if err != nil {
			return 0, false, fmt.Errorf("lookup beforeRoomID order: %w", err)
		}
		next := before.SectionOrder
		prevFilter := bson.M{"u.account": account, "sectionId": sectionID, "sectionOrder": bson.M{"$lt": next}}
		var prev model.Subscription
		err = s.subscriptions.FindOne(ctx, prevFilter, options.FindOne().SetSort(bson.D{{Key: "sectionOrder", Value: -1}}).SetProjection(bson.M{"sectionOrder": 1})).Decode(&prev)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return next - 1, false, nil // head
		}
		if err != nil {
			return 0, false, fmt.Errorf("lookup prev order: %w", err)
		}
		mid, exhausted := sectionMidpoint(prev.SectionOrder, next)
		return mid, exhausted, nil
	}
	if afterRoomID == "" {
		max, err := s.sectionOrderExtreme(ctx, secFilter, -1)
		if err != nil {
			return 0, false, err
		}
		return max + 1, false, nil
	}
	// Prev = afterRoomID's own order. If it isn't in this section (stale client),
	// fall back to append rather than midpoint against nothing.
	var after model.Subscription
	err := s.subscriptions.FindOne(ctx, bson.M{"roomId": afterRoomID, "u.account": account, "sectionId": sectionID},
		options.FindOne().SetProjection(bson.M{"roomId": 1, "sectionOrder": 1})).Decode(&after)
	if errors.Is(err, mongo.ErrNoDocuments) {
		max, mErr := s.sectionOrderExtreme(ctx, secFilter, -1)
		if mErr != nil {
			return 0, false, mErr
		}
		return max + 1, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("lookup afterRoomID order: %w", err)
	}
	prev := after.SectionOrder
	// Next = the smallest order strictly greater than prev in the section.
	nextFilter := bson.M{"u.account": account, "sectionId": sectionID, "sectionOrder": bson.M{"$gt": prev}}
	var next model.Subscription
	err = s.subscriptions.FindOne(ctx, nextFilter, options.FindOne().SetSort(bson.D{{Key: "sectionOrder", Value: 1}}).SetProjection(bson.M{"sectionOrder": 1})).Decode(&next)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return prev + 1, false, nil // tail
	}
	if err != nil {
		return 0, false, fmt.Errorf("lookup next order: %w", err)
	}
	mid, exhausted := sectionMidpoint(prev, next.SectionOrder)
	return mid, exhausted, nil
}

// sectionMidpoint returns the value strictly between prev and next, flagging
// exhausted when float precision has collapsed the gap (mid landed on a neighbor)
// so the caller rebalances first. Pure — the unit-tested core of the order math.
func sectionMidpoint(prev, next float64) (float64, bool) {
	mid := (prev + next) / 2
	return mid, mid <= prev || mid >= next
}

// sectionOrderExtreme returns the min (dir=1) or max (dir=-1) sectionOrder in the
// filtered section, or 0 when the section is empty.
func (s *MongoStore) sectionOrderExtreme(ctx context.Context, filter bson.M, dir int) (float64, error) {
	var sub model.Subscription
	err := s.subscriptions.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "sectionOrder", Value: dir}}).SetProjection(bson.M{"sectionOrder": 1})).Decode(&sub)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("section order extreme: %w", err)
	}
	return sub.SectionOrder, nil
}

// RebalanceSection re-spaces every (account) sub in sectionID to 1,2,3,… by
// current order. Bounded to one section, run only when a gap exhausts float
// precision. O(section-size) load + bulk write; a section rarely gets
// large enough to matter, and it happens only on the precision edge.
// Returns the full post-rebalance doc for every rewritten row (sectionUpdatedAt
// stamped to now) so the caller can fan those out — every sibling's order
// changed, not just the one being moved.
func (s *MongoStore) RebalanceSection(ctx context.Context, account, sectionID string, now time.Time) ([]model.Subscription, error) {
	cur, err := s.subscriptions.Find(ctx,
		bson.M{"u.account": account, "sectionId": sectionID},
		options.Find().SetSort(bson.D{{Key: "sectionOrder", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("rebalance load section: %w", err)
	}
	var subs []model.Subscription
	if err := cur.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("rebalance decode section: %w", err)
	}
	if len(subs) == 0 {
		return nil, nil
	}
	models := make([]mongo.WriteModel, 0, len(subs))
	for i := range subs {
		// Space by 10, not 1, so later midpoint inserts rarely exhaust the gap and re-rebalance.
		subs[i].SectionOrder = float64((i + 1) * 10)
		subs[i].SectionUpdatedAt = &now
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": subs[i].RoomID, "u.account": account}).
			SetUpdate(bson.M{"$set": bson.M{"sectionOrder": subs[i].SectionOrder, "sectionUpdatedAt": now}}))
	}
	if _, err := s.subscriptions.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return nil, fmt.Errorf("rebalance bulk write: %w", err)
	}
	return subs, nil
}

// OpenSubscription sets open=true. Set-not-toggle: no ordering guard is needed
// because applying true repeatedly or out of order always converges to true.
func (s *MongoStore) OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error) {
	return s.findOneAndUpdateSub(ctx, roomID, account, "open subscription", bson.M{
		"open": true,
	})
}

// SetOwnerRole atomically grants or revokes the owner role, returning the updated
// subscription. Promote appends "owner" only when absent; demote filters "owner"
// out. Any other roles (e.g. "member") are preserved and array order stays stable.
// rolesUpdatedAt is stamped from the same instant the caller publishes as the event
// timestamp, keeping the origin doc and every federated replica on one high-water mark.
func (s *MongoStore) SetOwnerRole(ctx context.Context, roomID, account string, makeOwner bool, rolesUpdatedAt time.Time) (*model.Subscription, error) {
	currentRoles := bson.M{"$ifNull": bson.A{"$roles", bson.A{}}}
	var rolesExpr bson.M
	if makeOwner {
		rolesExpr = bson.M{"$cond": bson.M{
			"if":   bson.M{"$in": bson.A{model.RoleOwner, currentRoles}},
			"then": currentRoles,
			"else": bson.M{"$concatArrays": bson.A{currentRoles, bson.A{model.RoleOwner}}},
		}}
	} else {
		// Remove owner, then ensure member is still present. Mirrors the worker's
		// old "AddRole(member) before RemoveRole(owner)" guard so a channel creator
		// (seeded roles ["owner"] only) demotes to ["member"], never an empty array.
		withoutOwner := bson.M{"$filter": bson.M{
			"input": currentRoles,
			"cond":  bson.M{"$ne": bson.A{"$$this", model.RoleOwner}},
		}}
		rolesExpr = bson.M{"$cond": bson.M{
			"if":   bson.M{"$in": bson.A{model.RoleMember, withoutOwner}},
			"then": withoutOwner,
			"else": bson.M{"$concatArrays": bson.A{withoutOwner, bson.A{model.RoleMember}}},
		}}
	}
	return s.findOneAndUpdateSub(ctx, roomID, account, "set owner role", bson.M{
		"roles":          rolesExpr,
		"rolesUpdatedAt": rolesUpdatedAt,
	})
}

// GetUserSiteID looks up users.siteId by account. Returns ("", nil) if no
// user document exists.
func (s *MongoStore) GetUserSiteID(ctx context.Context, account string) (string, error) {
	var doc struct {
		SiteID string `bson:"siteId"`
	}
	err := s.usersSecondary.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"siteId": 1, "_id": 0})).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", fmt.Errorf("get user siteId for %q: %w", account, err)
	}
	return doc.SiteID, nil
}

// Read-state queries exclude bots and the platform-admin pseudo-account via
// platformAdminRegex() / botOrPlatformAdminRegex() (helper.go). Plain `p_` QA
// accounts are ordinary users and DO hold the read floor.

// MinSubscriptionLastSeenByRoomID returns the room's strict read floor: the
// minimum lastSeenAt across the room's ordinary-member subscriptions, but only
// when EVERY such subscription has a usable lastSeenAt (> zero). If any has no
// usable lastSeenAt — missing, null, or the BSON zero date, i.e. a member who
// was invited but has never opened the room — it returns nil, meaning "not
// everyone has read yet". It also returns nil for a room with no subscriptions.
// Bots (u.isBot) and the platform-admin pseudo-account (platformAdminRegex()) are
// excluded, so a passive bot/admin never freezes the floor and a botDM resolves to
// the human's lastSeenAt. A room with only excluded subscriptions resolves to nil.
// The caller $unsets rooms.minUserLastSeenAt on a nil result.
func (s *MongoStore) MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error) {
	// The whole result is determined by a single document: the room's ordinary
	// (non-bot, non-admin) subscription with the smallest lastSeenAt. The
	// (roomId, lastSeenAt) index
	// (non-sparse, so missing fields are indexed as null) returns the room's
	// subscriptions in ascending lastSeenAt order, and BSON sorts missing/null
	// before the legacy zero date before real dates. So the first document by
	// ascending lastSeenAt answers both questions at once:
	//   - smallest value is missing/null/zero → at least one member has never
	//     read → strict floor is nil ("not everyone has read yet");
	//   - smallest value is a real post-zero date → every member has read and
	//     that value IS the minimum → the floor.
	// The (roomId, lastSeenAt) index still serves the sort; the u.isBot and
	// u.account predicates are applied as residual filters (bots/admin are few per
	// room), so this stays a bounded index seek on the message-read hot path
	// rather than the prior full-room $group scan.
	var doc struct {
		LastSeenAt time.Time `bson:"lastSeenAt"`
	}
	err := s.subscriptions.FindOne(ctx,
		// u.isBot excludes bots; the u.account $not predicate additionally excludes
		// the platform-admin pseudo-account whose isBot may be unset (cross-site
		// mirror subs).
		bson.M{"roomId": roomID, "u.isBot": bson.M{"$ne": true}, "u.account": bson.M{"$not": bson.Regex{Pattern: platformAdminRegex()}}},
		options.FindOne().
			SetSort(bson.D{{Key: "lastSeenAt", Value: 1}}).
			SetProjection(bson.M{"lastSeenAt": 1, "_id": 0}),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil // no subscriptions in the room
	}
	if err != nil {
		return nil, fmt.Errorf("find min lastSeenAt for room %q: %w", roomID, err)
	}
	// $gt-zeroTime equivalent: missing/null/zero decodes to the zero time and
	// counts as "never read", matching the previous aggregation's definition.
	if !doc.LastSeenAt.After(time.Time{}) {
		return nil, nil
	}
	minTime := doc.LastSeenAt
	return &minTime, nil
}

// UpdateRoomMinUserLastSeenAt sets or clears rooms.minUserLastSeenAt for roomID.
func (s *MongoStore) UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error {
	var update bson.M
	if t == nil {
		update = bson.M{"$unset": bson.M{"minUserLastSeenAt": ""}}
	} else {
		update = bson.M{"$set": bson.M{"minUserLastSeenAt": *t}}
	}
	if _, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, update); err != nil {
		return fmt.Errorf("update minUserLastSeenAt for room %q: %w", roomID, err)
	}
	return nil
}

func (s *MongoStore) ListReadReceipts(
	ctx context.Context,
	roomID string,
	since time.Time,
	excludeAccount string,
	limit int,
) ([]ReadReceiptRow, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"roomId":     roomID,
			"lastSeenAt": bson.M{"$gte": since},
			// Bots (u.isBot) and the platform-admin pseudo-account (platformAdminRegex())
			// are never surfaced as readers, in addition to excluding the sender.
			"u.account": bson.M{"$ne": excludeAccount, "$not": bson.Regex{Pattern: platformAdminRegex()}},
			"u.isBot":   bson.M{"$ne": true},
		}}},
		// $lookup justification: joins each reader's users doc for display fields
		// (name), keyed by the indexed _id and bounded by the $limit below;
		// app-side composition would be one extra round-trip per reader.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"uid": "$u._id"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": []any{"$_id", "$$uid"}}}},
				bson.M{"$project": bson.M{"_id": 1, "account": 1, "chineseName": 1, "engName": 1}},
			},
			"as": "user",
		}}},
		{{Key: "$unwind", Value: bson.M{
			"path":                       "$user",
			"preserveNullAndEmptyArrays": false,
		}}},
		{{Key: "$replaceWith", Value: "$user"}},
		{{Key: "$limit", Value: int64(limit)}},
	}
	cursor, err := s.subscriptionsSecondary.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate read receipts for room %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)

	rows := make([]ReadReceiptRow, 0)
	for cursor.Next(ctx) {
		var r ReadReceiptRow
		if err := cursor.Decode(&r); err != nil {
			return nil, fmt.Errorf("decode read-receipt row for room %q: %w", roomID, err)
		}
		rows = append(rows, r)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate read receipts for room %q: %w", roomID, err)
	}
	return rows, nil
}

// ListThreadReadReceipts mirrors ListReadReceipts over thread_subscriptions:
// readers are thread subscribers whose thread lastSeenAt passed the message.
// thread_subscriptions store userAccount/userId flat (no embedded "u" doc), so
// the match and the users $lookup key off those fields directly.
func (s *MongoStore) ListThreadReadReceipts(
	ctx context.Context,
	threadRoomID string,
	since time.Time,
	excludeAccount string,
	limit int,
) ([]ReadReceiptRow, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"threadRoomId": threadRoomID,
			"lastSeenAt":   bson.M{"$gte": since},
			// Bot and platform-admin subscribers are never surfaced as readers, in
			// addition to excluding the sender.
			"userAccount": bson.M{"$ne": excludeAccount, "$not": bson.Regex{Pattern: botOrPlatformAdminRegex()}},
		}}},
		// $lookup justification: joins each reader's users doc for display fields
		// (name), keyed by the indexed _id and bounded by the $limit below;
		// app-side composition would be one extra round-trip per reader.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"uid": "$userId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": []any{"$_id", "$$uid"}}}},
				bson.M{"$project": bson.M{"_id": 1, "account": 1, "chineseName": 1, "engName": 1}},
			},
			"as": "user",
		}}},
		{{Key: "$unwind", Value: bson.M{
			"path":                       "$user",
			"preserveNullAndEmptyArrays": false,
		}}},
		{{Key: "$replaceWith", Value: "$user"}},
		{{Key: "$limit", Value: int64(limit)}},
	}
	cursor, err := s.threadSubscriptionsSecondary.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate thread read receipts for thread room %q: %w", threadRoomID, err)
	}
	defer cursor.Close(ctx)

	rows := make([]ReadReceiptRow, 0)
	for cursor.Next(ctx) {
		var r ReadReceiptRow
		if err := cursor.Decode(&r); err != nil {
			return nil, fmt.Errorf("decode thread read-receipt row for thread room %q: %w", threadRoomID, err)
		}
		rows = append(rows, r)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread read receipts for thread room %q: %w", threadRoomID, err)
	}
	return rows, nil
}

// messageThreadRead reads only ThreadRoomID; the rest is the not-found sentinel.
var threadSubParentProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "threadRoomId", Value: 1},
}

func (s *MongoStore) GetThreadSubscriptionByParent(ctx context.Context, account, parentMessageID, roomID string) (*model.ThreadSubscription, error) {
	var ts model.ThreadSubscription
	opts := options.FindOne().SetProjection(threadSubParentProjection)
	err := s.threadSubscriptions.FindOne(ctx, bson.M{
		"parentMessageId": parentMessageID,
		"userAccount":     account,
		"roomId":          roomID,
	}, opts).Decode(&ts)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find thread subscription for %q parent %q in room %q: %w",
				account, parentMessageID, roomID, model.ErrThreadSubscriptionNotFound)
		}
		return nil, fmt.Errorf("find thread subscription for %q parent %q in room %q: %w",
			account, parentMessageID, roomID, err)
	}
	return &ts, nil
}

// ListDefaultChannelTabApps returns apps whose channelTab.enabled AND
// channelTab.default are both true, sorted by channelTab.name asc.
// Projection: _id, assistant, channelTab. Empty result is ([], nil).
func (s *MongoStore) ListDefaultChannelTabApps(ctx context.Context) ([]model.App, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "channelTab.name", Value: 1}}).
		SetProjection(bson.M{
			"_id":        1,
			"assistant":  1,
			"channelTab": 1,
		})
	cursor, err := s.appsSecondary.Find(ctx, bson.M{
		"channelTab.enabled": true,
		"channelTab.default": true,
	}, opts)
	if err != nil {
		return nil, fmt.Errorf("list default channel-tab apps: %w", err)
	}
	defer cursor.Close(ctx)
	apps := make([]model.App, 0, 8)
	if err := cursor.All(ctx, &apps); err != nil {
		return nil, fmt.Errorf("decode default channel-tab apps: %w", err)
	}
	return apps, nil
}

// ListRoomBotApps returns one entry per bot subscribed to roomID, joined with
// the owning app via assistant.name == u.account. Only apps with
// assistant.enabled=true are emitted. Empty result is ([], nil); result order
// is assistantName asc.
func (s *MongoStore) ListRoomBotApps(ctx context.Context, roomID string) ([]RoomBotAppEntry, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID, "u.isBot": true}}},
		// $lookup justification: resolves each bot subscription's apps doc (assistant
		// metadata) keyed by the indexed account; the room's bot set is tiny and
		// app-side composition would be one extra query per bot.
		{{Key: "$lookup", Value: bson.M{
			"from": "apps",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$assistant.enabled", true}},
					bson.M{"$eq": bson.A{"$assistant.name", "$$acct"}},
				}}}},
				bson.M{"$project": bson.M{
					"_id":           0,
					"assistantName": "$assistant.name",
					"appName":       "$name",
				}},
			},
			"as": "app",
		}}},
		{{Key: "$unwind", Value: "$app"}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$app"}}},
		{{Key: "$sort", Value: bson.D{{Key: "assistantName", Value: 1}}}},
	}
	cursor, err := s.subscriptionsSecondary.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("list room bot apps for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	entries := make([]RoomBotAppEntry, 0, 4)
	if err := cursor.All(ctx, &entries); err != nil {
		return nil, fmt.Errorf("decode room bot apps for %q: %w", roomID, err)
	}
	return entries, nil
}

// ListActiveCmdMenus returns bot_cmd_menu documents where activeStatus is true
// AND name IN assistantNames, sorted by name asc. Returns ([], nil) when
// assistantNames is empty (skips the query).
func (s *MongoStore) ListActiveCmdMenus(ctx context.Context, assistantNames []string) ([]model.BotCmdMenu, error) {
	if len(assistantNames) == 0 {
		return []model.BotCmdMenu{}, nil
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetProjection(bson.M{
			"_id":       0,
			"name":      1,
			"cmdBlocks": 1,
		})
	cursor, err := s.botCmdMenusSecondary.Find(ctx, bson.M{
		"activeStatus": true,
		"name":         bson.M{"$in": assistantNames},
	}, opts)
	if err != nil {
		return nil, fmt.Errorf("list active cmd menus: %w", err)
	}
	defer cursor.Close(ctx)
	menus := make([]model.BotCmdMenu, 0, len(assistantNames))
	if err := cursor.All(ctx, &menus); err != nil {
		return nil, fmt.Errorf("decode active cmd menus: %w", err)
	}
	return menus, nil
}

// ListMemberStatuses returns up to limit members of roomID (projected from the joined
// user doc); orphan subs, empty-statusText members, and members with statusIsShow=false
// (opted out of surfacing their status) are dropped before the post-join $limit.
func (s *MongoStore) ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID}}},
		// Join on u.account → users.account (the account-indexed majority pattern
		// here, as in GetSubscriptionWithMembership/enrichRoomMembersStages), not
		// the u._id → users._id join ListReadReceipts uses. account is not a unique
		// index, so the inner $limit 1 caps a duplicate-account match to one doc.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id":          0,
					"account":      1,
					"engName":      1,
					"chineseName":  1,
					"statusIsShow": 1,
					"statusText":   1,
				}},
			},
			"as": "user",
		}}},
		{{Key: "$unwind", Value: bson.M{"path": "$user", "preserveNullAndEmptyArrays": false}}},
		{{Key: "$replaceWith", Value: "$user"}},
		// Exclude members who aren't surfacing a status: an empty statusText is not
		// a presence to show, and statusIsShow=false (incl. legacy docs missing the
		// field, which decode as false) means the user opted out of surfacing it.
		{{Key: "$match", Value: bson.M{
			"statusText":   bson.M{"$nin": bson.A{"", nil}},
			"statusIsShow": true,
		}}},
		{{Key: "$limit", Value: int64(limit)}},
	}
	cursor, err := s.subscriptionsSecondary.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate member statuses for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	members := []model.MemberStatus{}
	if err := cursor.All(ctx, &members); err != nil {
		return nil, fmt.Errorf("decode member statuses for %q: %w", roomID, err)
	}
	return members, nil
}

// ListMentionableSubscriptions returns up to `limit` mentionable members of
// roomID whose dash-joined keyword (account, engName, chineseName, app.name,
// app.assistant.name) matches escapedFilter under case-insensitive regex.
// excludeAccount is dropped at the $match stage so the caller never sees
// themselves. The platform-admin pseudo-account (platformAdminRegex) is also
// dropped — it is not mentionable.
// `.bot` accounts classify as `app` and emit a non-nil App + empty SiteID;
// human accounts classify as `user` with a non-nil HRInfo. Orphan rows
// (bot sub with no apps doc, or human sub with no users doc) return empty
// strings rather than null leaves so the wire shape is well-typed.
func (s *MongoStore) ListMentionableSubscriptions(
	ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int,
) ([]model.MentionableSubscription, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"roomId": roomID,
			"u.account": bson.M{
				"$ne":  excludeAccount,
				"$not": bson.M{"$regex": platformAdminRegex()},
			},
		}}},
		// $lookup justification: joins each candidate's users doc for mention display
		// fields (name/site), keyed by the indexed account and capped at $limit 1;
		// app-side composition would be one extra query per mentionable.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id": 0, "account": 1, "engName": 1, "chineseName": 1, "siteId": 1,
				}},
			},
			"as": "_users",
		}}},
		// $lookup justification: joins the bot's apps doc for mention display, keyed
		// by the indexed assistant.name and capped at $limit 1; app-side composition
		// would be one extra query per mentionable bot.
		{{Key: "$lookup", Value: bson.M{
			"from": "apps",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$assistant.name", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id": 0, "name": 1, "assistant.name": 1,
				}},
			},
			"as": "_apps",
		}}},
		{{Key: "$addFields", Value: bson.M{
			"isApp":   bson.M{"$regexMatch": bson.M{"input": "$u.account", "regex": botAccountRegex}},
			"userDoc": bson.M{"$arrayElemAt": bson.A{"$_users", 0}},
			"appDoc":  bson.M{"$arrayElemAt": bson.A{"$_apps", 0}},
		}}},
		{{Key: "$addFields", Value: bson.M{
			"keyword": bson.M{"$concat": bson.A{
				bson.M{"$ifNull": bson.A{"$u.account", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$appDoc.name", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
			}},
		}}},
		{{Key: "$match", Value: bson.M{
			"keyword": bson.M{"$regex": escapedFilter, "$options": "i"},
		}}},
		{{Key: "$limit", Value: int64(limit)}},
		{{Key: "$project", Value: bson.M{
			"_id":        0,
			"optionType": bson.M{"$cond": bson.A{"$isApp", "app", "user"}},
			"userId":     "$u._id",
			"account":    "$u.account",
			"siteId": bson.M{"$cond": bson.A{
				"$isApp",
				"",
				bson.M{"$ifNull": bson.A{"$userDoc.siteId", ""}},
			}},
			"hrInfo": bson.M{"$cond": bson.A{
				"$isApp",
				"$$REMOVE",
				bson.M{
					"engName":     bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}},
					"chineseName": bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}},
				},
			}},
			"app": bson.M{"$cond": bson.A{
				"$isApp",
				bson.M{
					"name": bson.M{"$ifNull": bson.A{"$appDoc.name", ""}},
					"assistant": bson.M{
						"name": bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
					},
				},
				"$$REMOVE",
			}},
		}}},
	}

	cursor, err := s.subscriptionsSecondary.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate mentionable subscriptions for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	subs := []model.MentionableSubscription{}
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode mentionable subscriptions for %q: %w", roomID, err)
	}
	return subs, nil
}

// No order-safety guard on the source-site write; the $lt guard lives on the inbox-worker side.
func (s *MongoStore) UpdateThreadSubscriptionRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error {
	filter := bson.M{"threadRoomId": threadRoomID, "userAccount": account}
	update := bson.M{"$set": bson.M{
		"lastSeenAt": lastSeenAt,
		"updatedAt":  lastSeenAt,
		"hasMention": false,
	}}
	res, err := s.threadSubscriptions.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update thread subscription read for %q in thread room %q: %w",
			account, threadRoomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update thread subscription read for %q in thread room %q: %w",
			account, threadRoomID, model.ErrThreadSubscriptionNotFound)
	}
	return nil
}

// UpdateSubscriptionThreadRead removes threadID from threadUnread via a single
// $pull and returns the resulting array (nil when empty). A drained array stays
// stored as [] — omitempty keeps it off the wire and every reader checks
// len > 0, so a second $unset round-trip buys nothing. Missing subscription →
// ErrSubscriptionNotFound.
func (s *MongoStore) UpdateSubscriptionThreadRead(ctx context.Context, roomID, account, threadID string) ([]string, error) {
	filter := bson.M{"roomId": roomID, "u.account": account}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updated model.Subscription
	err := s.subscriptions.FindOneAndUpdate(ctx, filter,
		bson.M{"$pull": bson.M{"threadUnread": threadID}}, opts).Decode(&updated)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("update subscription thread-read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("update subscription thread-read for %q in room %q: %w", account, roomID, err)
	}
	if len(updated.ThreadUnread) == 0 {
		return nil, nil
	}
	return updated.ThreadUnread, nil
}

// ClearSubscriptionThreadUnreadForAccount removes threadUnread from every one
// of account's subscriptions that has unread threads.
func (s *MongoStore) ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error {
	if _, err := s.subscriptions.UpdateMany(ctx,
		bson.M{"u.account": account, "threadUnread.0": bson.M{"$exists": true}},
		bson.M{"$unset": bson.M{"threadUnread": ""}},
	); err != nil {
		return fmt.Errorf("clear subscription thread-unread for %q: %w", account, err)
	}
	return nil
}

// ClearThreadSubscriptionsForAccount marks every one of account's thread
// subscriptions as read in one account-scoped bulk update. No order-safety guard
// on the source-site write; the $lt guard lives on the inbox-worker side. A
// single thread_read_all event carries the cross-site convergence, so no per-row
// snapshot is returned (and no Find/Update window can miss a concurrently
// inserted row).
func (s *MongoStore) ClearThreadSubscriptionsForAccount(ctx context.Context, account string, now time.Time) error {
	if _, err := s.threadSubscriptions.UpdateMany(ctx, bson.M{"userAccount": account}, bson.M{"$set": bson.M{
		"lastSeenAt": now,
		"updatedAt":  now,
		"hasMention": false,
	}}); err != nil {
		return fmt.Errorf("clear thread subscriptions for %q: %w", account, err)
	}
	return nil
}

// UpdateRoomVisibility sets {restricted, externalAccess, updatedAt} on the
// room. Room-service callers have already validated type=channel before
// reaching this layer, so no type filter runs here.
func (s *MongoStore) UpdateRoomVisibility(ctx context.Context, roomID string, restricted, externalAccess bool) error {
	res, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, bson.M{
		"$set": bson.M{
			"restricted":     restricted,
			"externalAccess": externalAccess,
			"updatedAt":      time.Now().UTC(),
		},
	})
	if err != nil {
		return fmt.Errorf("update room visibility %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return ErrRoomNotFound
	}
	return nil
}

// ApplySubscriptionRestriction writes the {restricted, externalAccess} denorm
// flags to every subscription of the room. When restricted=true and ownerAccount
// is non-empty, an aggregation-pipeline $cond also rewrites roles so only
// ownerAccount holds RoleOwner — atomically, so the restrict transition cannot
// land in a zero-owner state. An empty ownerAccount writes the flags alone and
// leaves roles untouched. Returns ErrOwnerNotSubscribed when ownerAccount has no
// active subscription in the room.
func (s *MongoStore) ApplySubscriptionRestriction(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string, restrictUpdatedAt time.Time) error {
	filter := bson.M{"roomId": roomID}

	if restricted && ownerAccount != "" {
		// TOCTOU: if the owner unsubscribes between this count and the
		// UpdateMany below, the room is left with zero owners. Acceptable for
		// an admin RPC (rare, recoverable by retry).
		n, err := s.subscriptions.CountDocuments(ctx, bson.M{"roomId": roomID, "u.account": ownerAccount})
		if err != nil {
			return fmt.Errorf("count owner subscription: %w", err)
		}
		if n == 0 {
			return ErrOwnerNotSubscribed
		}
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.M{
				"restricted":        true,
				"externalAccess":    externalAccess,
				"restrictUpdatedAt": restrictUpdatedAt,
				"roles": bson.M{"$cond": bson.M{
					"if":   bson.M{"$eq": bson.A{"$u.account", ownerAccount}},
					"then": bson.A{string(model.RoleOwner)},
					"else": bson.A{string(model.RoleMember)},
				}},
			}}},
		}
		if _, err := s.subscriptions.UpdateMany(ctx, filter, pipeline); err != nil {
			return fmt.Errorf("apply visibility (restrict+rewrite): %w", err)
		}
		return nil
	}

	if _, err := s.subscriptions.UpdateMany(ctx, filter, bson.M{
		"$set": bson.M{"restricted": restricted, "externalAccess": externalAccess, "restrictUpdatedAt": restrictUpdatedAt},
	}); err != nil {
		return fmt.Errorf("apply visibility (flags only): %w", err)
	}
	return nil
}

// ListSubscriptionsByRoom returns every subscription in the room. Callers only
// read the subscriber account, so project just that field.
func (s *MongoStore) ListSubscriptionsByRoom(ctx context.Context, roomID string) ([]model.Subscription, error) {
	// Primary: this list drives event fan-out and room-restrict right after
	// membership/message writes, so a lagging secondary could miss a just-added member.
	cursor, err := s.subscriptions.Find(ctx,
		bson.M{"roomId": roomID},
		options.Find().SetProjection(bson.M{"_id": 0, "u.account": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for room %q: find: %w", roomID, err)
	}
	var subs []model.Subscription
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("list subscriptions for room %q: decode: %w", roomID, err)
	}
	return subs, nil
}

// FindUsersByAccounts returns User docs for the supplied accounts. Empty input
// returns nil, nil. The sole caller only reads siteId (for cross-site inbox
// fan-out), so project just that field.
func (s *MongoStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	cursor, err := s.usersSecondary.Find(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "siteId": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}

// GetThreadRoomByID returns the thread_rooms document for threadRoomID,
// projected to lastMsgAt + minUserLastSeenAt for the floor-recompute path.
// Other ThreadRoom fields are NOT populated. Returns (nil, nil) when no
// document matches.
func (s *MongoStore) GetThreadRoomByID(ctx context.Context, threadRoomID string) (*model.ThreadRoom, error) {
	var tr model.ThreadRoom
	err := s.threadRooms.FindOne(ctx, bson.M{"_id": threadRoomID},
		options.FindOne().SetProjection(bson.M{"lastMsgAt": 1, "minUserLastSeenAt": 1, "_id": 0}),
	).Decode(&tr)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get thread room %q: %w", threadRoomID, err)
	}
	return &tr, nil
}

// MinThreadSubscriptionLastSeenByThreadRoomID returns the thread room's strict
// read floor: the minimum lastSeenAt across the thread's ordinary-member
// thread_subscriptions for threadRoomID, but only when every such subscriber has
// a usable lastSeenAt (> zero). Bot (".bot") and platform-admin (p_admin
// prefix) subscribers are excluded by account, so a bot that authored a threaded
// message cannot freeze the floor via its never-read parent-author subscription.
// QA p_ accounts are ordinary users and DO hold the floor.
// Returns nil when any subscriber has never read, or when there are no subscribers.
// The (threadRoomId, lastSeenAt) index (non-sparse) returns the smallest value
// first — a missing/null/zero lastSeenAt sorts before real dates, so the first
// document answers both "is anyone unread?" and "what is the floor?" in one seek.
func (s *MongoStore) MinThreadSubscriptionLastSeenByThreadRoomID(ctx context.Context, threadRoomID string) (*time.Time, error) {
	var doc struct {
		LastSeenAt time.Time `bson:"lastSeenAt"`
	}
	err := s.threadSubscriptions.FindOne(ctx,
		// Bot/admin thread subs don't hold the floor: excluded by account so a bot
		// that authored a threaded message can't freeze it via its never-read
		// parent-author subscription.
		bson.M{"threadRoomId": threadRoomID, "userAccount": bson.M{"$not": bson.Regex{Pattern: botOrPlatformAdminRegex()}}},
		options.FindOne().
			SetSort(bson.D{{Key: "lastSeenAt", Value: 1}}).
			SetProjection(bson.M{"lastSeenAt": 1, "_id": 0}),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find min lastSeenAt for thread room %q: %w", threadRoomID, err)
	}
	if !doc.LastSeenAt.After(time.Time{}) {
		return nil, nil
	}
	minTime := doc.LastSeenAt
	return &minTime, nil
}

// UpdateThreadRoomMinUserLastSeenAt sets or clears thread_rooms.minUserLastSeenAt
// for threadRoomID. A nil value clears the field via $unset; non-nil writes via $set.
func (s *MongoStore) UpdateThreadRoomMinUserLastSeenAt(ctx context.Context, threadRoomID string, t *time.Time) error {
	var update bson.M
	if t == nil {
		update = bson.M{"$unset": bson.M{"minUserLastSeenAt": ""}}
	} else {
		update = bson.M{"$set": bson.M{"minUserLastSeenAt": *t}}
	}
	if _, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, update); err != nil {
		return fmt.Errorf("update minUserLastSeenAt for thread room %q: %w", threadRoomID, err)
	}
	return nil
}

// GetThreadRoomInfos returns each existing thread room's lastMsgAt via a single
// projected find; missing thread rooms are omitted.
func (s *MongoStore) GetThreadRoomInfos(ctx context.Context, threadRoomIDs []string) ([]ThreadRoomInfoRow, error) {
	cursor, err := s.threadRoomsSecondary.Find(ctx,
		bson.M{"_id": bson.M{"$in": threadRoomIDs}},
		options.Find().SetProjection(bson.M{"_id": 1, "lastMsgAt": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find thread rooms: %w", err)
	}
	defer cursor.Close(ctx)

	var trs []struct {
		ID        string    `bson:"_id"`
		LastMsgAt time.Time `bson:"lastMsgAt"`
	}
	if err := cursor.All(ctx, &trs); err != nil {
		return nil, fmt.Errorf("decode thread rooms: %w", err)
	}

	out := make([]ThreadRoomInfoRow, 0, len(trs))
	for _, tr := range trs {
		out = append(out, ThreadRoomInfoRow{
			ThreadRoomID: tr.ID,
			LastMsgAt:    tr.LastMsgAt,
		})
	}
	return out, nil
}
