package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/orgdisplay"
	"github.com/hmchangw/chat/pkg/pipelines"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/userstore"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	roomMembers   *mongo.Collection
	users         *mongo.Collection
	apps          *mongo.Collection
	// userReader serves point lookups (GetUser, FindUsersByAccounts) and the
	// direct-account candidate fast path. Defaults to an uncached read of the
	// users collection; EnableUserCache wraps it in an LRU+TTL cache. The
	// `users` collection above is still used directly for the org-expansion
	// candidate query, which the by-account reader cannot serve.
	userReader userstore.UserStore
	// roomMeta, when non-nil (EnableRoomMetaCache), fronts GetRoomMeta with an
	// in-process LRU+TTL cache of the room's stable fields. Nil means GetRoomMeta
	// reads Mongo directly.
	roomMeta *roommetacache.Cache
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		roomMembers:   db.Collection("room_members"),
		users:         db.Collection("users"),
		apps:          db.Collection("apps"),
		userReader:    userstore.NewMongoStore(db.Collection("users")),
	}
}

// EnableUserCache wraps the store's user reader in an in-process LRU+TTL cache.
// Call once at startup; user docs are near-static (account→id and isBot are
// immutable, org fields change rarely) so a short TTL bounds staleness. Reads
// fall through to Mongo on miss; ErrUserNotFound is not negatively cached.
func (s *MongoStore) EnableUserCache(size int, ttl time.Duration) error {
	cache, err := userstore.NewCache(s.userReader, size, ttl)
	if err != nil {
		return fmt.Errorf("enable user cache: %w", err)
	}
	s.userReader = cache
	return nil
}

// EnableRoomMetaCache fronts GetRoomMeta with an in-process LRU+TTL cache. The
// add-member hot path (loadAddMemberInputs) reads only the stable fields
// (Type/Name/SiteID/ID/UserCount), so caching them is safe; a rename is
// reflected after at most the TTL. GetRoom is deliberately left uncached so the
// DM idempotency path still sees the room's real CreatedAt. Call once at startup.
func (s *MongoStore) EnableRoomMetaCache(size int, ttl time.Duration) error {
	rooms := s.rooms // capture only the collection, not the whole store
	cache, err := roommetacache.New(size, ttl, func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
		return roommetacache.FetchFromMongo(ctx, rooms, roomID)
	})
	if err != nil {
		return fmt.Errorf("enable room meta cache: %w", err)
	}
	s.roomMeta = cache
	return nil
}

// ListByRoom returns all subscriptions for roomID across every site. Not part
// of SubscriptionStore — the handler's hot paths only need accounts (see
// GetSubscriptionAccounts); this full-document read is retained for integration
// test verification.
func (s *MongoStore) ListByRoom(ctx context.Context, roomID string) ([]model.Subscription, error) {
	cursor, err := s.subscriptions.Find(ctx, bson.M{"roomId": roomID})
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for room %q: find: %w", roomID, err)
	}
	var subs []model.Subscription
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("list subscriptions for room %q: decode: %w", roomID, err)
	}
	return subs, nil
}

// ReconcileMemberCounts recomputes the room's AppCount (bot subs) and UserCount
// (everyone else) and writes both back in a single updateOne. AppCount is an
// index-backed CountDocuments on {roomId, u.isBot} (the flag is stamped at
// sub-creation for ".bot"/"p_" accounts) and UserCount is total minus bots — both
// counts use the index and no per-document regex runs. Deriving UserCount by
// subtraction also means legacy docs written before u.isBot existed (and any
// missing the field) correctly fall into UserCount rather than being dropped.
// Recompute-and-$set keeps the counts idempotent under JetStream redelivery.
func (s *MongoStore) ReconcileMemberCounts(ctx context.Context, roomID string) error {
	// A transient count error must not fall through to an UpdateOne with zero
	// counts, which would clobber the rooms doc.
	total, err := s.subscriptions.CountDocuments(ctx, bson.M{"roomId": roomID})
	if err != nil {
		return fmt.Errorf("count subscriptions: %w", err)
	}
	appCount, err := s.subscriptions.CountDocuments(ctx, bson.M{"roomId": roomID, "u.isBot": true})
	if err != nil {
		return fmt.Errorf("count app subscriptions: %w", err)
	}

	now := time.Now().UTC()
	if _, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, bson.M{
		"$set": bson.M{
			"userCount":          total - appCount,
			"appCount":           appCount,
			"countsReconciledAt": now,
			"updatedAt":          now,
		},
	}); err != nil {
		return fmt.Errorf("update room counts: %w", err)
	}
	return nil
}

// ApplyMemberCountDelta atomically $inc's userCount/appCount and reports whether
// a full recompute is now due. The $inc is O(1); the delta is the actual number
// of subscriptions created/removed by the caller, so it stays correct under
// JetStream redelivery (net-new is recomputed from live state each delivery) and
// concurrent adds ($inc is atomic). The returned countsReconciledAt drives the
// per-room TTL: a crash between the subscription write and this $inc, or a rare
// racing duplicate, can leave the counter slightly drifted until the next
// reconcile, which this method schedules once the TTL elapses.
func (s *MongoStore) ApplyMemberCountDelta(ctx context.Context, roomID string, userDelta, appDelta int, ttl time.Duration) (bool, error) {
	var doc struct {
		CountsReconciledAt time.Time `bson:"countsReconciledAt"`
	}
	err := s.rooms.FindOneAndUpdate(ctx,
		bson.M{"_id": roomID},
		bson.M{
			"$inc": bson.M{"userCount": userDelta, "appCount": appDelta},
			"$set": bson.M{"updatedAt": time.Now().UTC()},
		},
		options.FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetProjection(bson.M{"countsReconciledAt": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// No such room — nothing to count, nothing to reconcile (matches
			// ReconcileMemberCounts' no-op-on-missing UpdateOne).
			return false, nil
		}
		return false, fmt.Errorf("apply member count delta for room %q: %w", roomID, err)
	}
	return doc.CountsReconciledAt.IsZero() || time.Since(doc.CountsReconciledAt) > ttl, nil
}

// GetRoom returns the full room document from Mongo. It is never cache-served:
// callers such as serverCreateDM's idempotency path read time-sensitive fields
// (CreatedAt) that the meta cache does not carry. The add-member hot path uses
// GetRoomMeta instead, which only needs the stable fields.
func (s *MongoStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	var room model.Room
	if err := s.rooms.FindOne(ctx, bson.M{"_id": roomID}).Decode(&room); err != nil {
		return nil, fmt.Errorf("room %q not found: %w", roomID, err)
	}
	return &room, nil
}

// GetRoomMeta returns a room populated with only its stable fields
// (ID/Type/Name/SiteID/UserCount) — the subset the add-member hot path reads.
// When the meta cache is enabled it is served from the in-process LRU+TTL cache;
// otherwise it falls through to a direct Mongo read. The returned room's
// CreatedAt/UpdatedAt are zero — callers needing those must use GetRoom.
func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (*model.Room, error) {
	var (
		meta roommetacache.Meta
		err  error
	)
	if s.roomMeta != nil {
		meta, err = s.roomMeta.Get(ctx, roomID)
	} else {
		meta, err = roommetacache.FetchFromMongo(ctx, s.rooms, roomID)
	}
	if err != nil {
		return nil, err
	}
	return &model.Room{ID: meta.ID, Type: meta.Type, Name: meta.Name, SiteID: meta.SiteID, UserCount: meta.UserCount}, nil
}

func (s *MongoStore) GetUser(ctx context.Context, account string) (*model.User, error) {
	u, err := s.userReader.FindUserByAccount(ctx, account)
	if errors.Is(err, userstore.ErrUserNotFound) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", account, err)
	}
	return u, nil
}

// GetApp reads the apps collection, which room-service owns and indexes
// (assistant.name); room-worker only reads it and creates no index of its own.
// Projects only name — the one field callers use (the botDM roomName).
func (s *MongoStore) GetApp(ctx context.Context, botAccount string) (*model.App, error) {
	var a model.App
	err := s.apps.FindOne(ctx, bson.M{"assistant.name": botAccount},
		options.FindOne().SetProjection(bson.M{"name": 1})).Decode(&a)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrAppNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get app for bot %q: %w", botAccount, err)
	}
	return &a, nil
}

func (s *MongoStore) CreateRoom(ctx context.Context, room *model.Room, key *roomkeystore.RoomKeyPair) (bool, error) {
	// Marshal the room struct (honouring omitempty) into a document so an optional
	// encKey field can be attached and the whole thing written in one upsert.
	raw, err := bson.Marshal(room)
	if err != nil {
		return false, fmt.Errorf("marshal room: %w", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return false, fmt.Errorf("unmarshal room doc: %w", err)
	}
	// _id is supplied by the filter; $setOnInsert must not also set it.
	delete(doc, "_id")
	// Only encrypted (channel) rooms carry a key; DM/botDM rooms pass key=nil.
	if key != nil {
		doc["encKey"] = roomkeystore.InitialKeyDoc(*key)
	}

	// $setOnInsert makes redelivery idempotent: on a matching _id nothing is
	// written, so an existing room keeps its original key (and the bytes clients
	// already hold) rather than being overwritten with a freshly generated one.
	res, err := s.rooms.UpdateOne(ctx,
		bson.M{"_id": room.ID},
		bson.M{"$setOnInsert": doc},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		// A concurrent upsert can lose the insert race and surface E11000; the
		// document now exists, so report it as a match (not inserted) and let the
		// caller reconcile against the winner's document.
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("upsert room: %w", err)
	}
	return res.UpsertedCount == 1, nil
}

func (s *MongoStore) ListNewMembersForNewRoom(ctx context.Context, orgIDs, accounts []string, excludeAccount string) ([]string, error) {
	if len(orgIDs) == 0 && len(accounts) == 0 {
		return nil, nil
	}
	filter := pipelines.MatchCandidatesFilter(orgIDs, accounts, excludeAccount)
	cur, err := s.users.Find(ctx, filter, options.Find().SetProjection(bson.M{"account": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("list new members for new room: %w", err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		Account string `bson:"account"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode new members for new room: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	// account is unique in the users collection, so rows carry no duplicates.
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Account
	}
	return out, nil
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	if err := s.subscriptions.FindOne(ctx, filter).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
		}
		return nil, fmt.Errorf("get subscription for %q in room %q: %w", account, roomID, err)
	}
	return &sub, nil
}

func (s *MongoStore) RemoveRole(ctx context.Context, account, roomID string, role model.Role) error {
	filter := bson.M{"u.account": account, "roomId": roomID}
	update := bson.M{"$pull": bson.M{"roles": role}}
	res, err := s.subscriptions.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("remove role %q for %q in room %q: %w", role, account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("subscription not found for %q in room %q", account, roomID)
	}
	return nil
}

func (s *MongoStore) GetUserWithMembership(ctx context.Context, roomID, account string) (*UserWithMembership, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"account": account}}},
		// Dept-aware org-membership lookup: a user added via Orgs:["X"] may
		// match the org by deptId only (no sectId), so the room_members row
		// has member.id = deptId. Checking only sectId would miss that case
		// and report HasOrgMembership=false, causing the remove flow to drop
		// the user's subscription even though they are still org-attached.
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let":  bson.M{"sectId": "$sectId", "deptId": "$deptId"},
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
		{{Key: "$lookup", Value: bson.M{
			"from": "subscriptions",
			"let":  bson.M{"acct": "$account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$roomId", roomID}},
					bson.M{"$eq": bson.A{"$u.account", "$$acct"}},
				}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"roles": 1}},
			},
			"as": "targetSub",
		}}},
		{{Key: "$addFields", Value: bson.M{
			"hasOrgMembership": bson.M{"$gt": bson.A{bson.M{"$size": "$orgMembership"}, 0}},
			"roles": bson.M{"$ifNull": bson.A{
				bson.M{"$arrayElemAt": bson.A{"$targetSub.roles", 0}},
				bson.A{},
			}},
		}}},
		{{Key: "$project", Value: bson.M{"orgMembership": 0, "targetSub": 0}}},
	}
	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate user with membership: %w", err)
	}
	defer cursor.Close(ctx)
	var result UserWithMembership
	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return nil, fmt.Errorf("iterate user with membership: %w", err)
		}
		return nil, fmt.Errorf("user %q not found: %w", account, mongo.ErrNoDocuments)
	}
	if err := cursor.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode user with membership: %w", err)
	}
	return &result, nil
}

func (s *MongoStore) GetOrgMembersWithIndividualStatus(ctx context.Context, roomID, orgID string) ([]OrgMemberStatus, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"$or": bson.A{
			bson.M{"sectId": orgID},
			bson.M{"deptId": orgID},
		}}}},
		{{Key: "$addFields", Value: bson.M{
			"isDept": bson.M{"$eq": bson.A{"$deptId", orgID}},
			"name": bson.M{"$cond": bson.A{
				bson.M{"$eq": bson.A{"$deptId", orgID}}, "$deptName", "$sectName"}},
			"tcName": bson.M{"$cond": bson.A{
				bson.M{"$eq": bson.A{"$deptId", orgID}}, "$deptTCName", "$sectTCName"}},
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let":  bson.M{"uid": "$_id"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$rid", roomID}},
					bson.M{"$eq": bson.A{"$member.type", "individual"}},
					bson.M{"$eq": bson.A{"$member.id", "$$uid"}},
				}}}},
				bson.M{"$limit": 1},
				// Outer stage only reads $size — drop everything else.
				bson.M{"$project": bson.M{"_id": 1}},
			},
			"as": "individualMembership",
		}}},
		// Sibling-org lookup: is there ANOTHER org row in the same room whose
		// member.id matches this user's sectId or deptId (excluding the org
		// being removed)? If yes, the user remains a member via that sibling
		// even after the current org is dropped, so processRemoveOrg must NOT
		// delete their subscription.
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let":  bson.M{"sectId": "$sectId", "deptId": "$deptId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$rid", roomID}},
					bson.M{"$eq": bson.A{"$member.type", "org"}},
					bson.M{"$ne": bson.A{"$member.id", orgID}},
					bson.M{"$or": bson.A{
						bson.M{"$eq": bson.A{"$member.id", "$$sectId"}},
						bson.M{"$eq": bson.A{"$member.id", "$$deptId"}},
					}},
				}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"_id": 1}},
			},
			"as": "otherOrgMembership",
		}}},
		{{Key: "$project", Value: bson.M{
			"_id":                     0,
			"account":                 1,
			"siteId":                  1,
			"name":                    1,
			"tcName":                  1,
			"isDept":                  1,
			"hasIndividualMembership": bson.M{"$gt": bson.A{bson.M{"$size": "$individualMembership"}, 0}},
			"hasOtherOrgMembership":   bson.M{"$gt": bson.A{bson.M{"$size": "$otherOrgMembership"}, 0}},
		}}},
	}
	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate org members: %w", err)
	}
	defer cursor.Close(ctx)
	var results []OrgMemberStatus
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode org members: %w", err)
	}
	return results, nil
}

func (s *MongoStore) DeleteSubscription(ctx context.Context, roomID, account string) (int64, error) {
	res, err := s.subscriptions.DeleteOne(ctx, bson.M{"roomId": roomID, "u.account": account})
	if err != nil {
		return 0, fmt.Errorf("delete subscription for %q in room %q: %w", account, roomID, err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) (int64, error) {
	res, err := s.subscriptions.DeleteMany(ctx, bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}})
	if err != nil {
		return 0, fmt.Errorf("delete subscriptions for room %q: %w", roomID, err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteRoomMember(ctx context.Context, roomID string, memberType model.RoomMemberType, memberID string) error {
	_, err := s.roomMembers.DeleteOne(ctx, bson.M{"rid": roomID, "member.type": memberType, "member.id": memberID})
	if err != nil {
		return fmt.Errorf("delete room member: %w", err)
	}
	return nil
}

// BulkCreateSubscriptions upserts each sub idempotently, keyed on
// (roomId, u.account). On collision with an existing document (e.g. a
// JetStream redelivery of the same create/add-member event), $setOnInsert
// is a no-op so the persisted sub is preserved unchanged — preserving the
// insert-only contract for channel/DM/add-member paths while avoiding
// the duplicate-key error path entirely.
func (s *MongoStore) BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(subs))
	for _, sub := range subs {
		filter := bson.M{"roomId": sub.RoomID, "u.account": sub.User.Account}
		models = append(models, mongoutil.UpsertModel(filter, bson.M{"$setOnInsert": sub}))
	}
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := s.subscriptions.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("bulk create %d subscriptions: %w", len(subs), err)
	}
	return nil
}

// BulkCreateRoomMembers upserts each row on the (rid, member.type, member.id) unique key. $setOnInsert
// makes a re-add/redelivery an idempotent no-op that preserves the persisted _id/ts. The filter and the
// $setOnInsert paths are disjoint dotted leaves (member.type/member.id vs member.account) to avoid a
// path-conflict on insert.
func (s *MongoStore) BulkCreateRoomMembers(ctx context.Context, members []*model.RoomMember) error {
	if len(members) == 0 {
		return nil
	}
	writes := make([]mongo.WriteModel, len(members))
	for i, m := range members {
		set := bson.M{"_id": m.ID, "ts": m.Ts}
		if m.Member.Account != "" {
			set["member.account"] = m.Member.Account
		}
		filter := bson.M{"rid": m.RoomID, "member.type": m.Member.Type, "member.id": m.Member.ID}
		writes[i] = mongoutil.UpsertModel(filter, bson.M{"$setOnInsert": set})
	}
	if _, err := s.roomMembers.BulkWrite(ctx, writes, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk upsert %d room members: %w", len(members), err)
	}
	return nil
}

func (s *MongoStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	return s.userReader.FindUsersByAccounts(ctx, accounts)
}

func (s *MongoStore) FetchOrgDisplayUsers(ctx context.Context, orgIDs []string) ([]orgdisplay.User, error) {
	return pipelines.OrgDisplayUsers(ctx, s.users, orgIDs)
}

func (s *MongoStore) HasAnyRoomMembers(ctx context.Context, roomID string) (bool, error) {
	// Existence check only — cap at 1 so it short-circuits instead of counting every member row.
	count, err := s.roomMembers.CountDocuments(ctx, bson.M{"rid": roomID}, options.Count().SetLimit(1))
	if err != nil {
		return false, fmt.Errorf("count room members for %q: %w", roomID, err)
	}
	return count > 0, nil
}

// ExistingOrgMembers returns the subset of orgIDs that already have an org
// room_members row for roomID. Indexed on (rid, member.type, member.id).
func (s *MongoStore) ExistingOrgMembers(ctx context.Context, roomID string, orgIDs []string) (map[string]struct{}, error) {
	if len(orgIDs) == 0 {
		return map[string]struct{}{}, nil
	}
	cursor, err := s.roomMembers.Find(ctx,
		bson.M{"rid": roomID, "member.type": string(model.RoomMemberOrg), "member.id": bson.M{"$in": orgIDs}},
		options.Find().SetProjection(bson.M{"member.id": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("find existing org members for room %q: %w", roomID, err)
	}
	var rows []struct {
		Member struct {
			ID string `bson:"id"`
		} `bson:"member"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing org members: %w", err)
	}
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		set[r.Member.ID] = struct{}{}
	}
	return set, nil
}

func (s *MongoStore) ListAddMemberCandidates(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]AddMemberCandidate, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return nil, nil
	}
	// 1. Resolve the candidate users (account + id).
	type candidate struct{ ID, Account string }
	var candidates []candidate
	if len(orgIDs) == 0 {
		// Direct accounts only, cache-friendly. Bots are NOT filtered here —
		// room-service already validated the explicitly-listed ones.
		users, err := s.userReader.FindUsersByAccounts(ctx, directAccounts)
		if err != nil {
			return nil, fmt.Errorf("find add-member candidate users: %w", err)
		}
		for i := range users {
			candidates = append(candidates, candidate{ID: users[i].ID, Account: users[i].Account})
		}
	} else {
		// Org expansion needs the sectId/deptId query. WithDirectBots admits
		// listed bots while keeping the org arms bot-free.
		filter := pipelines.MatchCandidatesFilterWithDirectBots(orgIDs, directAccounts, "")
		cursor, err := s.users.Find(ctx, filter, options.Find().SetProjection(bson.M{"account": 1, "_id": 1}))
		if err != nil {
			return nil, fmt.Errorf("find add-member candidates: %w", err)
		}
		var rows []struct {
			ID      string `bson:"_id"`
			Account string `bson:"account"`
		}
		if err := cursor.All(ctx, &rows); err != nil {
			return nil, fmt.Errorf("decode add-member candidates: %w", err)
		}
		for _, r := range rows {
			candidates = append(candidates, candidate{ID: r.ID, Account: r.Account})
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	accounts := make([]string, len(candidates))
	ids := make([]string, len(candidates))
	for i, c := range candidates {
		accounts[i] = c.Account
		ids[i] = c.ID
	}
	// 2. Already-subscribed candidates (always needed).
	subbed, err := pipelines.SubscribedAccounts(ctx, s.subscriptions, roomID, accounts)
	if err != nil {
		return nil, err
	}
	// 3. Existing individual room_members rows — always resolved: the write-gate reads
	// HasIndividualRoomMember on any tracked room, so a stale false would re-insert an existing row.
	irm, err := s.individualMemberIDs(ctx, roomID, ids)
	if err != nil {
		return nil, err
	}
	out := make([]AddMemberCandidate, len(candidates))
	for i, c := range candidates {
		_, hasSub := subbed[c.Account]
		_, hasIRM := irm[c.ID]
		out[i] = AddMemberCandidate{Account: c.Account, HasSubscription: hasSub, HasIndividualRoomMember: hasIRM}
	}
	return out, nil
}

// individualMemberIDs returns the subset of user ids that already have an
// individual room_members row for roomID. Indexed on
// (rid, member.type, member.id).
func (s *MongoStore) individualMemberIDs(ctx context.Context, roomID string, ids []string) (map[string]struct{}, error) {
	cursor, err := s.roomMembers.Find(ctx,
		bson.M{"rid": roomID, "member.type": string(model.RoomMemberIndividual), "member.id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"member.id": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("find existing room members for room %q: %w", roomID, err)
	}
	var rows []struct {
		Member struct {
			ID string `bson:"id"`
		} `bson:"member"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing room members: %w", err)
	}
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		set[r.Member.ID] = struct{}{}
	}
	return set, nil
}

func (s *MongoStore) GetSubscriptionAccounts(ctx context.Context, roomID string) ([]string, error) {
	cursor, err := s.subscriptions.Find(ctx, bson.M{"roomId": roomID},
		options.Find().SetProjection(bson.M{"u.account": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("get subscription accounts for room %q: %w", roomID, err)
	}
	var subs []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode subscription accounts: %w", err)
	}
	accounts := make([]string, len(subs))
	for i, s := range subs {
		accounts[i] = s.User.Account
	}
	return accounts, nil
}

// FindDMSubscriptionPair returns both subs of a DM/botDM room in a single
// query. The first return value is the sub owned by requesterAccount, the
// second is the counterpart's.
func (s *MongoStore) FindDMSubscriptionPair(ctx context.Context, roomID, requesterAccount string) (*model.Subscription, *model.Subscription, error) {
	cursor, err := s.subscriptions.Find(ctx, bson.M{
		"roomId":   roomID,
		"roomType": bson.M{"$in": []model.RoomType{model.RoomTypeDM, model.RoomTypeBotDM}},
	})
	if err != nil {
		return nil, nil, err
	}
	var subs []model.Subscription
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, nil, err
	}
	if len(subs) != 2 {
		return nil, nil, model.ErrSubscriptionNotFound
	}
	var requesterSub, counterpartSub *model.Subscription
	for i := range subs {
		switch subs[i].User.Account {
		case requesterAccount:
			requesterSub = &subs[i]
		default:
			counterpartSub = &subs[i]
		}
	}
	if requesterSub == nil || counterpartSub == nil {
		return nil, nil, model.ErrSubscriptionNotFound
	}
	return requesterSub, counterpartSub, nil
}

func (s *MongoStore) UpdateRoomName(ctx context.Context, roomID, newName string) error {
	return s.updateChannelRoom(ctx, roomID, bson.M{
		"$set": bson.M{"name": newName, "updatedAt": time.Now().UTC()},
	})
}

// updateChannelRoom applies a $set update; room-service validates type=channel
// upstream before publishing the canonical event, so the store layer does not
// re-check.
func (s *MongoStore) updateChannelRoom(ctx context.Context, roomID string, update bson.M) error {
	res, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, update)
	if err != nil {
		return fmt.Errorf("update channel room %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return ErrRoomNotFound
	}
	return nil
}

func (s *MongoStore) UpdateSubscriptionNamesForRoom(ctx context.Context, roomID, newName string, nameUpdatedAt time.Time) error {
	// Guard each subscription on a monotonic nameUpdatedAt high-water mark so a
	// stale or reordered rename can't regress a newer name — and so the origin
	// doc never diverges from the nameUpdatedAt it federates. Mirrors
	// inbox-worker's guarded apply. Evaluated per document by UpdateMany.
	filter := bson.M{
		"roomId": roomID,
		"$or": bson.A{
			bson.M{"nameUpdatedAt": bson.M{"$exists": false}},
			bson.M{"nameUpdatedAt": bson.M{"$lt": nameUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"name": newName, "nameUpdatedAt": nameUpdatedAt}}
	if _, err := s.subscriptions.UpdateMany(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription names for room %s: %w", roomID, err)
	}
	return nil
}
