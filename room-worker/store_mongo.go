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
	"github.com/hmchangw/chat/pkg/pipelines"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	roomMembers   *mongo.Collection
	users         *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		roomMembers:   db.Collection("room_members"),
		users:         db.Collection("users"),
	}
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
// sub-creation via model.IsBotAccount) and UserCount is total minus bots — both
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

	if _, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, bson.M{
		"$set": bson.M{
			"userCount": total - appCount,
			"appCount":  appCount,
			"updatedAt": time.Now().UTC(),
		},
	}); err != nil {
		return fmt.Errorf("update room counts: %w", err)
	}
	return nil
}

func (s *MongoStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	var room model.Room
	if err := s.rooms.FindOne(ctx, bson.M{"_id": roomID}).Decode(&room); err != nil {
		return nil, fmt.Errorf("room %q not found: %w", roomID, err)
	}
	return &room, nil
}

func (s *MongoStore) GetUser(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"account": account}).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", account, err)
	}
	return &u, nil
}

func (s *MongoStore) CreateRoom(ctx context.Context, room *model.Room) error {
	if _, err := s.rooms.InsertOne(ctx, room); err != nil {
		return fmt.Errorf("insert room: %w", err)
	}
	return nil
}

func (s *MongoStore) ListNewMembersForNewRoom(ctx context.Context, orgIDs, accounts []string, excludeAccount string) ([]string, error) {
	pipe := pipelines.GetNewMembersPipeline(orgIDs, accounts, "", excludeAccount)
	pipe = append(pipe, bson.M{"$group": bson.M{
		"_id":      nil,
		"accounts": bson.M{"$addToSet": "$account"},
	}})
	cur, err := s.users.Aggregate(ctx, pipe)
	if err != nil {
		return nil, fmt.Errorf("list new members for new room: %w", err)
	}
	defer cur.Close(ctx)
	if !cur.Next(ctx) {
		// Distinguish a true empty result from a cursor/read failure — the
		// caller must not proceed to room creation when membership resolution
		// silently failed.
		if err := cur.Err(); err != nil {
			return nil, fmt.Errorf("iterate aggregation result: %w", err)
		}
		return nil, nil
	}
	var doc struct {
		Accounts []string `bson:"accounts"`
	}
	if err := cur.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode aggregation result: %w", err)
	}
	return doc.Accounts, nil
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

func (s *MongoStore) BulkCreateRoomMembers(ctx context.Context, members []*model.RoomMember) error {
	if len(members) == 0 {
		return nil
	}
	docs := make([]interface{}, len(members))
	for i, m := range members {
		docs[i] = m
	}
	opts := options.InsertMany().SetOrdered(false)
	if _, err := s.roomMembers.InsertMany(ctx, docs, opts); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("bulk create %d room members: %w", len(members), err)
		}
	}
	return nil
}

func (s *MongoStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	cursor, err := s.users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}})
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}

func (s *MongoStore) HasOrgRoomMembers(ctx context.Context, roomID string) (bool, error) {
	count, err := s.roomMembers.CountDocuments(ctx, bson.M{"rid": roomID, "member.type": model.RoomMemberOrg})
	if err != nil {
		return false, fmt.Errorf("count room members for %q: %w", roomID, err)
	}
	return count > 0, nil
}

func (s *MongoStore) ListAddMemberCandidates(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]AddMemberCandidate, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return nil, nil
	}
	pipeline, err := pipelines.GetAddMemberCandidatesPipeline(orgIDs, directAccounts, roomID, "")
	if err != nil {
		return nil, fmt.Errorf("build add-member candidates pipeline: %w", err)
	}
	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate add-member candidates: %w", err)
	}
	defer cursor.Close(ctx)
	var out []AddMemberCandidate
	if err := cursor.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode add-member candidates: %w", err)
	}
	return out, nil
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
