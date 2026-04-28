package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pipelines"
)

type MongoStore struct {
	rooms         *mongo.Collection
	subscriptions *mongo.Collection
	roomMembers   *mongo.Collection
	users         *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		rooms:         db.Collection("rooms"),
		subscriptions: db.Collection("subscriptions"),
		roomMembers:   db.Collection("room_members"),
		users:         db.Collection("users"),
	}
}

// EnsureIndexes creates the indexes that back the read paths in this service
// and the unique constraints required for retry-safe writes by room-worker.
// Must be invoked once at startup. Mongo treats index creation as idempotent
// when the key spec and options match.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	if _, err := s.roomMembers.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "rid", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure room_members (rid) index: %w", err)
	}
	// Unique logical key — retries from room-worker generate fresh _id values
	// (see processAddMembers), so without this constraint a redelivered
	// member.add would silently insert duplicate room_members. The bulk-insert
	// path in room-worker already ignores mongo.IsDuplicateKeyError, so this
	// makes redelivery idempotent.
	if _, err := s.roomMembers.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "rid", Value: 1}, {Key: "member.type", Value: 1}, {Key: "member.id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure room_members (rid,member.type,member.id) unique index: %w", err)
	}
	// Unique logical key for subscriptions. Same retry-idempotency rationale
	// as room_members above.
	if _, err := s.subscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure subscriptions (roomId,u.account) unique index: %w", err)
	}
	if _, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure users (account) index: %w", err)
	}
	if _, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "sectId", Value: 1}, {Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure users (sectId,account) index: %w", err)
	}
	return nil
}

func (s *MongoStore) CreateRoom(ctx context.Context, room *model.Room) error {
	_, err := s.rooms.InsertOne(ctx, room)
	return err
}

func (s *MongoStore) GetRoom(ctx context.Context, id string) (*model.Room, error) {
	var room model.Room
	if err := s.rooms.FindOne(ctx, bson.M{"_id": id}).Decode(&room); err != nil {
		return nil, fmt.Errorf("room %q not found: %w", id, err)
	}
	return &room, nil
}

func (s *MongoStore) ListRooms(ctx context.Context) ([]model.Room, error) {
	cursor, err := s.rooms.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	var rooms []model.Room
	if err := cursor.All(ctx, &rooms); err != nil {
		return nil, err
	}
	return rooms, nil
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

func (s *MongoStore) CreateSubscription(ctx context.Context, sub *model.Subscription) error {
	_, err := s.subscriptions.InsertOne(ctx, sub)
	return err
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
				bson.M{"$project": bson.M{"sectId": 1}},
			},
			"as": "userDoc",
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "room_members",
			"let":  bson.M{"sectId": bson.M{"$arrayElemAt": bson.A{"$userDoc.sectId", 0}}},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$rid", roomID}},
					bson.M{"$eq": bson.A{"$member.type", "org"}},
					bson.M{"$eq": bson.A{"$member.id", "$$sectId"}},
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
	if len(result.Owners) > 0 {
		counts.OwnerCount = result.Owners[0].Count
	}
	return counts, nil
}

func (s *MongoStore) ListRoomsByIDs(ctx context.Context, ids []string) ([]model.Room, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.rooms.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
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

func (s *MongoStore) CountNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) (int, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return 0, nil
	}

	pipeline := pipelines.GetNewMembersPipeline(orgIDs, directAccounts, roomID)
	pipeline = append(pipeline, bson.M{
		"$count": "n",
	})

	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("count new members: %w", err)
	}
	var results []struct {
		Count int `bson:"n"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return 0, fmt.Errorf("decode count new members: %w", err)
	}
	if len(results) == 0 {
		return 0, nil
	}
	return results[0].Count, nil
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
	// `display` sub-document (the aggregation writes values there to sidestep
	// the bson:"-" tags on RoomMemberEntry's display fields). Then copy the
	// display values onto Member.* in Go memory, where bson:"-" is irrelevant.
	var rows []roomMemberEnrichedRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode enriched room_members for %q: %w", roomID, err)
	}
	members := make([]model.RoomMember, 0, len(rows))
	for i := range rows {
		rm := rows[i].RoomMember
		d := rows[i].Display
		rm.Member.EngName = d.EngName
		rm.Member.ChineseName = d.ChineseName
		rm.Member.IsOwner = d.IsOwner
		rm.Member.SectName = d.SectName
		rm.Member.MemberCount = d.MemberCount
		members = append(members, rm)
	}
	return members, nil
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
	MemberCount int    `bson:"memberCount,omitempty"`
}

// enrichRoomMembersStages returns the $lookup + $set stages appended to the
// room_members aggregation when enrich=true. Each stage matches on
// member.type via $expr so it only fires for rows of the appropriate kind.
// All enrichment output is written into a `display` sub-document so it
// survives the RoomMemberEntry bson:"-" tags on decode.
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
				bson.M{"$project": bson.M{"engName": 1, "chineseName": 1, "_id": 0}},
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
		// Orgs: join users on sectId = member.id → sectName + count.
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let": bson.M{
				"orgId": "$member.id",
				"mtyp":  "$member.type",
			},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$$mtyp", "org"}},
					bson.M{"$eq": bson.A{"$sectId", "$$orgId"}},
				}}}},
				// $first:$sectName relies on the invariant that all users
				// sharing a sectId carry the same sectName; if that ever drifts,
				// the chosen name is non-deterministic without an upstream $sort.
				bson.M{"$group": bson.M{
					"_id":         nil,
					"sectName":    bson.M{"$first": "$sectName"},
					"memberCount": bson.M{"$sum": 1},
				}},
			},
			"as": "_orgMatch",
		}}},
		// Fold the three matches into a single `display` sub-document.
		{{Key: "$set", Value: bson.M{
			"display": bson.M{
				"engName":     bson.M{"$arrayElemAt": bson.A{"$_userMatch.engName", 0}},
				"chineseName": bson.M{"$arrayElemAt": bson.A{"$_userMatch.chineseName", 0}},
				"isOwner": bson.M{"$in": bson.A{
					"owner",
					bson.M{"$ifNull": bson.A{
						bson.M{"$arrayElemAt": bson.A{"$_subMatch.roles", 0}},
						bson.A{},
					}},
				}},
				"sectName":    bson.M{"$arrayElemAt": bson.A{"$_orgMatch.sectName", 0}},
				"memberCount": bson.M{"$arrayElemAt": bson.A{"$_orgMatch.memberCount", 0}},
			},
		}}},
		// Drop the temporary join arrays.
		{{Key: "$project", Value: bson.M{"_userMatch": 0, "_subMatch": 0, "_orgMatch": 0}}},
	}
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
	for i := range subs {
		sub := &subs[i]
		entry := model.RoomMemberEntry{
			ID:      sub.User.ID,
			Type:    model.RoomMemberIndividual,
			Account: sub.User.Account,
		}
		if enrich {
			entry.IsOwner = hasRole(sub.Roles, model.RoleOwner)
		}
		members = append(members, model.RoomMember{
			ID:     sub.ID,
			RoomID: roomID,
			Ts:     sub.JoinedAt,
			Member: entry,
		})
	}

	if enrich && len(members) > 0 {
		if err := s.attachUserDisplayNames(ctx, roomID, members); err != nil {
			return nil, fmt.Errorf("attach user display names for %q: %w", roomID, err)
		}
	}
	return members, nil
}

// attachUserDisplayNames batch-loads users for all individual members in the
// slice and copies EngName / ChineseName onto each member entry in place.
// Used only on the subscriptions-fallback + enrichment path.
func (s *MongoStore) attachUserDisplayNames(ctx context.Context, roomID string, members []model.RoomMember) error {
	accounts := make([]string, 0, len(members))
	for i := range members {
		if members[i].Member.Type == model.RoomMemberIndividual && members[i].Member.Account != "" {
			accounts = append(accounts, members[i].Member.Account)
		}
	}
	if len(accounts) == 0 {
		return nil
	}
	cursor, err := s.users.Find(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "account": 1, "engName": 1, "chineseName": 1}),
	)
	if err != nil {
		return fmt.Errorf("find users for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)

	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return fmt.Errorf("decode users for %q: %w", roomID, err)
	}
	byAccount := make(map[string]*model.User, len(users))
	for i := range users {
		byAccount[users[i].Account] = &users[i]
	}
	for i := range members {
		if u, ok := byAccount[members[i].Member.Account]; ok {
			members[i].Member.EngName = u.EngName
			members[i].Member.ChineseName = u.ChineseName
		}
	}
	return nil
}

// ListOrgMembers returns all users whose sectId equals orgID, projected as
// OrgMember rows sorted by account ascending. Returns errInvalidOrg when the
// query matches no users.
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
	cursor, err := s.users.Find(ctx, bson.M{"sectId": orgID}, opts)
	if err != nil {
		return nil, fmt.Errorf("find users for org %q: %w", orgID, err)
	}
	defer cursor.Close(ctx)

	var members []model.OrgMember
	if err := cursor.All(ctx, &members); err != nil {
		return nil, fmt.Errorf("decode users for org %q: %w", orgID, err)
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("list org members for %q: %w", orgID, errInvalidOrg)
	}
	return members, nil
}
