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
)

type storeMongo struct {
	rooms *mongo.Collection
	subs  *mongo.Collection
	users *mongo.Collection
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		rooms: db.Collection("rooms"),
		subs:  db.Collection("subscriptions"),
		users: db.Collection("users"),
	}
}

// EnsureIndexes creates the indexes backing this service's subscription reads and
// writes, and must be invoked once at startup. The rooms and users access here
// (including the room-key store) is entirely _id-keyed, so neither needs one.
func (s *storeMongo) EnsureIndexes(ctx context.Context) error {
	// subscriptions.{roomId,u.account} (unique) is owned by room-service; verify + warn only, never create.
	mongoutil.WarnMissingIndexes(ctx, s.subs, "roomId_1_u.account_1")
	// Upsert and delete identify a subscription by u._id — the remove RPC carries
	// user ids, not accounts — which the unique index above cannot serve.
	if _, err := s.subs.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "roomId", Value: 1}, {Key: "u._id", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure subscriptions (roomId,u._id) index: %w", err)
	}
	return nil
}

func (s *storeMongo) InsertRoom(ctx context.Context, room *Room) error {
	doc := bson.M{
		"_id":       room.ID,
		"t":         room.Type,
		"name":      room.Name,
		"topic":     room.Topic,
		"siteId":    room.SiteID,
		"createdAt": room.CreatedAt,
	}
	if room.Owner != nil {
		doc["u"] = participantBSON(room.Owner)
	}
	if room.CreatedByBot != "" {
		doc["createdByBot"] = room.CreatedByBot
	}
	if _, err := s.rooms.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicate
		}
		return fmt.Errorf("insert room %s: %w", room.ID, err)
	}
	return nil
}

func (s *storeMongo) FindRoom(ctx context.Context, roomID string) (*Room, error) {
	var doc struct {
		ID           string     `bson:"_id"`
		Type         string     `bson:"t"`
		Name         string     `bson:"name"`
		Topic        string     `bson:"topic"`
		SiteID       string     `bson:"siteId"`
		LastMsgAt    *time.Time `bson:"lastMsgAt"`
		CreatedAt    time.Time  `bson:"createdAt"`
		CreatedByBot string     `bson:"createdByBot"`
	}
	err := s.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1, "t": 1, "name": 1, "topic": 1, "siteId": 1, "lastMsgAt": 1, "createdAt": 1, "createdByBot": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find room %s: %w", roomID, err)
	}
	return &Room{
		ID: doc.ID, Type: doc.Type, Name: doc.Name, Topic: doc.Topic,
		SiteID: doc.SiteID, LastMsgAt: doc.LastMsgAt, CreatedAt: doc.CreatedAt, CreatedByBot: doc.CreatedByBot,
	}, nil
}

// UpsertSubscription uses $setOnInsert; re-execute is a no-op returning created=false. Uses the
// canonical u._id/u.account shape so notification-worker/broadcast-worker/inbox-worker/room-service all read it.
func (s *storeMongo) UpsertSubscription(ctx context.Context, sub *Subscription) (bool, error) {
	filter := bson.M{"roomId": sub.RoomID, "u._id": sub.UserID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":       sub.ID,
			"roomId":    sub.RoomID,
			"u":         bson.M{"_id": sub.UserID, "account": sub.Account, "isBot": sub.IsBot},
			"siteId":    sub.SiteID,
			"createdAt": sub.CreatedAt,
		},
	}
	res, err := s.subs.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		// The filter keys on u._id while the unique constraint keys on u.account, so a
		// concurrent add for the same account races the index instead of matching the
		// existing row. The account is subscribed either way — report it as an existing
		// membership rather than failing the caller's add.
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("upsert subscription (%s,%s): %w", sub.RoomID, sub.UserID, err)
	}
	return res.UpsertedCount > 0, nil
}

// DeleteSubscription deletes and returns the row's account in one round trip.
// FindOneAndDelete rather than DeleteOne so the caller learns which account it
// just de-authorized without a second lookup that could fail independently —
// u.account is exactly the key subauthcache is stored under.
func (s *storeMongo) DeleteSubscription(ctx context.Context, roomID, userID string) (string, bool, error) {
	var deleted struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	err := s.subs.FindOneAndDelete(ctx,
		bson.M{"roomId": roomID, "u._id": userID},
		options.FindOneAndDelete().SetProjection(bson.M{"u.account": 1, "_id": 0}),
	).Decode(&deleted)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("delete subscription (%s,%s): %w", roomID, userID, err)
	}
	return deleted.User.Account, true, nil
}

func (s *storeMongo) FindUser(ctx context.Context, userID string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx,
		bson.M{"_id": userID},
		options.FindOne().SetProjection(bson.M{
			"_id": 1, "account": 1, "siteId": 1, "engName": 1, "chineseName": 1, "roles": 1,
		}),
	).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find user %s: %w", userID, err)
	}
	return &u, nil
}

// ListRoomMemberAccounts returns the accounts subscribed to roomID.
func (s *storeMongo) ListRoomMemberAccounts(ctx context.Context, roomID string) ([]string, error) {
	cur, err := s.subs.Find(ctx,
		bson.M{"roomId": roomID},
		options.Find().SetProjection(bson.M{"u.account": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find room subscriptions: %w", err)
	}
	var docs []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.User.Account != "" {
			out = append(out, d.User.Account)
		}
	}
	return out, nil
}

func participantBSON(p *Participant) bson.M {
	out := bson.M{
		"id":       p.UserID,
		"username": p.Account,
	}
	if p.SiteID != "" {
		out["siteId"] = p.SiteID
	}
	if p.EngName != "" {
		out["engName"] = p.EngName
	}
	if p.ChineseName != "" {
		out["chineseName"] = p.ChineseName
	}
	if p.AppID != "" {
		out["appId"] = p.AppID
	}
	if p.AppName != "" {
		out["appName"] = p.AppName
	}
	if p.IsBot {
		out["isBot"] = true
	}
	return out
}
