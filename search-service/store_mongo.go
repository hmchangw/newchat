package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

// mongoStore is the Mongo-backed implementation of MongoStore.
type mongoStore struct {
	apps          *mongo.Collection
	subscriptions *mongo.Collection
	users         *mongo.Collection
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		apps:          db.Collection("apps"),
		subscriptions: db.Collection("subscriptions"),
		users:         db.Collection("users"),
	}
}

// ensureIndexes creates the index backing SearchAppsByName's $match on name.
// The match is a case-insensitive substring $regex, which Mongo cannot satisfy
// with tight index bounds — but a {name:1} index still lets the planner scan
// the compact index instead of doing a full collection scan, and supports the
// $lookup/$group stages the spec'd pipeline will add. CreateOne is idempotent
// when the key spec matches.
func (s *mongoStore) ensureIndexes(ctx context.Context) error {
	if _, err := s.apps.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "name", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure apps (name) index: %w", err)
	}
	if _, err := s.subscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomId", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure subscriptions (u.account, roomId) index: %w", err)
	}
	if _, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure users (account) index: %w", err)
	}
	if _, err := s.apps.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "assistant.name", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure apps (assistant.name) index: %w", err)
	}
	return nil
}

// SubscriptionsByRoomIDs returns the caller's subscription meta keyed by roomID.
func (s *mongoStore) SubscriptionsByRoomIDs(ctx context.Context, account string, roomIDs []string) (map[string]SubscriptionMeta, error) {
	out := map[string]SubscriptionMeta{}
	if len(roomIDs) == 0 {
		return out, nil
	}
	filter := bson.M{"u.account": account, "roomId": bson.M{"$in": roomIDs}}
	proj := bson.M{"roomId": 1, "roomType": 1, "name": 1}
	cur, err := s.subscriptions.Find(ctx, filter, options.Find().SetProjection(proj))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions: %w", err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		RoomID   string         `bson:"roomId"`
		RoomType model.RoomType `bson:"roomType"`
		Name     string         `bson:"name"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}
	for _, r := range rows {
		out[r.RoomID] = SubscriptionMeta{RoomType: r.RoomType, Name: r.Name}
	}
	return out, nil
}

// UsersByAccounts returns HR/display projections keyed by account.
func (s *mongoStore) UsersByAccounts(ctx context.Context, accounts []string) (map[string]HRUser, error) {
	out := map[string]HRUser{}
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"account": bson.M{"$in": accounts}}
	proj := bson.M{"account": 1, "engName": 1, "chineseName": 1}
	cur, err := s.users.Find(ctx, filter, options.Find().SetProjection(proj))
	if err != nil {
		return nil, fmt.Errorf("find users: %w", err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		Account     string `bson:"account"`
		EngName     string `bson:"engName"`
		ChineseName string `bson:"chineseName"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	for _, r := range rows {
		out[r.Account] = HRUser{Account: r.Account, EngName: r.EngName, ChineseName: r.ChineseName}
	}
	return out, nil
}

// AppsByAssistantNames returns apps keyed by their assistant.name (bot account).
func (s *mongoStore) AppsByAssistantNames(ctx context.Context, botAccounts []string) (map[string]model.App, error) {
	out := map[string]model.App{}
	if len(botAccounts) == 0 {
		return out, nil
	}
	filter := bson.M{"assistant.name": bson.M{"$in": botAccounts}}
	cur, err := s.apps.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find apps by assistant: %w", err)
	}
	defer cur.Close(ctx)
	var apps []model.App
	if err := cur.All(ctx, &apps); err != nil {
		return nil, fmt.Errorf("decode apps: %w", err)
	}
	for i := range apps {
		if apps[i].Assistant != nil && apps[i].Assistant.Name != "" {
			out[apps[i].Assistant.Name] = apps[i]
		}
	}
	return out, nil
}

func (s *mongoStore) SearchAppsByName(
	ctx context.Context,
	query, account string,
	assistantEnabled *bool,
	offset, limit int,
) ([]model.App, error) {
	pipeline := buildSearchAppsPipeline(query, account, assistantEnabled, offset, limit)
	cur, err := s.apps.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate apps: %w", err)
	}
	defer cur.Close(ctx)

	results := make([]model.App, 0)
	if err := cur.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode apps: %w", err)
	}
	return results, nil
}
