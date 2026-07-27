package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type mongoSubscriptionSource struct {
	col *mongo.Collection
}

// Compile-time assertion that *mongoSubscriptionSource satisfies SubscriptionSource.
var _ SubscriptionSource = (*mongoSubscriptionSource)(nil)

func newMongoSubscriptionSource(db *mongo.Database) *mongoSubscriptionSource {
	return &mongoSubscriptionSource{col: db.Collection("subscriptions")}
}

// toStringSlice converts a mongo.Collection.Distinct result (untyped
// []any) to []string, dropping any non-string or empty value defensively
// — a malformed roomId on one row must not break enumeration for every
// other room.
func toStringSlice(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func (s *mongoSubscriptionSource) RoomIDs(ctx context.Context, siteID string) ([]string, error) {
	var values []any
	if err := s.col.Distinct(ctx, "roomId", bson.M{"siteId": siteID}).Decode(&values); err != nil {
		return nil, fmt.Errorf("distinct roomId for site %s: %w", siteID, err)
	}
	return toStringSlice(values), nil
}

var subscriptionProjection = bson.M{
	"_id": 1, "u": 1, "roomId": 1, "siteId": 1, "name": 1, "roomType": 1,
	"historySharedSince": 1, "joinedAt": 1,
}

func (s *mongoSubscriptionSource) Subscriptions(ctx context.Context, siteID string) ([]model.Subscription, error) {
	cur, err := s.col.Find(ctx, bson.M{"siteId": siteID}, options.Find().SetProjection(subscriptionProjection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for site %s: %w", siteID, err)
	}
	defer func() { _ = cur.Close(ctx) }() // read-only cursor; a close error here can't affect already-decoded results

	var subs []model.Subscription
	if err := cur.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode subscriptions for site %s: %w", siteID, err)
	}
	return subs, nil
}
