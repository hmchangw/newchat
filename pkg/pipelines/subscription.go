package pipelines

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// SubscribedAccounts returns the subset of accounts that already have a
// subscription to roomID, via an indexed point read on (roomId, u.account).
// It is the "subtract already-subscribed" half of candidate resolution, shared
// by room-service (CountNewMembers) and room-worker (ListAddMemberCandidates)
// so it stays in lock-step with MatchCandidatesFilter — exactly the drift this
// package exists to prevent.
func SubscribedAccounts(ctx context.Context, subscriptions *mongo.Collection, roomID string, accounts []string) (map[string]struct{}, error) {
	cursor, err := subscriptions.Find(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"u.account": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("find existing subscriptions for room %q: %w", roomID, err)
	}
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing subscriptions: %w", err)
	}
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		set[r.User.Account] = struct{}{}
	}
	return set, nil
}
