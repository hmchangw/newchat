package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ThreadFollowerLister returns the set of accounts following the thread rooted at parentMessageID.
// Backed by thread_rooms.replyAccounts (every replier + parent author seeded at creation) — matches
// the legacy notification rule.
type ThreadFollowerLister interface {
	Followers(ctx context.Context, parentMessageID string) (map[string]struct{}, error)
}

type mongoThreadFollowers struct {
	col *mongo.Collection
}

func newMongoThreadFollowers(col *mongo.Collection) *mongoThreadFollowers {
	return &mongoThreadFollowers{col: col}
}

func (m *mongoThreadFollowers) Followers(ctx context.Context, parentMessageID string) (map[string]struct{}, error) {
	if parentMessageID == "" {
		return map[string]struct{}{}, nil
	}
	var doc struct {
		ReplyAccounts []string `bson:"replyAccounts"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "_id": 0})
	err := m.col.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return map[string]struct{}{}, nil
		}
		return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	out := make(map[string]struct{}, len(doc.ReplyAccounts))
	for _, a := range doc.ReplyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	return out, nil
}
