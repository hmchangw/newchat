package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ThreadRoomInfo is the per-thread metadata read from thread_rooms in one query.
// The parent's createdAt is no longer read here — it comes authoritatively from
// history-service (see ParentFetcher), which is race-free on the first reply.
type ThreadRoomInfo struct {
	Followers map[string]struct{}
}

// ThreadFollowerLister reads thread metadata for the thread rooted at parentMessageID.
type ThreadFollowerLister interface {
	Lookup(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)
}

type mongoThreadFollowers struct {
	col *mongo.Collection
}

func newMongoThreadFollowers(col *mongo.Collection) *mongoThreadFollowers {
	return &mongoThreadFollowers{col: col}
}

func (m *mongoThreadFollowers) Lookup(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error) {
	if parentMessageID == "" {
		return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
	}
	var doc struct {
		ReplyAccounts []string `bson:"replyAccounts"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "_id": 0})
	err := m.col.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
		}
		return ThreadRoomInfo{}, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	out := make(map[string]struct{}, len(doc.ReplyAccounts))
	for _, a := range doc.ReplyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	return ThreadRoomInfo{Followers: out}, nil
}
