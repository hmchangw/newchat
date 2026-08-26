package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ThreadRoomInfo is the per-thread metadata read from thread_rooms in one query.
// ParentCreatedAt is nil when the document is absent or its timestamp is zero —
// "unknown", never the epoch, so the suppression gate fails closed on missing data.
type ThreadRoomInfo struct {
	Followers       map[string]struct{}
	ParentCreatedAt *time.Time
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
		ReplyAccounts         []string  `bson:"replyAccounts"`
		ThreadParentCreatedAt time.Time `bson:"threadParentCreatedAt"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "threadParentCreatedAt": 1, "_id": 0})
	err := m.col.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
		}
		return ThreadRoomInfo{}, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	return threadRoomInfoFrom(doc.ReplyAccounts, doc.ThreadParentCreatedAt), nil
}

// threadRoomInfoFrom builds the projection, mapping a zero parent timestamp to nil.
// model.ThreadRoom.ThreadParentCreatedAt is a non-pointer time.Time, so an
// unresolved parent persists as the zero value rather than as absent.
func threadRoomInfoFrom(replyAccounts []string, parentCreatedAt time.Time) ThreadRoomInfo {
	out := make(map[string]struct{}, len(replyAccounts))
	for _, a := range replyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	info := ThreadRoomInfo{Followers: out}
	if !parentCreatedAt.IsZero() {
		at := parentCreatedAt.UTC()
		info.ParentCreatedAt = &at
	}
	return info
}
