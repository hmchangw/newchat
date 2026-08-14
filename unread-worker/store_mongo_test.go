package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRoomLastMsgFilter_RejectsStaleReplay(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	got := roomLastMsgFilter("r1", at)

	// $not/$gte (not $lt) so the filter still matches a room whose lastMsgAt is
	// missing or null — a plain $lt would skip those and never set the pointer.
	assert.Equal(t, bson.M{
		"_id":       "r1",
		"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}},
	}, got)
}

func TestMentionFilter_SkipsAccountsThatAlreadyRead(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	got := mentionFilter(subKey{roomID: "r1", account: "bob"}, at)

	assert.Equal(t, bson.M{
		"roomId":     "r1",
		"u.account":  "bob",
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": at}},
	}, got)
}

// Every bulk write must return before it reaches its collection when there is
// nothing to write. The store is built with nil collections, so a stray call
// panics rather than silently passing.
func TestMongoStore_BulkWrites_EmptyMapNoOp(t *testing.T) {
	tests := []struct {
		name     string
		nilMap   func(context.Context, *mongoStore) error
		emptyMap func(context.Context, *mongoStore) error
	}{
		{
			name:   "BulkUpdateRoomLastMessage",
			nilMap: func(ctx context.Context, s *mongoStore) error { return s.BulkUpdateRoomLastMessage(ctx, nil) },
			emptyMap: func(ctx context.Context, s *mongoStore) error {
				return s.BulkUpdateRoomLastMessage(ctx, make(map[string]roomLastMsgUpdate))
			},
		},
		{
			name:   "BulkAdvanceLastSeen",
			nilMap: func(ctx context.Context, s *mongoStore) error { return s.BulkAdvanceLastSeen(ctx, nil) },
			emptyMap: func(ctx context.Context, s *mongoStore) error {
				return s.BulkAdvanceLastSeen(ctx, make(map[subKey]time.Time))
			},
		},
		{
			name:   "BulkSetMentions",
			nilMap: func(ctx context.Context, s *mongoStore) error { return s.BulkSetMentions(ctx, nil) },
			emptyMap: func(ctx context.Context, s *mongoStore) error {
				return s.BulkSetMentions(ctx, make(map[subKey]time.Time))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			store := &mongoStore{}

			assert.NoError(t, tc.nilMap(ctx, store), "nil map must be a no-op")
			assert.NoError(t, tc.emptyMap(ctx, store), "empty non-nil map must be a no-op")
		})
	}
}
