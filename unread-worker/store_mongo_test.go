package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

// modelParts pulls the filter and update documents back out of a write model so
// a test can assert what would actually be sent, without a live Mongo.
func modelParts(t *testing.T, m mongo.WriteModel) (bson.M, bson.M) {
	t.Helper()
	one, ok := m.(*mongo.UpdateOneModel)
	require.True(t, ok, "expected an UpdateOneModel, got %T", m)
	filter, ok := one.Filter.(bson.M)
	require.True(t, ok, "expected a bson.M filter, got %T", one.Filter)
	update, ok := one.Update.(bson.M)
	require.True(t, ok, "expected a bson.M update, got %T", one.Update)
	return filter, update
}

// lastMentionAllAt is an independent monotonic dimension, not part of the room
// pointer, so it must not ride the pointer's regression guard. Bundled into
// that $set, a replay whose pointer already lost writes NOTHING — and
// user-service derives HasGroupMention from this field, so the @all badge is
// gone for every member of the room, permanently.
func TestRoomLastMsgModels_GroupMentionIsNotGatedOnThePointer(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	mentionAll := at.Add(-time.Minute)

	models := roomLastMsgModels(map[string]roomLastMsgUpdate{
		"r1": {at: at, msgID: "m1", lastMentionAllAt: mentionAll},
	})

	require.Len(t, models, 2, "the pointer and the badge need different filters, so they are separate writes")

	pointerFilter, pointerUpdate := modelParts(t, models[0])
	assert.Equal(t, roomLastMsgFilter("r1", at), pointerFilter)
	assert.Equal(t, bson.M{"$set": bson.M{
		"lastMsgAt": at, "lastMsgId": "m1", "updatedAt": at,
	}}, pointerUpdate, "the badge must not be inside the guarded $set")

	badgeFilter, badgeUpdate := modelParts(t, models[1])
	assert.Equal(t, bson.M{"_id": "r1"}, badgeFilter,
		"the badge write matches on identity alone — a lost pointer race must not skip it")
	// $max, not $set: the write is unguarded now, so monotonicity has to come
	// from the operator or an older replay would drag the badge backwards.
	assert.Equal(t, bson.M{"$max": bson.M{"lastMentionAllAt": mentionAll}}, badgeUpdate)
}

// A plain message carries no badge, and must not emit a write that could
// create or disturb the field.
func TestRoomLastMsgModels_NoGroupMentionEmitsPointerWriteOnly(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	models := roomLastMsgModels(map[string]roomLastMsgUpdate{
		"r1": {at: at, msgID: "m1"},
	})

	require.Len(t, models, 1)
	filter, update := modelParts(t, models[0])
	assert.Equal(t, roomLastMsgFilter("r1", at), filter)
	assert.Equal(t, bson.M{"$set": bson.M{
		"lastMsgAt": at, "lastMsgId": "m1", "updatedAt": at,
	}}, update)
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
