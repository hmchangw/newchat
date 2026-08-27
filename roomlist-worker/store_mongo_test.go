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

	got := roomLastMsgFilter("r1", "m-5", at)

	// Two admitting clauses, together spelling msgbucket.NewerRow in BSON.
	//
	// $not/$gte (not $lt) on the first so the filter still matches a room whose
	// lastMsgAt is missing or null — a plain $lt would skip those and never set
	// the pointer. The second admits the same-instant case the first refuses,
	// when this message sorts after the stored one by id.
	assert.Equal(t, bson.M{
		"_id": "r1",
		"$or": []bson.M{
			{"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}}},
			{"lastMsgAt": at, "lastMsgId": bson.M{"$lt": "m-5"}},
		},
	}, got)
}

// The guard has to break a same-instant tie the same way the in-memory
// coalescer does, and the same way broadcast-worker's preview writer does —
// all three are msgbucket.NewerRow. Two messages at one millisecond landing in
// separate flush batches would otherwise leave lastMsgId on whichever arrived
// first while broadcast-worker stamped previewForMsgId with the higher id, and
// history-service serves a stored preview only while those two agree. The room
// then reads as a permanent miss until a newer message arrives.
func TestRoomLastMsgFilter_AdmitsAHigherIDAtTheSameInstant(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	tie := roomLastMsgFilter("r1", "m-2", at)["$or"].([]bson.M)[1]

	assert.Equal(t, bson.M{"lastMsgAt": at, "lastMsgId": bson.M{"$lt": "m-2"}}, tie,
		"a same-instant write must land when its id sorts after the stored one")
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
		"r1": {at: at, msgID: "m1", lastMentionAllAt: mentionAll, userAt: at, userMsgID: "m1"},
	})

	require.Len(t, models, 3,
		"the pointer, the user position and the badge each need their own filter, so all three are separate writes")

	pointerFilter, pointerUpdate := modelParts(t, models[0])
	assert.Equal(t, roomLastMsgFilter("r1", "m1", at), pointerFilter)
	assert.Equal(t, bson.M{"$set": bson.M{
		"lastMsgAt": at, "lastMsgId": "m1", "updatedAt": at,
	}}, pointerUpdate, "the badge must not be inside the guarded $set")

	badgeFilter, badgeUpdate := modelParts(t, models[2])
	assert.Equal(t, bson.M{"_id": "r1"}, badgeFilter,
		"the badge write matches on identity alone — a lost pointer race must not skip it")
	// $max, not $set: the write is unguarded now, so monotonicity has to come
	// from the operator or an older replay would drag the badge backwards.
	assert.Equal(t, bson.M{"$max": bson.M{"lastMentionAllAt": mentionAll}}, badgeUpdate)
}

// A plain message carries no badge, and must not emit a write that could
// create or disturb the field. It does carry a user position, so the pointer
// write is accompanied by that $max and nothing else.
func TestRoomLastMsgModels_NoGroupMentionEmitsNoBadgeWrite(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	models := roomLastMsgModels(map[string]roomLastMsgUpdate{
		"r1": {at: at, msgID: "m1", userAt: at, userMsgID: "m1"},
	})

	require.Len(t, models, 2, "the pointer and the user position; no badge write")
	filter, update := modelParts(t, models[0])
	assert.Equal(t, roomLastMsgFilter("r1", "m1", at), filter)
	assert.Equal(t, bson.M{"$set": bson.M{
		"lastMsgAt": at, "lastMsgId": "m1", "updatedAt": at,
	}}, update)

	_, userUpdate := modelParts(t, models[1])
	assert.Equal(t, bson.M{"$max": bson.M{"lastUserMsgAt": at}}, userUpdate,
		"the only other write is the user position — nothing touches lastMentionAllAt")
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

// The user position is its own monotonic dimension, exactly like
// lastMentionAllAt: a batch that loses the pointer's regression guard to a newer
// message must not also lose a newer user position it alone carries. So it takes
// a separate $max write matched on identity, not a field in the guarded $set.
func TestRoomLastMsgModels_UserPositionIsItsOwnGuardedWrite(t *testing.T) {
	at := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)

	models := roomLastMsgModels(map[string]roomLastMsgUpdate{
		"r1": {msgID: "m1", at: at, userAt: at, userMsgID: "m1"},
	})

	require.Len(t, models, 2, "the room pointer and the user position are separate writes")
	upd, ok := models[1].(*mongo.UpdateOneModel)
	require.True(t, ok)
	assert.Equal(t, bson.M{"_id": "r1"}, upd.Filter, "matched on identity alone, like lastMentionAllAt")
	assert.Equal(t, bson.M{"$max": bson.M{"lastUserMsgAt": at}}, upd.Update,
		"$max supplies the monotonicity the pointer's guard would otherwise have implied")
}

// A system-only window must not name a user position. The write instead keeps
// whatever is stored, and pins a floor ONCE for a room that has never carried a
// message — which needs a read-before-write expression, hence a pipeline.
func TestRoomLastMsgModels_SystemOnlyWindowFreezesTheUserPosition(t *testing.T) {
	at := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)

	models := roomLastMsgModels(map[string]roomLastMsgUpdate{
		"r1": {msgID: "m-sys", at: at},
	})

	require.Len(t, models, 1, "a system-only window writes the pointer and nothing else")
	upd, ok := models[0].(*mongo.UpdateOneModel)
	require.True(t, ok)
	pipeline, ok := upd.Update.(mongo.Pipeline)
	require.True(t, ok, "pinning the floor reads the pre-update document, which a plain $set cannot do")

	// The freeze must ride the SAME model as the pointer. The bulk is unordered,
	// so a separate model could read lastMsgAt after this write had already set
	// it and conclude the room had carried a message all along.
	set := pipeline[0][0].Value.(bson.M)
	require.Contains(t, set, "lastUserMsgAt")
	require.Contains(t, set, "lastMsgAt", "the pointer and the freeze are one document read")
}
