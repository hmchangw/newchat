//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func setupStore(t *testing.T) (*mongoStore, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "room_state_worker_test")
	return NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions")), db
}

func seedRoom(t *testing.T, db *mongo.Database, roomID string) {
	t.Helper()
	_, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
		"_id": roomID, "type": model.RoomTypeChannel, "siteId": "site-a",
	})
	require.NoError(t, err)
}

func seedSubscription(t *testing.T, db *mongo.Database, roomID, account string, lastSeenAt *time.Time) {
	t.Helper()
	doc := bson.M{
		"_id":    roomID + "-" + account,
		"roomId": roomID,
		"u":      bson.M{"account": account},
	}
	if lastSeenAt != nil {
		doc["lastSeenAt"] = *lastSeenAt
	}
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), doc)
	require.NoError(t, err)
}

func readRoom(t *testing.T, db *mongo.Database, roomID string) bson.M {
	t.Helper()
	var got bson.M
	require.NoError(t, db.Collection("rooms").FindOne(context.Background(), bson.M{"_id": roomID}).Decode(&got))
	return got
}

func readSub(t *testing.T, db *mongo.Database, roomID, account string) bson.M {
	t.Helper()
	var got bson.M
	require.NoError(t, db.Collection("subscriptions").
		FindOne(context.Background(), bson.M{"roomId": roomID, "u.account": account}).Decode(&got))
	return got
}

// flushOne runs one event end-to-end through the real flusher and store.
func flushOne(t *testing.T, store Store, evt model.MessageEvent) *fakeMsg {
	t.Helper()
	f := newFlusher(store)
	m := &fakeMsg{}
	f.add(deriveIntents(&evt), held(m))
	f.Flush(context.Background())
	return m
}

func TestIntegration_CreatedMessageWritesRoomPointerSenderSeenAndMention(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", nil)
	seedSubscription(t, db, "r1", "bob", nil)

	m := flushOne(t, store, model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "hey @bob", CreatedAt: created,
		},
	})

	assert.True(t, m.acked)
	room := readRoom(t, db, "r1")
	assert.Equal(t, "m1", room["lastMsgId"])
	assert.WithinDuration(t, created, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)

	alice := readSub(t, db, "r1", "alice")
	assert.WithinDuration(t, created, alice["lastSeenAt"].(bson.DateTime).Time(), time.Millisecond)
	assert.Nil(t, alice["hasMention"], "alice is not @-mentioned by \"hey @bob\", so no badge is expected here — self-mention exclusion is covered by TestIntegration_SelfMentionDoesNotBadgeSender")

	bob := readSub(t, db, "r1", "bob")
	assert.Equal(t, true, bob["hasMention"])
}

// TestIntegration_SelfMentionDoesNotBadgeSender is the regression the write
// order in flush.go's write exists for: lastSeenAt is advanced before
// mentions are set, so a message that @-mentions its own sender is filtered
// out by mentionFilter (lastSeenAt already >= the mention's createdAt) and
// the sender is never badged. Swapping that order (verified manually — see
// the final-fix-report discrimination evidence) makes this test fail with
// alice's hasMention set to true.
func TestIntegration_SelfMentionDoesNotBadgeSender(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", nil)

	m := flushOne(t, store, model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "@alice reminder", CreatedAt: created,
		},
	})

	assert.True(t, m.acked)
	alice := readSub(t, db, "r1", "alice")
	assert.WithinDuration(t, created, alice["lastSeenAt"].(bson.DateTime).Time(), time.Millisecond)
	assert.Nil(t, alice["hasMention"], "a message that @-mentions its own sender must not badge the sender")
}

// The regression this design adds over broadcast-worker's coalescer: a
// redelivered older message must not drag the room pointer backwards.
func TestIntegration_StaleReplayDoesNotRegressRoomPointer(t *testing.T) {
	store, db := setupStore(t)
	older := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	seedRoom(t, db, "r1")

	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m2", RoomID: "r1", UserAccount: "alice", CreatedAt: newer},
	})
	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: older},
	})

	room := readRoom(t, db, "r1")
	assert.Equal(t, "m2", room["lastMsgId"], "the older replay must not win")
	assert.WithinDuration(t, newer, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)
}

func TestIntegration_MentionSkippedWhenAccountAlreadyRead(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	readAfter := created.Add(time.Minute)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "bob", &readAfter)

	flushOne(t, store, model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "@bob", CreatedAt: created,
		},
	})

	bob := readSub(t, db, "r1", "bob")
	assert.Nil(t, bob["hasMention"], "an account that already read past the message must not be badged")
}

func TestIntegration_SenderLastSeenNeverRegresses(t *testing.T) {
	store, db := setupStore(t)
	earlier := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", &later)

	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: earlier},
	})

	alice := readSub(t, db, "r1", "alice")
	assert.WithinDuration(t, later, alice["lastSeenAt"].(bson.DateTime).Time(), time.Millisecond)
}

func TestIntegration_EditBadgesNewlyMentionedAccount(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	edited := created.Add(time.Minute)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "bob", nil)

	flushOne(t, store, model.MessageEvent{
		Event: model.EventUpdated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "now with @bob", CreatedAt: created, EditedAt: &edited, UpdatedAt: &edited,
		},
	})

	assert.Equal(t, true, readSub(t, db, "r1", "bob")["hasMention"])
	// An edit must not move the room pointer.
	room := readRoom(t, db, "r1")
	assert.Nil(t, room["lastMsgId"])
}

func TestIntegration_AllEventsInOneBatchCoalesceToOneRoomWrite(t *testing.T) {
	store, db := setupStore(t)
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", nil)

	f := newFlusher(store)
	msgs := make([]*fakeMsg, 3)
	for i := range msgs {
		msgs[i] = &fakeMsg{}
		f.add(deriveIntents(&model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: string(rune('a' + i)), RoomID: "r1", UserAccount: "alice",
				CreatedAt: t1.Add(time.Duration(i) * time.Second),
			},
		}), held(msgs[i]))
	}
	f.Flush(context.Background())

	for i, m := range msgs {
		assert.True(t, m.acked, "message %d must be acked once its batch lands", i)
	}
	assert.Equal(t, "c", readRoom(t, db, "r1")["lastMsgId"], "the latest message in the batch wins")
}
