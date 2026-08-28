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
	db := testutil.MongoDB(t, "roomlist_worker_test")
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
func flushOne(t *testing.T, store Store, evt eventProjection) *fakeMsg {
	t.Helper()
	f := newFlusher(store, 0, 0)
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

	m := flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
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

	m := flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
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

	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m2", RoomID: "r1", UserAccount: "alice", CreatedAt: newer},
	})
	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: older},
	})

	room := readRoom(t, db, "r1")
	assert.Equal(t, "m2", room["lastMsgId"], "the older replay must not win")
	assert.WithinDuration(t, newer, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)
}

// Two messages at the SAME millisecond, arriving in separate flush batches so
// the in-memory coalescer cannot break the tie for us — the server guard has to.
// The rule is msgbucket.NewerRow: equal created_at, higher message id wins.
//
// It has to be that rule specifically, because broadcast-worker's preview writer
// uses the same comparator to stamp previewForMsgId, and history-service serves
// a stored preview only while previewForMsgId equals lastMsgId. A guard that
// refused every same-instant write would leave lastMsgId on whichever message
// landed first while the preview named the other, and that room would read as a
// miss until a newer message arrived.
func TestIntegration_SameInstantTieBreaksOnTheMessageID(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	// Both arrival orders: the outcome must not depend on which landed first.
	for _, order := range [][2]string{{"m-1", "m-2"}, {"m-2", "m-1"}} {
		t.Run(order[0]+" then "+order[1], func(t *testing.T) {
			store, db := setupStore(t)
			seedRoom(t, db, "r1")

			for _, id := range order {
				flushOne(t, store, eventProjection{
					Event:   model.EventCreated,
					Message: messageProjection{ID: id, RoomID: "r1", UserAccount: "alice", CreatedAt: at},
				})
			}

			room := readRoom(t, db, "r1")
			assert.Equal(t, "m-2", room["lastMsgId"],
				"the higher id wins the tie regardless of arrival order")
		})
	}
}

// The same-instant clause must not become a way for an OLDER message to win:
// it admits equal timestamps only, so a strictly older replay is still refused
// however high its id sorts.
func TestIntegration_HigherIDCannotRegressAnOlderPointer(t *testing.T) {
	store, db := setupStore(t)
	older := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	seedRoom(t, db, "r1")

	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m-aaa", RoomID: "r1", UserAccount: "alice", CreatedAt: newer},
	})
	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m-zzz", RoomID: "r1", UserAccount: "alice", CreatedAt: older},
	})

	room := readRoom(t, db, "r1")
	assert.Equal(t, "m-aaa", room["lastMsgId"], "a higher id at an older instant must still lose")
	assert.WithinDuration(t, newer, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)
}

// The room pointer and the @all badge are independent dimensions. A batch
// carrying an @all message can be Nak'd once (a Mongo blip, a step-down, a
// flush that outran FLUSH_TIMEOUT) and redelivered after a later plain message
// has already advanced lastMsgAt. Guarding lastMentionAllAt on the pointer
// filter means that replay writes nothing at all, and since
// user-service derives HasGroupMention from this field, every member of the
// room silently loses the badge for that message — permanently, because the
// batch Acks on the retry that appears to succeed.
func TestIntegration_StaleReplayStillRecordsGroupMention(t *testing.T) {
	store, db := setupStore(t)
	older := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	seedRoom(t, db, "r1")

	// The plain, newer message lands first and advances the pointer.
	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m2", RoomID: "r1", UserAccount: "alice", CreatedAt: newer},
	})
	// The older @all message is the redelivery.
	flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "@all standup in 5", CreatedAt: older,
		},
	})

	room := readRoom(t, db, "r1")
	assert.Equal(t, "m2", room["lastMsgId"], "the older replay must still not win the pointer")
	assert.WithinDuration(t, newer, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)
	require.NotNil(t, room["lastMentionAllAt"], "the @all badge must survive a replay that loses the pointer race")
	assert.WithinDuration(t, older, room["lastMentionAllAt"].(bson.DateTime).Time(), time.Millisecond)
}

// The badge is monotonic in its own right: a replayed OLDER @all must not drag
// it back from a newer one already recorded.
func TestIntegration_GroupMentionNeverRegresses(t *testing.T) {
	store, db := setupStore(t)
	older := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	seedRoom(t, db, "r1")

	flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
			ID: "m2", RoomID: "r1", UserAccount: "alice",
			Content: "@all later", CreatedAt: newer,
		},
	})
	flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "@all earlier", CreatedAt: older,
		},
	})

	room := readRoom(t, db, "r1")
	assert.WithinDuration(t, newer, room["lastMentionAllAt"].(bson.DateTime).Time(), time.Millisecond,
		"an older @all replay must not regress the badge")
}

func TestIntegration_MentionSkippedWhenAccountAlreadyRead(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	readAfter := created.Add(time.Minute)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "bob", &readAfter)

	flushOne(t, store, eventProjection{
		Event: model.EventCreated,
		Message: messageProjection{
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

	flushOne(t, store, eventProjection{
		Event:   model.EventCreated,
		Message: messageProjection{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: earlier},
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

	flushOne(t, store, eventProjection{
		Event: model.EventUpdated,
		Message: messageProjection{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "now with @bob", CreatedAt: created, EditedAt: &edited,
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

	f := newFlusher(store, 0, 0)
	msgs := make([]*fakeMsg, 3)
	for i := range msgs {
		msgs[i] = &fakeMsg{}
		f.add(deriveIntents(&eventProjection{
			Event: model.EventCreated,
			Message: messageProjection{
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
