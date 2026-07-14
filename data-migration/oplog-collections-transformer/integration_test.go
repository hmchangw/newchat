//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// --------------------------------------------------------------------------
// Target store tests (real Mongo via testutil.MongoDB)
// --------------------------------------------------------------------------

func TestTargetStore_UpsertUserIfAbsent(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "tgt")
	s := NewMongoTargetStore(db)
	require.NoError(t, s.EnsureIndexes(ctx))

	u := model.User{
		ID:          "userabc123",
		Account:     "alice",
		EngName:     "Alice A",
		ChineseName: "愛麗絲",
		SiteID:      "site1",
	}

	// First call — must insert.
	inserted, err := s.UpsertUserIfAbsent(ctx, u)
	require.NoError(t, err)
	assert.True(t, inserted, "first upsert must create the doc")

	// Second call with a different user but same account — must NOT overwrite.
	u2 := model.User{
		ID:          "differentid",
		Account:     "alice", // same account → filter matches existing
		EngName:     "Someone Else",
		ChineseName: "別人",
		SiteID:      "site2",
	}
	inserted2, err := s.UpsertUserIfAbsent(ctx, u2)
	require.NoError(t, err)
	assert.False(t, inserted2, "second upsert for same account must not insert")

	// Verify the stored doc is unchanged (first one wins).
	storedID, found, err := s.FindUserID(ctx, "alice")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, u.ID, storedID, "stored user id must be the first-inserted one")

	// Confirm the full doc still has the original engName.
	var got model.User
	require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "alice"}).Decode(&got))
	assert.Equal(t, "Alice A", got.EngName, "$setOnInsert must not overwrite existing fields")
}

func TestTargetStore_FindUserID(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "tgt")
	s := NewMongoTargetStore(db)
	require.NoError(t, s.EnsureIndexes(ctx))

	// Missing → found==false, no error.
	id, found, err := s.FindUserID(ctx, "nobody")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, id)

	// Insert then find.
	u := model.User{ID: "uid123abc", Account: "bob", SiteID: "site1"}
	inserted, err := s.UpsertUserIfAbsent(ctx, u)
	require.NoError(t, err)
	require.True(t, inserted)

	id2, found2, err := s.FindUserID(ctx, "bob")
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, "uid123abc", id2)
}

func TestTargetStore_FindThreadRoom(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "tgt")
	s := NewMongoTargetStore(db)

	// Missing → found==false, no error.
	roomID, trID, siteID, found, err := s.FindThreadRoom(ctx, "nonexistent_parent")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, roomID)
	assert.Empty(t, trID)
	assert.Empty(t, siteID)

	// Insert a thread room doc directly, then find it by parentMessageID.
	tr := model.ThreadRoom{
		ID:              "tr1",
		ParentMessageID: "msg1",
		RoomID:          "room1",
		SiteID:          "site1",
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	_, err = db.Collection("thread_rooms").InsertOne(ctx, tr)
	require.NoError(t, err)

	gotRoomID, gotTRID, gotSiteID, gotFound, err := s.FindThreadRoom(ctx, "msg1")
	require.NoError(t, err)
	assert.True(t, gotFound)
	assert.Equal(t, "room1", gotRoomID)
	assert.Equal(t, "tr1", gotTRID)
	assert.Equal(t, "site1", gotSiteID)
}

// --------------------------------------------------------------------------
// Inbox publisher round-trip (real NATS via testutil.NATS)
// --------------------------------------------------------------------------

func TestInboxPublisher_RoundTrip(t *testing.T) {
	const site = "site1rt"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()

	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	// Create the INBOX stream so publishes are captured.
	inboxCfg := stream.Inbox(site)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     inboxCfg.Name,
		Subjects: inboxCfg.Subjects,
	})
	require.NoError(t, err)

	pub := &jetstreamPublisher{publish: js.PublishMsg}

	evt := model.InboxEvent{
		Type:       "room_sync",
		SiteID:     site,
		DestSiteID: site,
		Payload:    []byte(`{"id":"r1","type":"channel"}`),
		Timestamp:  1700000000000,
	}
	require.NoError(t, pub.Publish(ctx, evt))

	// Create a consumer filtered to the aggregate lane for room_sync.
	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subject.InboxExternal(site, "room_sync")},
	})
	require.NoError(t, err)

	var got jetstream.Msg
	require.Eventually(t, func() bool {
		batch, berr := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for msg := range batch.Messages() {
			assert.NoError(t, msg.Ack())
			got = msg
			return true
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "room_sync event must land on INBOX")

	require.NotNil(t, got)
	assert.Equal(t, subject.InboxExternal(site, "room_sync"), got.Subject())

	var gotEvt model.InboxEvent
	require.NoError(t, json.Unmarshal(got.Data(), &gotEvt))
	assert.Equal(t, evt.Type, gotEvt.Type)
	assert.Equal(t, evt.SiteID, gotEvt.SiteID)
	assert.Equal(t, evt.DestSiteID, gotEvt.DestSiteID)
	assert.Equal(t, evt.Timestamp, gotEvt.Timestamp)
	assert.JSONEq(t, string(evt.Payload), string(gotEvt.Payload))
}

// --------------------------------------------------------------------------
// End-to-end handler tests (real Mongo source+target + real NATS)
// --------------------------------------------------------------------------

func TestEndToEnd_UserInsert(t *testing.T) {
	const site = "site1ue"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srcDB := testutil.MongoDB(t, "src")
	tgtDB := testutil.MongoDB(t, "tgt")

	// Source lookup via the real migration package.
	srcColl := srcDB.Collection("users")
	lookup := migration.NewMongoSourceLookup(srcColl)

	// Seed a source users doc.
	const srcID = "user001"
	_, err := srcColl.InsertOne(ctx, bson.M{
		"_id":      srcID,
		"username": "charlie",
		"type":     "user",
		"customFields": bson.M{
			"engName":     "Charlie C",
			"companyName": "查理",
			"deptId":      "dept1",
			"deptName":    "Engineering",
		},
	})
	require.NoError(t, err)

	// Read back as relaxed extJSON — the shape the connector emits.
	fullDoc, err := lookup.FindByID(ctx, srcID)
	require.NoError(t, err)
	require.NotEmpty(t, fullDoc)

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	tgtStore := NewMongoTargetStore(tgtDB)
	require.NoError(t, tgtStore.EnsureIndexes(ctx))

	h := &handler{
		siteID:         site,
		roomsColl:      "rocketchat_rooms",
		subsColl:       "rocketchat_subscriptions",
		threadSubsColl: "company_thread_subscriptions",
		usersColl:      "users",
		pub:            &jetstreamPublisher{publish: js.PublishMsg},
		target:         tgtStore,
		lookups:        map[string]migration.SourceLookup{"users": lookup},
		now:            func() int64 { return 1700000000000 },
	}

	require.NoError(t, h.handle(ctx, oplogEvent{
		Collection:   "users",
		Op:           "insert",
		FullDocument: fullDoc,
	}))

	// Assert the user now exists in the target users collection.
	var got model.User
	err = tgtDB.Collection("users").FindOne(ctx, bson.M{"account": "charlie"}).Decode(&got)
	require.NoError(t, err, "user must be present in target users collection")
	assert.Equal(t, "charlie", got.Account)
	assert.Equal(t, "Charlie C", got.EngName)
	assert.Equal(t, "查理", got.ChineseName)
	assert.Equal(t, site, got.SiteID)
}

func TestEndToEnd_RoomInsert_PublishesRoomSync(t *testing.T) {
	const site = "site1ri"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srcDB := testutil.MongoDB(t, "src")
	tgtDB := testutil.MongoDB(t, "tgt")
	srcColl := srcDB.Collection("rocketchat_rooms")
	lookup := migration.NewMongoSourceLookup(srcColl)

	const roomSrcID = "room001"
	_, err := srcColl.InsertOne(ctx, bson.M{
		"_id":   roomSrcID,
		"t":     "c",
		"fname": "General",
		"name":  "general",
		"uids":  bson.A{"u1", "u2"},
	})
	require.NoError(t, err)

	fullDoc, err := lookup.FindByID(ctx, roomSrcID)
	require.NoError(t, err)
	require.NotEmpty(t, fullDoc)

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	// Create the INBOX stream so the room_sync publish is captured.
	inboxCfg := stream.Inbox(site)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     inboxCfg.Name,
		Subjects: inboxCfg.Subjects,
	})
	require.NoError(t, err)

	tgtStore := NewMongoTargetStore(tgtDB)
	require.NoError(t, tgtStore.EnsureIndexes(ctx))

	h := &handler{
		siteID:         site,
		roomsColl:      "rocketchat_rooms",
		subsColl:       "rocketchat_subscriptions",
		threadSubsColl: "company_thread_subscriptions",
		usersColl:      "users",
		pub:            &jetstreamPublisher{publish: js.PublishMsg},
		target:         tgtStore,
		lookups:        map[string]migration.SourceLookup{"rocketchat_rooms": lookup},
		now:            func() int64 { return 1700000000000 },
	}

	require.NoError(t, h.handle(ctx, oplogEvent{
		Collection:   "rocketchat_rooms",
		Op:           "insert",
		FullDocument: fullDoc,
	}))

	// Fetch the room_sync event off INBOX.
	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subject.InboxExternal(site, "room_sync")},
	})
	require.NoError(t, err)

	var got jetstream.Msg
	require.Eventually(t, func() bool {
		batch, berr := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for msg := range batch.Messages() {
			assert.NoError(t, msg.Ack())
			got = msg
			return true
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "room_sync event must land on INBOX")

	require.NotNil(t, got)

	var evt model.InboxEvent
	require.NoError(t, json.Unmarshal(got.Data(), &evt))
	assert.Equal(t, model.InboxEventType("room_sync"), evt.Type)
	assert.Equal(t, site, evt.SiteID)

	var room model.Room
	require.NoError(t, json.Unmarshal(evt.Payload, &room))
	assert.Equal(t, roomSrcID, room.ID)
	assert.Equal(t, model.RoomTypeChannel, room.Type)
}

func TestEndToEnd_ThreadSub_NakThenResolve(t *testing.T) {
	const site = "site1ts"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srcDB := testutil.MongoDB(t, "src")
	tgtDB := testutil.MongoDB(t, "tgt")
	srcColl := srcDB.Collection("company_thread_subscriptions")
	lookup := migration.NewMongoSourceLookup(srcColl)

	// Seed a source thread sub doc.
	const tsubSrcID = "tsub001"
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err := srcColl.InsertOne(ctx, bson.M{
		"_id": tsubSrcID,
		"u": bson.M{
			"_id":      "user001",
			"username": "dave",
		},
		"rid": "room1",
		"parentMessage": bson.M{
			"_id": "msg_parent_1",
		},
		"lastSeenAt":    now,
		"unreadMention": 0,
		"createdAt":     now,
	})
	require.NoError(t, err)

	fullDoc, err := lookup.FindByID(ctx, tsubSrcID)
	require.NoError(t, err)
	require.NotEmpty(t, fullDoc)

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	// Create the INBOX stream.
	inboxCfg := stream.Inbox(site)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     inboxCfg.Name,
		Subjects: inboxCfg.Subjects,
	})
	require.NoError(t, err)

	tgtStore := NewMongoTargetStore(tgtDB)
	require.NoError(t, tgtStore.EnsureIndexes(ctx))

	h := &handler{
		siteID:         site,
		roomsColl:      "rocketchat_rooms",
		subsColl:       "rocketchat_subscriptions",
		threadSubsColl: "company_thread_subscriptions",
		usersColl:      "users",
		pub:            &jetstreamPublisher{publish: js.PublishMsg},
		target:         tgtStore,
		lookups:        map[string]migration.SourceLookup{"company_thread_subscriptions": lookup},
		now:            func() int64 { return 1700000000000 },
	}

	// Phase 1: thread_room and user both absent → transient error (Nak).
	err = h.handle(ctx, oplogEvent{
		Collection:   "company_thread_subscriptions",
		Op:           "insert",
		FullDocument: fullDoc,
	})
	require.Error(t, err, "missing FK must return a transient error")
	assert.NotErrorIs(t, err, migration.ErrSkipped, "must not be skipped")
	assert.NotErrorIs(t, err, migration.ErrPoison, "must not be poison")

	// Phase 2: seed the thread_room and user into the target, re-run → success + INBOX event.
	tr := model.ThreadRoom{
		ID:              "tr_parent_1",
		ParentMessageID: "msg_parent_1",
		RoomID:          "room1",
		SiteID:          site,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	_, err = tgtDB.Collection("thread_rooms").InsertOne(ctx, tr)
	require.NoError(t, err)

	u := model.User{ID: "dave_uid", Account: "dave", SiteID: site}
	inserted, err := tgtStore.UpsertUserIfAbsent(ctx, u)
	require.NoError(t, err)
	require.True(t, inserted)

	err = h.handle(ctx, oplogEvent{
		Collection:   "company_thread_subscriptions",
		Op:           "insert",
		FullDocument: fullDoc,
	})
	require.NoError(t, err, "after FKs are seeded, handle must succeed")

	// Confirm the thread_subscription_upserted event landed on INBOX.
	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subject.InboxExternal(site, string(model.InboxThreadSubscriptionUpserted))},
	})
	require.NoError(t, err)

	var got jetstream.Msg
	require.Eventually(t, func() bool {
		batch, berr := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for msg := range batch.Messages() {
			assert.NoError(t, msg.Ack())
			got = msg
			return true
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "thread_subscription_upserted event must land on INBOX")

	require.NotNil(t, got)
	var outboxEvt model.InboxEvent
	require.NoError(t, json.Unmarshal(got.Data(), &outboxEvt))
	assert.Equal(t, model.InboxThreadSubscriptionUpserted, outboxEvt.Type)
	assert.Equal(t, site, outboxEvt.SiteID)
}

func TestMongoTargetStore_RoomMemberUpsertAndDelete(t *testing.T) {
	db := testutil.MongoDB(t, "collxform-rm")
	store := NewMongoTargetStore(db)
	ctx := context.Background()

	rm := model.RoomMember{
		ID: "legacyRandomId17ch", RoomID: "GENERAL",
		Ts:     time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Member: model.RoomMemberEntry{ID: "org-123", Type: model.RoomMemberOrg},
	}
	require.NoError(t, store.UpsertRoomMember(ctx, rm))
	var got model.RoomMember
	require.NoError(t, db.Collection("room_members").FindOne(ctx, bson.M{"_id": rm.ID}).Decode(&got))
	assert.Equal(t, "GENERAL", got.RoomID)
	assert.Equal(t, model.RoomMemberOrg, got.Member.Type)

	// Redelivery-idempotent: same _id replaced, still one doc.
	require.NoError(t, store.UpsertRoomMember(ctx, rm))
	n, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": rm.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	deleted, err := store.DeleteRoomMember(ctx, rm.ID)
	require.NoError(t, err)
	assert.True(t, deleted)
	n, err = db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": rm.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Delete of a never-migrated id is a no-op, not an error.
	deleted, err = store.DeleteRoomMember(ctx, "ghost")
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestRoomMembers_EndToEnd_InsertThenDelete(t *testing.T) {
	db := testutil.MongoDB(t, "collxform-rm-e2e")
	store := NewMongoTargetStore(db)
	ctx := context.Background()

	// Seed the user the individual entry resolves against.
	_, err := db.Collection("users").InsertOne(ctx, bson.M{"_id": "newU1", "account": "jdoe"})
	require.NoError(t, err)

	h := &handler{
		siteID:          "site1",
		roomMembersColl: "company_room_members",
		target:          store,
		lookups:         map[string]migration.SourceLookup{},
	}

	ins := oplogEvent{Op: "insert", Collection: "company_room_members",
		DocumentKey:  json.RawMessage(`{"_id":"srcE2E"}`),
		FullDocument: json.RawMessage(`{"_id":"srcE2E","rid":"GENERAL","member":{"type":"individual","id":"legacyU9","username":"jdoe"},"ts":{"$date":"2026-07-04T00:00:00Z"}}`)}
	require.NoError(t, h.handle(ctx, ins))

	var got model.RoomMember
	require.NoError(t, db.Collection("room_members").FindOne(ctx, bson.M{"_id": "srcE2E"}).Decode(&got))
	assert.Equal(t, "newU1", got.Member.ID)
	assert.Equal(t, "jdoe", got.Member.Account)

	del := oplogEvent{Op: "delete", Collection: "company_room_members",
		DocumentKey: json.RawMessage(`{"_id":"srcE2E"}`)}
	require.NoError(t, h.handle(ctx, del))
	n, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "srcE2E"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}
