//go:build integration

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "broadcast_worker_test")
}

type recordingPublisher struct {
	mu      sync.Mutex
	records []publishRecord
}

func (p *recordingPublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, publishRecord{subject: subj, data: data})
	return nil
}

func (p *recordingPublisher) getRecords() []publishRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]publishRecord, len(p.records))
	copy(cp, p.records)
	return cp
}

func seedUsers(t *testing.T, db *mongo.Database) {
	t.Helper()
	_, err := db.Collection("users").InsertMany(context.Background(), []interface{}{
		model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲", EmployeeID: "E001"},
		model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", EngName: "Bob Chen", ChineseName: "鮑勃", EmployeeID: "E002"},
	})
	require.NoError(t, err)
}

type fakeRoomKeyProvider struct {
	pair *roomkeystore.VersionedKeyPair
}

func (f *fakeRoomKeyProvider) Get(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
	return f.pair, nil
}

func TestBroadcastWorker_ChannelRoom_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", Content: "hello", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("r1", true), records[0].subject)

	roomEvt, msg := decryptClientMessage(t, records[0].data, key)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, msg)
	require.NotNil(t, msg.Sender)
	assert.Equal(t, "u1", msg.Sender.UserID)

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r1"}).Decode(&room))
	assert.Equal(t, "m1", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_ChannelRoom_MentionAll_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r2", Name: "announcements", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m2", RoomID: "r2", UserID: "u1", UserAccount: "alice", Content: "hello @All", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r2"}).Decode(&room))
	require.NotNil(t, room.LastMentionAllAt)
	assert.WithinDuration(t, msgTime, *room.LastMentionAllAt, time.Millisecond)
}

func TestBroadcastWorker_ChannelRoom_IndividualMention_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r3", Name: "dev", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s5", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r3"},
		model.Subscription{ID: "s6", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r3"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m3", RoomID: "r3", UserID: "u1", UserAccount: "alice", Content: "hey @bob", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	roomEvt, _ := decryptClientMessage(t, records[0].data, key)
	require.Len(t, roomEvt.Mentions, 1)
	assert.Equal(t, "bob", roomEvt.Mentions[0].Account)
	assert.Equal(t, "鮑勃", roomEvt.Mentions[0].ChineseName)
	assert.Equal(t, "Bob Chen", roomEvt.Mentions[0].EngName)
	assert.Equal(t, "u-bob", roomEvt.Mentions[0].UserID)

	var subBob model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"u.account": "bob", "roomId": "r3"}).Decode(&subBob))
	assert.True(t, subBob.HasMention)

	var subAlice model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"u.account": "alice", "roomId": "r3"}).Decode(&subAlice))
	assert.False(t, subAlice.HasMention)
}

func TestBroadcastWorker_DMRoom_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "dm-1", Name: "", Type: model.RoomTypeDM, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s7", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "dm-1"},
		model.Subscription{ID: "s8", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "dm-1"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	keyStore := &fakeRoomKeyProvider{pair: nil}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m4", RoomID: "dm-1", UserID: "u1", UserAccount: "alice", Content: "hey", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 2)
	var subjects []string
	for _, rec := range records {
		subjects = append(subjects, rec.subject)
	}
	assert.ElementsMatch(t, []string{
		subject.UserRoomEvent("alice"),
		subject.UserRoomEvent("bob"),
	}, subjects)

	for _, rec := range records {
		var roomEvt model.RoomEvent
		require.NoError(t, json.Unmarshal(rec.data, &roomEvt))
		require.NotNil(t, roomEvt.Message)
		require.NotNil(t, roomEvt.Message.Sender)
		assert.Equal(t, "u1", roomEvt.Message.Sender.UserID)
		assert.Equal(t, "alice", roomEvt.Message.Sender.Account)
		assert.Equal(t, "愛麗絲", roomEvt.Message.Sender.ChineseName)
	}

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "dm-1"}).Decode(&room))
	assert.Equal(t, "m4", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "rNoEnc", Name: "plain", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "sN1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "rNoEnc"},
		model.Subscription{ID: "sN2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "rNoEnc"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}

	// nil keyStore — encryption is disabled, handler must not consult it
	handler := NewHandler(store, us, pub, nil, defaultParentFetcher, false, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "mNoEnc", RoomID: "rNoEnc", UserID: "u1", UserAccount: "alice", Content: "plaintext please", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("rNoEnc", true), records[0].subject)

	var roomEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(records[0].data, &roomEvt))
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, roomEvt.Message, "plaintext channel event must carry Message")
	assert.Empty(t, roomEvt.EncryptedMessage, "plaintext channel event must NOT carry EncryptedMessage")
	assert.Equal(t, "mNoEnc", roomEvt.Message.ID)
	assert.Equal(t, "plaintext please", roomEvt.Message.Content)
	require.NotNil(t, roomEvt.Message.Sender)
	assert.Equal(t, "u1", roomEvt.Message.Sender.UserID)
	assert.Equal(t, "alice", roomEvt.Message.Sender.Account)

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "rNoEnc"}).Decode(&room))
	assert.Equal(t, "mNoEnc", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_PersistsLastMessage_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-last", Name: "general", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	cached, err := newCachedMetaStore(store, 10, time.Minute)
	require.NoError(t, err)

	pub := &recordingPublisher{}
	h := NewHandler(cached, userstore.NewMongoStore(db.Collection("users")), pub, &fakeRoomKeyProvider{}, defaultParentFetcher, false, subject.RouteGlobal)

	msgTime := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:          "msg-last",
			RoomID:      "r-last",
			UserID:      "u-alice",
			UserAccount: "alice",
			Content:     "hi",
			CreatedAt:   msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, data))

	// Verify the room doc now has lastMsgAt/lastMsgId persisted.
	var got struct {
		LastMsgAt time.Time `bson:"lastMsgAt"`
		LastMsgID string    `bson:"lastMsgId"`
	}
	err = db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-last"}).Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, "msg-last", got.LastMsgID)
	assert.WithinDuration(t, msgTime, got.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_BulkUpdateRoomLastMessage_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertMany(ctx, []interface{}{
		model.Room{ID: "r-bulk-a", Name: "a", Type: model.RoomTypeChannel, SiteID: "site-a"},
		model.Room{ID: "r-bulk-b", Name: "b", Type: model.RoomTypeChannel, SiteID: "site-a"},
	})
	require.NoError(t, err)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	t1 := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	updates := map[string]roomLastMsgUpdate{
		"r-bulk-a": {msgID: "msg-a", at: t1},
		"r-bulk-b": {msgID: "msg-b", at: t2, lastMentionAllAt: t2},
	}
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, updates))

	var a, b struct {
		LastMsgAt        time.Time `bson:"lastMsgAt"`
		LastMsgID        string    `bson:"lastMsgId"`
		LastMentionAllAt time.Time `bson:"lastMentionAllAt"`
	}
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-bulk-a"}).Decode(&a))
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-bulk-b"}).Decode(&b))

	assert.Equal(t, "msg-a", a.LastMsgID)
	assert.WithinDuration(t, t1, a.LastMsgAt, time.Millisecond)
	assert.True(t, a.LastMentionAllAt.IsZero(), "no mention-all → field stays unset")

	assert.Equal(t, "msg-b", b.LastMsgID)
	assert.WithinDuration(t, t2, b.LastMsgAt, time.Millisecond)
	assert.WithinDuration(t, t2, b.LastMentionAllAt, time.Millisecond)
}

func TestBroadcastWorker_BulkUpdateRoomLastMessage_EmptyIsNoOp_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	require.NoError(t, store.BulkUpdateRoomLastMessage(context.Background(), nil))
	require.NoError(t, store.BulkUpdateRoomLastMessage(context.Background(), map[string]roomLastMsgUpdate{}))
}

func TestBroadcastWorker_GetThreadFollowers_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	// Seed a thread room document with replyAccounts (siteID isolation is handled
	// at the deployment level — each site has its own MongoDB instance).
	_, err := db.Collection("thread_rooms").InsertMany(ctx, []interface{}{
		bson.M{
			"_id":             "tr-1",
			"parentMessageId": "parent-1",
			"replyAccounts":   []string{"bob", "carol", ""},
		},
		bson.M{
			"_id":             "tr-3",
			"parentMessageId": "parent-2",
			"replyAccounts":   []string{"dave"},
		},
	})
	require.NoError(t, err)

	t.Run("returns followers with empty strings filtered", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "parent-1")
		require.NoError(t, err)
		// Empty string is filtered out.
		assert.Equal(t, map[string]struct{}{"bob": {}, "carol": {}}, followers)
	})

	t.Run("different parentMessageId returns correct subset", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "parent-2")
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"dave": {}}, followers)
	})

	t.Run("no document returns empty map", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "nonexistent-parent")
		require.NoError(t, err)
		assert.Empty(t, followers)
	})
}

func TestBroadcastWorker_EnsureIndexes_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	// EnsureIndexes should be idempotent — call it twice without error.
	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx))

	// Verify the compound index was created by listing indexes.
	// MongoDB driver v2 decodes nested documents as bson.D (not bson.M), so we
	// decode the index list into []bson.D and iterate element-by-element.
	cursor, err := db.Collection("thread_rooms").Indexes().List(ctx)
	require.NoError(t, err)
	var idxes []bson.D
	require.NoError(t, cursor.All(ctx, &idxes))

	var found bool
	for _, idx := range idxes {
		var gotKeys bson.D
		for _, elem := range idx {
			if elem.Key == "key" {
				if kd, ok := elem.Value.(bson.D); ok {
					gotKeys = kd
				}
			}
		}
		var hasParent, hasSite bool
		for _, kv := range gotKeys {
			if kv.Key == "parentMessageId" {
				hasParent = true
			}
			if kv.Key == "siteId" {
				hasSite = true
			}
		}
		if hasParent && hasSite {
			found = true
			break
		}
	}
	assert.True(t, found, "compound index on (parentMessageId, siteId) must exist")
}

func TestAdvanceSubscriptionLastSeen_OnlyAdvances(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	t1 := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s-adv", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r-adv", LastSeenAt: &t1,
	})
	require.NoError(t, err)

	read := func() time.Time {
		var sub model.Subscription
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-adv"}).Decode(&sub))
		require.NotNil(t, sub.LastSeenAt)
		return sub.LastSeenAt.UTC()
	}

	t2 := t1.Add(time.Minute)
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "r-adv", "alice", t2))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "newer time advances")

	t0 := t1.Add(-time.Minute)
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "r-adv", "alice", t0))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "$max never regresses")

	// Missing subscription is a best-effort no-op.
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "no-room", "nobody", t2))
}

func TestSetSubscriptionMentions_ReadGuard_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	msgAt := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	readAt := msgAt.Add(time.Minute) // already read past the message

	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s-read", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r-mention", LastSeenAt: &readAt},
		model.Subscription{ID: "s-unread", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r-mention"}, // lastSeenAt never set (omitempty)
	})
	require.NoError(t, err)

	require.NoError(t, store.SetSubscriptionMentions(ctx, "r-mention", []string{"alice", "bob"}, msgAt))

	var alice, bob model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-read"}).Decode(&alice))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-unread"}).Decode(&bob))

	assert.False(t, alice.HasMention, "already-read subscription must not be re-flagged (#467)")
	assert.True(t, bob.HasMention, "never-read subscription (lastSeenAt absent) must still be flagged")
}

func TestBroadcastWorker_GetHistorySharedSince_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	shared := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "hss1", User: model.SubscriptionUser{ID: "u-al", Account: "alice"}, RoomID: "r-hss", HistorySharedSince: &shared},
		model.Subscription{ID: "hss2", User: model.SubscriptionUser{ID: "u-bo", Account: "bob"}, RoomID: "r-hss"},
	})
	require.NoError(t, err)

	got, err := store.GetHistorySharedSince(ctx, "r-hss", []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	require.NotNil(t, got["alice"])
	assert.Equal(t, shared.UnixMilli(), got["alice"].UTC().UnixMilli())
	bobWindow, bobPresent := got["bob"]
	require.True(t, bobPresent, "member with a nil window must still be present in the map")
	assert.Nil(t, bobWindow, "member without window decodes to nil")
	_, present := got["carol"]
	assert.False(t, present, "non-member is absent from the map")

	// Empty accounts short-circuits without a query.
	empty, err := store.GetHistorySharedSince(ctx, "r-hss", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// natsConnPublisher lets the test assert on what a client receives, not a fake.
type natsConnPublisher struct{ nc *nats.Conn }

func (p *natsConnPublisher) Publish(_ context.Context, subj string, data []byte) error {
	if err := p.nc.Publish(subj, data); err != nil {
		return fmt.Errorf("publish to %s: %w", subj, err)
	}
	return nil
}

// dave never replied, so the per-follower fan-out skips him; the panel must not.
func TestBroadcastWorker_ThreadViewSubject_NonFollowerReceivesReply_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	seedUsers(t, db)

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-view", Name: "general", Type: model.RoomTypeChannel, UserCount: 3, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("thread_rooms").InsertOne(ctx, bson.M{
		"_id":             "tr-view",
		"parentMessageId": "parent-view",
		"replyAccounts":   []string{"alice", "bob"},
	})
	require.NoError(t, err)

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	h := NewHandler(store, userstore.NewMongoStore(db.Collection("users")), &natsConnPublisher{nc: nc},
		&fakeRoomKeyProvider{}, stubParentFetcher{}, false, subject.RouteGlobal, withThreadViewSubject(true))

	// dave follows nothing; his only subscription is the open panel.
	viewerSub, err := nc.SubscribeSync(subject.RoomThreadEvent("r-view", "parent-view", true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = viewerSub.Unsubscribe() })
	// A follower's per-user lane, to prove both lanes carry the same event.
	followerSub, err := nc.SubscribeSync(subject.UserRoomEvent("bob"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = followerSub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	msgTime := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	data, err := json.Marshal(model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-view", RoomID: "r-view", UserID: "u-alice", UserAccount: "alice",
			Content: "a reply nobody following", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-view", TShow: false,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, data))

	viewerMsg, err := viewerSub.NextMsg(5 * time.Second)
	require.NoError(t, err, "a non-follower with the panel open must receive the reply")

	var viewerEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(viewerMsg.Data, &viewerEvt))
	assert.Equal(t, model.RoomEventNewThreadMessage, viewerEvt.Type)
	assert.Equal(t, "r-view", viewerEvt.RoomID)
	require.NotNil(t, viewerEvt.Message)
	assert.Equal(t, "reply-view", viewerEvt.Message.ID)

	followerMsg, err := followerSub.NextMsg(5 * time.Second)
	require.NoError(t, err, "the per-follower lane must be unaffected")
	assert.Equal(t, followerMsg.Data, viewerMsg.Data, "both lanes carry the identical envelope")
}

// A same-site room keeps its thread lane local; the global subject stays silent.
func TestBroadcastWorker_ThreadViewSubject_SameSiteRoomRoutesLocal_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	seedUsers(t, db)

	sameSite := false
	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-local", Name: "general", Type: model.RoomTypeChannel, UserCount: 2,
		SiteID: "site-a", CrossSite: &sameSite,
	})
	require.NoError(t, err)

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	h := NewHandler(store, userstore.NewMongoStore(db.Collection("users")), &natsConnPublisher{nc: nc},
		&fakeRoomKeyProvider{}, stubParentFetcher{}, false, subject.RouteLocal, withThreadViewSubject(true))

	localSub, err := nc.SubscribeSync(subject.RoomThreadEvent("r-local", "parent-local", false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localSub.Unsubscribe() })
	globalSub, err := nc.SubscribeSync(subject.RoomThreadEvent("r-local", "parent-local", true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = globalSub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	msgTime := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	data, err := json.Marshal(model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-local", RoomID: "r-local", UserID: "u-alice", UserAccount: "alice",
			Content: "same-site reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-local", TShow: false,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, data))

	_, err = localSub.NextMsg(5 * time.Second)
	require.NoError(t, err, "same-site thread events ride the local namespace")

	_, err = globalSub.NextMsg(200 * time.Millisecond)
	assert.Error(t, err, "a same-site room must not leak its thread lane onto the global namespace")
}

// The view lane rides the room namespace, which any authenticated client may
// subscribe to, so what actually reaches the wire must be ciphertext a room-key
// holder can open — and nothing a non-member could read.
func TestBroadcastWorker_ThreadViewSubject_EncryptedForRoomNamespace_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	seedUsers(t, db)

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-enc", Name: "general", Type: model.RoomTypeChannel, UserCount: 3, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("thread_rooms").InsertOne(ctx, bson.M{
		"_id": "tr-enc", "parentMessageId": "parent-enc", "replyAccounts": []string{"bob"},
	})
	require.NoError(t, err)

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	secret := make([]byte, 32)
	_, err = rand.Read(secret)
	require.NoError(t, err)
	key := &roomkeystore.VersionedKeyPair{Version: 7, KeyPair: roomkeystore.RoomKeyPair{PrivateKey: secret}}

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	h := NewHandler(store, userstore.NewMongoStore(db.Collection("users")), &natsConnPublisher{nc: nc},
		&fakeRoomKeyProvider{pair: key}, stubParentFetcher{}, true, subject.RouteGlobal, withThreadViewSubject(true))

	viewerSub, err := nc.SubscribeSync(subject.RoomThreadEvent("r-enc", "parent-enc", true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = viewerSub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	msgTime := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	data, err := json.Marshal(model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-enc", RoomID: "r-enc", UserID: "u-alice", UserAccount: "alice",
			Content: "secret thread body", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-enc", TShow: false,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, data))

	msg, err := viewerSub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.NotContains(t, string(msg.Data), "secret thread body", "the body must not cross the wire in the clear")

	evt, clientMsg := decryptClientMessage(t, msg.Data, key)
	assert.Equal(t, model.RoomEventNewThreadMessage, evt.Type)
	assert.Equal(t, "secret thread body", clientMsg.Content)
}

// TestBroadcastWorker_MentionFederation_Integration is the producer leg of the
// cross-site badge chain: a real message mentioning a remote-homed user must
// land on the local OUTBOX stream as an OutboxEvent addressed to that user's
// site, while the local-homed mentionee is badged in Mongo and never federated.
// outbox-worker's own suite covers the OUTBOX→INBOX forward, and inbox-worker's
// covers the destination write.
func TestBroadcastWorker_MentionFederation_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-fed", Name: "dev", Type: model.RoomTypeChannel, UserCount: 3, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "sf1", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: "r-fed"},
		model.Subscription{ID: "sf2", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: "r-fed"},
		model.Subscription{ID: "sf3", User: model.SubscriptionUser{ID: "u-carol", Account: "carol"}, RoomID: "r-fed"},
	})
	require.NoError(t, err)
	seedUsers(t, db)
	// carol is homed on site-b, so her badge has to cross sites.
	_, err = db.Collection("users").InsertOne(ctx,
		model.User{ID: "u-carol", Account: "carol", SiteID: "site-b", EngName: "Carol Lin"})
	require.NoError(t, err)

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	outboxCfg := stream.Outbox("site-a")
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: outboxCfg.Name, Subjects: outboxCfg.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), outboxCfg.Name) })

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	key := testRoomKey(t)
	handler := NewHandler(store, us, &recordingPublisher{}, &fakeRoomKeyProvider{pair: key},
		defaultParentFetcher, true, subject.RouteGlobal,
		withOutboxFederation("site-a", func(ctx context.Context, subj string, data []byte, msgID string) error {
			_, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID))
			return err
		}))

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m-fed", RoomID: "r-fed", UserID: "u-alice", UserAccount: "alice",
			Content: "hey @bob @carol", CreatedAt: msgTime,
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.HandleMessage(ctx, data))

	// Both mentionees are badged locally; only the remote one is federated.
	for _, account := range []string{"bob", "carol"} {
		var sub model.Subscription
		require.NoError(t, db.Collection("subscriptions").
			FindOne(ctx, bson.M{"u.account": account, "roomId": "r-fed"}).Decode(&sub))
		assert.True(t, sub.HasMention, "account %s", account)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, outboxCfg.Name, jetstream.ConsumerConfig{
		Name: "mention-fed-assert", FilterSubject: subject.OutboxWildcard("site-a"),
	})
	require.NoError(t, err)
	// Fetch exactly the one expected message so the pull returns as soon as it
	// lands; NumPending then proves no second destination was federated.
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	require.NoError(t, err)

	var got []*nats.Msg
	for msg := range msgs.Messages() {
		got = append(got, &nats.Msg{Subject: msg.Subject(), Data: msg.Data()})
		require.NoError(t, msg.Ack())
	}
	require.NoError(t, msgs.Error())
	require.Len(t, got, 1)
	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxSubscriptionMention), got[0].Subject)

	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Zero(t, info.NumPending, "exactly one destination site should be federated")

	relay, envelope, payload := unwrapOutbox(t, got[0].Data)
	assert.Equal(t, "r-fed", relay.RoomID)
	assert.Equal(t, fmt.Sprintf("mention:r-fed:m-fed:%d:%s:site-b", msgTime.UnixMilli(), accountsDigest([]string{"carol"})), relay.DedupID)
	assert.Equal(t, model.InboxSubscriptionMention, envelope.Type)
	assert.Equal(t, "site-a", envelope.SiteID)
	assert.Equal(t, "site-b", envelope.DestSiteID)
	assert.Equal(t, "r-fed", payload.RoomID)
	assert.Equal(t, []string{"carol"}, payload.Accounts, "only the remote-homed mentionee")
	assert.Equal(t, msgTime.UnixMilli(), payload.MentionedAt)
	assert.NotZero(t, payload.Timestamp)
}
