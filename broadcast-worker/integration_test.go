//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, true)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		SiteID: "site-a",
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", Content: "hello", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("r1"), records[0].subject)

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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, true)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, true)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	keyStore := &fakeRoomKeyProvider{pair: nil}
	handler := NewHandler(store, us, pub, keyStore, true)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}

	// nil keyStore — encryption is disabled, handler must not consult it
	handler := NewHandler(store, us, pub, nil, false)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		SiteID: "site-a",
		Message: model.Message{
			ID: "mNoEnc", RoomID: "rNoEnc", UserID: "u1", UserAccount: "alice", Content: "plaintext please", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("rNoEnc"), records[0].subject)

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
