//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestNotificationWorker_CacheBackedFanOut(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_test")
	valkeyClient := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	natsURL := testutil.NATS(t)

	ctx := context.Background()
	subCol := db.Collection("subscriptions")
	threadRoomCol := db.Collection("thread_rooms")

	seedSubscriptions(t, ctx, subCol)

	cache := roomsubcache.NewValkeyCache(valkeyutil.WrapClusterClient(valkeyClient))
	loader := &mongoMemberLoader{col: subCol}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	pushSub := subscribePush(t, nc, "site-a")

	emitter := newMobileEmitter(&directNATSAsyncPub{nc: nc}, "chat.server.notification.push.site-a.send", 0)
	handler := NewHandler(HandlerDeps{
		Members:            lookup,
		Followers:          newMongoThreadFollowers(threadRoomCol),
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emitter,
		LargeRoomThreshold: 500,
	})

	evt := model.MessageEvent{
		SiteID: "site-a",
		Message: model.Message{
			ID:          "m1",
			RoomID:      "r1",
			UserID:      "alice",
			UserAccount: "alice",
			Content:     "hello",
			CreatedAt:   time.Now(),
		},
	}
	data, _ := json.Marshal(evt)
	require.NoError(t, handler.HandleMessage(ctx, data))

	got := pushSub.collect(t, 2*time.Second, 2)
	assert.ElementsMatch(t, []string{"bob", "carol"}, got)
}

func seedSubscriptions(t *testing.T, ctx context.Context, col *mongo.Collection) {
	t.Helper()
	_, err := col.InsertMany(ctx, []any{
		model.Subscription{ID: "s1", RoomID: "r1", User: model.SubscriptionUser{ID: "alice", Account: "alice"}},
		model.Subscription{ID: "s2", RoomID: "r1", User: model.SubscriptionUser{ID: "bob", Account: "bob"}},
		model.Subscription{ID: "s3", RoomID: "r1", User: model.SubscriptionUser{ID: "carol", Account: "carol"}},
	})
	require.NoError(t, err)
}

type pushCollector struct {
	mu      sync.Mutex
	gotAcct []string
	got     chan struct{}
}

func subscribePush(t *testing.T, nc *nats.Conn, siteID string) *pushCollector {
	t.Helper()
	c := &pushCollector{got: make(chan struct{}, 256)}
	sub, err := nc.Subscribe(subject.PushNotification(siteID), func(msg *nats.Msg) {
		var evt model.PushNotificationEvent
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Logf("decode push: %v", err)
			return
		}
		c.mu.Lock()
		c.gotAcct = append(c.gotAcct, evt.Accounts...)
		c.mu.Unlock()
		for range evt.Accounts {
			c.got <- struct{}{}
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return c
}

func (c *pushCollector) collect(t *testing.T, timeout time.Duration, want int) []string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		if len(c.gotAcct) >= want {
			out := append([]string(nil), c.gotAcct...)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		select {
		case <-c.got:
		case <-deadline:
			c.mu.Lock()
			defer c.mu.Unlock()
			t.Fatalf("collect timeout: got %v want %d", c.gotAcct, want)
			return nil
		}
	}
}

// directNATSAsyncPub bypasses JetStream so the test can observe pushes without the PUSH-NOTIFICATION stream.
type directNATSAsyncPub struct{ nc *nats.Conn }

func (d *directNATSAsyncPub) PublishMsg(_ context.Context, msg *nats.Msg) error {
	return d.nc.PublishMsg(msg)
}

func TestMongoThreadFollowers_Lookup(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_test")
	ctx := context.Background()
	col := db.Collection("thread_rooms")

	parentCreatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	_, err := col.InsertMany(ctx, []any{
		model.ThreadRoom{ID: "tr1", ParentMessageID: "parent-1", RoomID: "r1", SiteID: "site-a", ReplyAccounts: []string{"alice", "bob"}, ThreadParentCreatedAt: parentCreatedAt},
		model.ThreadRoom{ID: "tr2", ParentMessageID: "parent-2", RoomID: "r1", SiteID: "site-a", ReplyAccounts: []string{"carol"}},
	})
	require.NoError(t, err)

	tf := newMongoThreadFollowers(col)

	t.Run("returns replyAccounts for parent", func(t *testing.T) {
		got, err := tf.Lookup(ctx, "parent-1")
		require.NoError(t, err)
		assert.Len(t, got.Followers, 2)
		assert.Contains(t, got.Followers, "alice")
		assert.Contains(t, got.Followers, "bob")
		assert.NotContains(t, got.Followers, "carol")
	})

	t.Run("returns replyAccounts for a thread with no parent createdAt column", func(t *testing.T) {
		got, err := tf.Lookup(ctx, "parent-2")
		require.NoError(t, err)
		assert.Len(t, got.Followers, 1)
		assert.Contains(t, got.Followers, "carol")
	})

	t.Run("empty parentMessageID returns empty set", func(t *testing.T) {
		got, err := tf.Lookup(ctx, "")
		require.NoError(t, err)
		assert.Empty(t, got.Followers)
	})

	t.Run("unknown parent returns empty set", func(t *testing.T) {
		got, err := tf.Lookup(ctx, "no-such-parent")
		require.NoError(t, err)
		assert.Empty(t, got.Followers)
	})
}

// TestMongoMemberLoader_Load_HistorySharedSince verifies the aggregation loader
// maps the nested subscription shape into the flat roomsubcache.Member and
// converts historySharedSince (a Date) to epoch-ms — leaving it nil for
// full-access members (the $$REMOVE branch of the projection).
func TestMongoMemberLoader_Load_HistorySharedSince(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_loader")
	ctx := context.Background()
	col := db.Collection("subscriptions")

	shared := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	_, err := col.InsertMany(ctx, []any{
		// restricted member: history-shared window set, muted
		model.Subscription{
			ID: "s1", RoomID: "rX", RoomType: model.RoomTypeChannel,
			User:               model.SubscriptionUser{ID: "alice", Account: "alice"},
			Muted:              true,
			HistorySharedSince: &shared,
		},
		// full-access member: no history-shared window (nil), a bot
		model.Subscription{
			ID: "s2", RoomID: "rX", RoomType: model.RoomTypeChannel,
			User: model.SubscriptionUser{ID: "botty", Account: "botty", IsBot: true},
		},
		// different room: must not leak into the rX result
		model.Subscription{
			ID: "s3", RoomID: "rY", RoomType: model.RoomTypeChannel,
			User: model.SubscriptionUser{ID: "carol", Account: "carol"},
		},
	})
	require.NoError(t, err)

	loader := &mongoMemberLoader{col: col}
	got, err := loader.Load(ctx, "rX")
	require.NoError(t, err)
	require.Len(t, got, 2)

	byAccount := make(map[string]roomsubcache.Member, len(got))
	for _, m := range got {
		byAccount[m.Account] = m
	}

	alice, ok := byAccount["alice"]
	require.True(t, ok)
	assert.Equal(t, "alice", alice.ID, "nested u._id maps to flat ID")
	assert.Equal(t, model.RoomTypeChannel, alice.RoomType)
	assert.False(t, alice.IsBot)
	assert.True(t, alice.Muted)
	require.NotNil(t, alice.HistorySharedSince, "restricted member must carry the window")
	assert.Equal(t, shared.UnixMilli(), *alice.HistorySharedSince, "Date converted to epoch-ms")

	botty, ok := byAccount["botty"]
	require.True(t, ok)
	assert.Equal(t, "botty", botty.ID)
	assert.True(t, botty.IsBot, "nested u.isBot maps to flat IsBot")
	assert.Nil(t, botty.HistorySharedSince, "full-access member must have nil HistorySharedSince")
}

func seedUserSettings(t *testing.T, ctx context.Context, col *mongo.Collection) {
	t.Helper()
	docs := []any{
		bson.M{
			"_id": "u-muted", "account": "muted-user", "active": true,
			"settings": bson.M{
				"muteAllNotifications":             true,
				"alwaysAllowPriorityNotifications": true,
				"priorityContacts":                 []string{"alice", "helper.bot"},
			},
		},
		bson.M{
			"_id": "u-partial", "account": "partial-user", "active": true,
			"settings": bson.M{"showNotificationsInCall": true},
		},
		bson.M{"_id": "u-nosettings", "account": "nosettings-user", "active": true},
		bson.M{"_id": "u-nofield", "account": "noactive-user"}, // missing active → active
		bson.M{
			"_id": "u-inactive", "account": "inactive-user", "active": false,
			"settings": bson.M{"muteAllNotifications": true},
		},
	}
	_, err := col.InsertMany(ctx, docs)
	require.NoError(t, err)
}

func TestMongoUserSettings_Snapshot_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_settings")
	ctx := context.Background()
	usersCol := db.Collection("users")
	seedUserSettings(t, ctx, usersCol)

	s := newMongoUserSettings(usersCol, 512, 5*time.Second)
	got, err := s.Snapshot(ctx, []string{
		"muted-user", "partial-user", "nosettings-user", "noactive-user",
		"inactive-user", "absent-user",
	})
	require.NoError(t, err)

	muted := got["muted-user"]
	assert.True(t, muted.muteAll)
	assert.True(t, muted.allowPriority)
	assert.False(t, muted.showInCall, "unset field resolves to false")
	assert.True(t, muted.isPriority("alice"))
	assert.True(t, muted.isPriority("helper.bot"), "bot accounts are valid priority contacts")
	assert.False(t, muted.isPriority("bob"))

	partial := got["partial-user"]
	assert.False(t, partial.muteAll)
	assert.True(t, partial.showInCall)

	assert.Contains(t, got, "nosettings-user", "an active user always appears in the map, even when zero-filled")
	assert.Equal(t, notifSettings{}, got["nosettings-user"], "no settings sub-document → zero value")
	assert.Contains(t, got, "noactive-user", "missing active field counts as active")
	assert.Equal(t, notifSettings{}, got["noactive-user"], "no settings sub-document → zero value")

	assert.NotContains(t, got, "inactive-user", "active:false is treated as absent, not zero-filled")
	assert.NotContains(t, got, "absent-user", "unknown account is absent, not zero-filled")
}

func TestMongoUserSettings_ChunkingBoundary_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_settings_chunk")
	ctx := context.Background()
	usersCol := db.Collection("users")

	accounts := make([]string, 0, 7)
	docs := make([]any, 0, 7)
	for i := 0; i < 7; i++ {
		account := fmt.Sprintf("chunk-user-%d", i)
		accounts = append(accounts, account)
		docs = append(docs, bson.M{
			"_id": account, "account": account, "active": true,
			"settings": bson.M{"muteAllNotifications": true},
		})
	}
	_, err := usersCol.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Batch size below the seeded count forces ceil(7/3) = 3 chunked $in queries.
	s := newMongoUserSettings(usersCol, 3, 5*time.Second)
	got, err := s.Snapshot(ctx, accounts)
	require.NoError(t, err)

	assert.Len(t, got, 7, "every chunk's results must be merged into one map")
	for _, a := range accounts {
		assert.True(t, got[a].muteAll, "account %s", a)
	}
}

func TestMongoUserSettings_FailsOpenOnReadError_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_settings_failopen")
	ctx := context.Background()
	usersCol := db.Collection("users")
	seedUserSettings(t, ctx, usersCol)

	s := newMongoUserSettings(usersCol, 512, 5*time.Second)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	got, err := s.Snapshot(cancelCtx, []string{"muted-user", "partial-user"})
	require.NoError(t, err, "Snapshot must never return an error, even when the read fails")
	assert.Empty(t, got, "a failed read yields an empty map so every account defaults to push")
}
