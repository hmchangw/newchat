//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
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

	emitter := newMobileEmitter(&directNATSAsyncPub{nc: nc}, "site-a", 0)
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
		payload, err := natsutil.DecodePayload(msg)
		if err != nil {
			t.Logf("decode payload: %v", err)
			return
		}
		var evt model.PushNotificationEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
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

// directNATSAsyncPub bypasses JetStream so the test can observe pushes without the PUSH_NOTIFICATIONS stream.
type directNATSAsyncPub struct{ nc *nats.Conn }

func (d *directNATSAsyncPub) PublishMsg(_ context.Context, msg *nats.Msg) error {
	return d.nc.PublishMsg(msg)
}

func TestMongoThreadFollowers_Followers(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_test")
	ctx := context.Background()
	col := db.Collection("thread_rooms")

	_, err := col.InsertMany(ctx, []any{
		model.ThreadRoom{ID: "tr1", ParentMessageID: "parent-1", RoomID: "r1", SiteID: "site-a", ReplyAccounts: []string{"alice", "bob"}},
		model.ThreadRoom{ID: "tr2", ParentMessageID: "parent-2", RoomID: "r1", SiteID: "site-a", ReplyAccounts: []string{"carol"}},
	})
	require.NoError(t, err)

	tf := newMongoThreadFollowers(col)

	t.Run("returns replyAccounts for parent", func(t *testing.T) {
		got, err := tf.Followers(ctx, "parent-1")
		require.NoError(t, err)
		assert.Len(t, got, 2)
		assert.Contains(t, got, "alice")
		assert.Contains(t, got, "bob")
		assert.NotContains(t, got, "carol")
	})

	t.Run("empty parentMessageID returns empty set", func(t *testing.T) {
		got, err := tf.Followers(ctx, "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("unknown parent returns empty set", func(t *testing.T) {
		got, err := tf.Followers(ctx, "no-such-parent")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
