//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/bucketcache"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// TestCassandraStore_ThreadWrite_BustsParentBucket proves the invalidation this
// worker owes history-service.
//
// history-service caches whole sealed buckets, and both thread paths here
// rewrite the parent row — tcount / thread_last_msg_at, and thread_room_id on a
// first reply. All three are columns that cache serves. A parent is usually old
// enough that its bucket has sealed, so without this DEL a cached copy keeps
// serving the pre-reply values until its TTL, and the client (which gates the
// thread affordance on tcount > 0) shows no reply count at all.
//
// The test stands in for history-service by writing the cache key itself, then
// asserts the worker removed it. That is the whole contract between the two
// services: one key format, one DEL.
func TestCassandraStore_ThreadWrite_BustsParentBucket(t *testing.T) {
	t.Cleanup(func() { testutil.FlushValkey(t) })
	vk := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	cassSession := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, sizer, nil,
		WithBucketCacheInvalidator(bucketcache.NewInvalidator(vk)))
	ctx := context.Background()

	const roomID = "bust-room"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := sizer.Of(parentCreatedAt)
	key := bucketcache.Key(roomID, parentBucket)

	parentSender := &cassParticipant{ID: "u-parent", Account: "alice", EngName: "Alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID: "bust-parent", RoomID: roomID, UserID: "u-parent",
		CreatedAt: parentCreatedAt, Content: "parent message",
	}, parentSender, "site-a"))

	// Stand in for history-service having cached the parent's sealed bucket.
	require.NoError(t, vk.Set(ctx, key, "cached-bucket-blob", time.Hour))
	require.True(t, valkeyHasBucketKey(t, vk, key), "precondition: the bucket is cached")

	replyCreatedAt := parentCreatedAt.Add(5 * time.Minute)
	replySender := &cassParticipant{ID: "u-replier", Account: "bob", EngName: "Bob"}
	_, err := store.SaveThreadMessage(ctx, &model.Message{
		ID: "bust-reply-1", RoomID: roomID, UserID: "u-replier",
		Content: "first reply", CreatedAt: replyCreatedAt,
		ThreadParentMessageID:        "bust-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}, replySender, "site-a", "tr-bust-1")
	require.NoError(t, err)

	assert.False(t, valkeyHasBucketKey(t, vk, key),
		"the thread write must invalidate the parent's cached bucket")
}

func valkeyHasBucketKey(t *testing.T, vk valkeyutil.Client, key string) bool {
	t.Helper()
	_, err := vk.Get(context.Background(), key)
	return err == nil
}
