//go:build integration

package cassrepo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/bucketcache"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

const percacheWindow = 24 * time.Hour

// newBucketCachedRepo wires a Repository over the given session with a per-bucket
// cache backed by the shared test Valkey, and a fixed clock so bucket sealedness
// is deterministic.
func newBucketCachedRepo(t *testing.T, session *gocql.Session, maxRows int, now time.Time) (*Repository, valkeyutil.Client) {
	t.Helper()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	vk := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	bc, err := bucketcache.NewCache(vk, 1<<20, time.Minute)
	require.NoError(t, err)
	repo := NewRepository(session, msgbucket.New(percacheWindow), 365, nil,
		WithBucketCache(bc, maxRows),
		withClock(func() time.Time { return now }),
	)
	return repo, vk
}

func valkeyHasKey(t *testing.T, vk valkeyutil.Client, key string) bool {
	t.Helper()
	_, err := vk.Get(context.Background(), key)
	if errors.Is(err, valkeyutil.ErrCacheMiss) {
		return false
	}
	require.NoError(t, err)
	return true
}

// A sealed read is served from the per-bucket cache even after the Cassandra
// rows are deleted — proving the whole bucket was cached and reused.
func TestPerBucketCache_ReuseAcrossReads(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5) // all in Of(base)

	now := base.Add(48 * time.Hour) // two windows later → Of(base) is sealed
	repo, _ := newBucketCachedRepo(t, session, 2000, now)
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	pr := PageRequest{PageSize: 20}

	page1, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page1.Data, 5)

	// Delete the underlying partition; the cache must still serve it.
	require.NoError(t, session.Query(
		`DELETE FROM messages_by_room WHERE room_id = ? AND bucket = ?`,
		"r1", msgbucket.New(percacheWindow).Of(base),
	).Exec())

	page2, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	assert.Len(t, page2.Data, 5, "served from the per-bucket cache after the Cassandra rows were deleted")
}

// The current (hot) bucket is never cached: a message inserted after a read is
// immediately visible, and no cache key is written for the current bucket.
func TestPerBucketCache_HotBucketAlwaysLive(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, session, "r1", "m0", base)

	now := base.Add(1 * time.Hour) // same window as base → current bucket
	repo, vk := newBucketCachedRepo(t, session, 2000, now)
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	pr := PageRequest{PageSize: 20}

	page1, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page1.Data, 1)

	current := msgbucket.New(percacheWindow).Of(now)
	assert.False(t, valkeyHasKey(t, vk, bucketcache.Key("r1", current)),
		"the current bucket must not be cached")

	// A new message in the current bucket shows up on the next read (live).
	seedMessage(t, session, "r1", "m1", base.Add(2*time.Minute))
	page2, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	assert.Len(t, page2.Data, 2, "current bucket is read live, so the new message is visible")
}

// A bucket exceeding the size guard is not cached; the read still returns all
// rows via the live fallback.
func TestPerBucketCache_SizeGuard(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 3) // three rows in Of(base)

	now := base.Add(48 * time.Hour)
	repo, vk := newBucketCachedRepo(t, session, 1, now) // maxRows=1 → oversized
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	pr := PageRequest{PageSize: 20}

	page, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	assert.Len(t, page.Data, 3, "oversized bucket still served correctly via live fallback")

	sealed := msgbucket.New(percacheWindow).Of(base)
	assert.False(t, valkeyHasKey(t, vk, bucketcache.Key("r1", sealed)),
		"an oversized bucket must not be cached")
}

// Editing a message busts its bucket, so a subsequent read reflects the edit.
func TestPerBucketCache_BustOnEdit(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, session, "r1", "m0", base)

	now := base.Add(48 * time.Hour)
	repo, vk := newBucketCachedRepo(t, session, 2000, now)
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	pr := PageRequest{PageSize: 20}

	page1, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page1.Data, 1)

	sealed := msgbucket.New(percacheWindow).Of(base)
	require.True(t, valkeyHasKey(t, vk, bucketcache.Key("r1", sealed)), "bucket cached after the first read")

	msg := page1.Data[0]
	require.NoError(t, repo.UpdateMessageContent(ctx, &msg, "edited body", now))
	assert.False(t, valkeyHasKey(t, vk, bucketcache.Key("r1", sealed)), "edit busts the message's bucket")

	page2, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page2.Data, 1)
	assert.Equal(t, "edited body", page2.Data[0].Msg, "read after bust reflects the edit")
}
