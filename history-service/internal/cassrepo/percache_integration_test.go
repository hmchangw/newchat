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
	"github.com/hmchangw/chat/history-service/internal/models"
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

// probeBucket reads a bucket through a cache instance with a cold L1, so it
// observes what Valkey holds rather than what a previous probe promoted into its
// own memory. Reusing one instance across observations would see its own stale
// L1 after another replica busts the key.
func probeBucket(t *testing.T, vk valkeyutil.Client, roomID string, bucket int64) ([]models.Message, bucketcache.Lookup) {
	t.Helper()
	c, err := bucketcache.NewCache(vk, 1<<20, time.Minute)
	require.NoError(t, err)
	return c.Get(context.Background(), roomID, bucket)
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

// The cache is populated from loadSealedBucket, so that load must return rows in
// the sealed form Cassandra stores. If it decrypted here, every cached bucket
// would put plaintext message bodies into Valkey — a second copy outside the
// at-rest protection the enc_payload column exists to provide. Serving-side
// decryption is covered by the cachedDescFetcher unit tests.
func TestPerBucketCache_PopulatesFromSealedRows(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	bucket := msgbucket.New(percacheWindow).Of(base)

	// An at-rest row as the write path stores it: user-authored fields sealed in
	// enc_payload, clustering columns and sender left plaintext, msg empty.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"r-enc", bucket, base, "m-enc",
		map[string]interface{}{"id": "u1", "account": "alice"},
		[]byte("SEALED-CIPHERTEXT"),
		map[string]interface{}{"nonce": []byte("012345678901")},
	).Exec())

	repo, _ := newBucketCachedRepo(t, session, 2000, base.Add(48*time.Hour))

	rows, oversized, err := repo.loadSealedBucket(context.Background(), "r-enc", bucket, 2000)
	require.NoError(t, err)
	require.False(t, oversized)
	require.Len(t, rows, 1)
	assert.Equal(t, []byte("SEALED-CIPHERTEXT"), rows[0].EncPayload,
		"rows cached from here must stay sealed, or Valkey holds decrypted bodies")
	require.NotNil(t, rows[0].EncMeta)
	assert.Equal(t, []byte("012345678901"), rows[0].EncMeta.Nonce)
	assert.Empty(t, rows[0].Msg, "the populate path must not decrypt")
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

	// The bucket's rows are not cached, but the verdict is: the key holds an
	// oversized marker so later reads skip the probe that discovered it.
	sealed := msgbucket.New(percacheWindow).Of(base)
	require.True(t, valkeyHasKey(t, vk, bucketcache.Key("r1", sealed)),
		"the oversized verdict is remembered")

	rows, res := probeBucket(t, vk, "r1", sealed)
	assert.Equal(t, bucketcache.Oversized, res, "marker cached, not rows")
	assert.Nil(t, rows, "an oversized bucket's rows must never be cached")

	// Reading through the marker still returns every row via the live fallback.
	page2, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	assert.Len(t, page2.Data, 3, "a marked bucket is still served completely")
	for i := range page.Data {
		assert.Equal(t, page.Data[i].MessageID, page2.Data[i].MessageID,
			"the marked read must match the probing read row for row")
	}
}

// The marker must never wedge a bucket: a mutation busts the key, so the next
// read re-probes and re-decides rather than trusting a stale verdict.
func TestPerBucketCache_SizeGuard_MarkerClearedByMutation(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 3)

	now := base.Add(48 * time.Hour)
	repo, vk := newBucketCachedRepo(t, session, 1, now) // maxRows=1 → oversized
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	pr := PageRequest{PageSize: 20}

	page, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page.Data, 3)

	sealed := msgbucket.New(percacheWindow).Of(base)
	_, res := probeBucket(t, vk, "r1", sealed)
	require.Equal(t, bucketcache.Oversized, res, "bucket over the cap is marked")

	msg := page.Data[0]
	require.NoError(t, repo.UpdateMessageContent(ctx, &msg, "edited body", now))

	_, res = probeBucket(t, vk, "r1", sealed)
	require.Equal(t, bucketcache.Miss, res, "the edit's bust clears the oversized marker")

	// The re-read re-probes, still finds the bucket over the cap, and re-marks it
	// — while serving the edit through the live fallback.
	page2, err := repo.GetMessagesBefore(ctx, "r1", now, floor, pr)
	require.NoError(t, err)
	require.Len(t, page2.Data, 3)
	assert.Equal(t, "edited body", page2.Data[0].Msg, "read after bust reflects the edit")
	_, res = probeBucket(t, vk, "r1", sealed)
	assert.Equal(t, bucketcache.Oversized, res, "still over the cap → marked again")
}

// A DESC cursor minted while its bucket was the current (hot) bucket must stay on
// the live fetcher when replayed after that bucket seals: the cache can't honor an
// intra-bucket page state and would re-serve the top of the bucket, repeating rows.
// This exercises the hasResumeCursor guard in fillMessagePageCachedDesc.
func TestPerBucketCache_CursorMintedHotReplayedSealed(t *testing.T) {
	session := setupCassandra(t)
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5) // m0..m4, 1 min apart, all in Of(base)

	// Mutable clock: Of(base) is current at mint time, sealed at replay time.
	var clockNow time.Time
	t.Cleanup(func() { testutil.FlushValkey(t) })
	vk := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	bc, err := bucketcache.NewCache(vk, 1<<20, time.Minute)
	require.NoError(t, err)
	repo := NewRepository(session, msgbucket.New(percacheWindow), 365, nil,
		WithBucketCache(bc, 2000),
		withClock(func() time.Time { return clockNow }),
	)
	ctx := context.Background()
	floor := base.Add(-30 * percacheWindow)
	before := base.Add(10 * time.Minute) // excludes none of m0..m4

	// Mint the cursor while Of(base) is the current (hot) bucket.
	clockNow = base.Add(10 * time.Minute) // same window as base → hot
	pr, err := ParsePageRequest("", 2)
	require.NoError(t, err)
	page1, err := repo.GetMessagesBefore(ctx, "r1", before, floor, pr)
	require.NoError(t, err)
	require.Len(t, page1.Data, 2)
	require.True(t, page1.HasNext)
	require.NotEmpty(t, page1.NextCursor)
	assert.Equal(t, "m4", page1.Data[0].MessageID)
	assert.Equal(t, "m3", page1.Data[1].MessageID)

	sealedKey := bucketcache.Key("r1", msgbucket.New(percacheWindow).Of(base))
	require.False(t, valkeyHasKey(t, vk, sealedKey),
		"minting a cursor against the hot bucket must not cache it")

	// Seal Of(base), then replay the cursor. The guard keeps this on the live
	// fetcher, which honors the page state → the next two rows, not a repeat.
	clockNow = base.Add(48 * time.Hour)
	cur, err := NewCursor(page1.NextCursor)
	require.NoError(t, err)
	page2, err := repo.GetMessagesBefore(ctx, "r1", before, floor, PageRequest{Cursor: cur, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page2.Data, 2)
	assert.Equal(t, "m2", page2.Data[0].MessageID, "cursor replay must continue past page 1, not re-serve its top")
	assert.Equal(t, "m1", page2.Data[1].MessageID)

	seen := map[string]bool{}
	for _, m := range append(append([]models.Message{}, page1.Data...), page2.Data...) {
		require.False(t, seen[m.MessageID], "row %s returned twice across the cursor transition", m.MessageID)
		seen[m.MessageID] = true
	}
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
