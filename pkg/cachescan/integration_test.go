//go:build integration

package cachescan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/cachekeys"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTestsWithPrewarm(m, testutil.EnsureValkey) }

// TestScan_AgainstRealValkey exercises the go-redis adapter end to end: the
// cluster fan-out, cursor paging, classification, and MEMORY USAGE. The unit
// tests cover the arithmetic; this covers the wiring the fakes stand in for.
func TestScan_AgainstRealValkey(t *testing.T) {
	ctx := context.Background()
	client := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })

	const (
		metaKeys = 120
		subsKeys = 40
		idemKeys = 3
	)
	for i := range metaKeys {
		require.NoError(t, client.Set(ctx, cachekeys.RoomMeta(roomID(i)), `{"id":"r","type":"channel"}`, 0).Err())
	}
	for i := range subsKeys {
		require.NoError(t, client.Set(ctx, cachekeys.RoomSubs(roomID(i)), `[{"id":"u1","account":"alice"}]`, 0).Err())
	}
	for i := range idemKeys {
		require.NoError(t, client.Set(ctx, cachekeys.BotIdempotency(roomID(i)), "processing", 0).Err())
	}
	require.NoError(t, client.Set(ctx, "not:a:registered:cache", "x", 0).Err())

	// MinSamples is what keeps the 3-key idempotency cache measured despite a
	// 1-in-50 rate; without it that cache would report zero bytes.
	got, err := Scan(ctx, NewCluster(client), Options{ScanCount: 25, SampleRate: 50, MinSamples: 5})
	require.NoError(t, err)

	assert.Equal(t, int64(metaKeys+subsKeys+idemKeys+1), got.TotalKeys)

	meta := findUsage(t, got, "roommeta")
	assert.Equal(t, int64(metaKeys), meta.Keys)
	assert.Positive(t, meta.EstimatedBytes)

	subs := findUsage(t, got, "roomsubs")
	assert.Equal(t, int64(subsKeys), subs.Keys)

	idem := findUsage(t, got, "botidempotency")
	assert.Equal(t, int64(idemKeys), idem.Keys)
	assert.Positive(t, idem.EstimatedBytes, "MinSamples must cover a cache smaller than SampleRate")

	unknown := findUsage(t, got, cachekeys.Unclassified)
	assert.Equal(t, int64(1), unknown.Keys, "an unregistered key must surface as unclassified")

	assert.Positive(t, got.TotalBytes)
	assert.InDelta(t, 1.0, got.Share("roommeta")+got.Share("roomsubs")+
		got.Share("botidempotency")+got.Share(cachekeys.Unclassified), 1e-6,
		"the four populated caches must account for the whole keyspace")
}

// TestScan_EmptyKeyspaceAgainstRealValkey confirms a flushed cluster still
// reports every registered cache at zero rather than an empty report.
func TestScan_EmptyKeyspaceAgainstRealValkey(t *testing.T) {
	ctx := context.Background()
	client := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	testutil.FlushValkey(t)

	got, err := Scan(ctx, NewCluster(client), DefaultOptions())
	require.NoError(t, err)

	assert.Zero(t, got.TotalKeys)
	assert.NotEmpty(t, got.Caches)
	assert.Zero(t, findUsage(t, got, "roommeta").Keys)
}

func roomID(i int) string {
	return "r" + string(rune('a'+i/26)) + string(rune('a'+i%26))
}

func findUsage(t *testing.T, r Report, cache string) CacheUsage {
	t.Helper()
	for _, u := range r.Caches {
		if u.Cache == cache {
			return u
		}
	}
	t.Fatalf("cache %q absent from report", cache)
	return CacheUsage{}
}
