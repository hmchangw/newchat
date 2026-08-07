package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSortKeyCache_DisabledOnNonPositiveSizeOrTTL(t *testing.T) {
	cases := []struct {
		name string
		size int
		ttl  time.Duration
	}{
		{"zero size", 0, time.Second},
		{"negative size", -1, time.Second},
		{"zero ttl", 100, 0},
		{"negative ttl", 100, -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Nil(t, newSortKeyCache(tc.size, tc.ttl), "non-positive size/ttl must disable the cache")
		})
	}
}

func TestSortKeyCache_NilIsSafeAndAlwaysMisses(t *testing.T) {
	var c *sortKeyCache
	c.add("r1", roomSortKey{Name: "Eng"}) // must not panic
	_, ok := c.get(context.Background(), "r1")
	assert.False(t, ok, "disabled cache never hits")
}

func TestSortKeyCache_AddGetRoundTrip(t *testing.T) {
	c := newSortKeyCache(10, time.Minute)
	require.NotNil(t, c)

	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	created := at.Add(-time.Hour)
	c.add("r1", roomSortKey{Name: "Eng", LastMsgAt: &at, CreatedAt: &created})

	got, ok := c.get(context.Background(), "r1")
	require.True(t, ok)
	assert.Equal(t, "Eng", got.Name)
	require.NotNil(t, got.LastMsgAt)
	assert.True(t, at.Equal(*got.LastMsgAt))
	require.NotNil(t, got.CreatedAt)
	assert.True(t, created.Equal(*got.CreatedAt))
	assert.False(t, got.Missing)
}

func TestSortKeyCache_MissOnUnknownID(t *testing.T) {
	c := newSortKeyCache(10, time.Minute)
	require.NotNil(t, c)
	_, ok := c.get(context.Background(), "never-added")
	assert.False(t, ok)
}

func TestSortKeyCache_NegativeEntryIsAHit(t *testing.T) {
	// A cached "no local room doc" marker must be distinguishable from a plain
	// miss, otherwise every request re-queries Mongo for cross-site rooms.
	c := newSortKeyCache(10, time.Minute)
	require.NotNil(t, c)

	c.add("r-remote", roomSortKey{Missing: true})

	got, ok := c.get(context.Background(), "r-remote")
	require.True(t, ok, "negative entry must be a cache hit")
	assert.True(t, got.Missing)
}

func TestSortKeyCache_EntriesExpireAfterTTL(t *testing.T) {
	c := newSortKeyCache(10, 20*time.Millisecond)
	require.NotNil(t, c)

	c.add("r1", roomSortKey{Name: "Eng"})
	_, ok := c.get(context.Background(), "r1")
	require.True(t, ok, "entry readable before TTL")

	assert.Eventually(t, func() bool {
		_, ok := c.get(context.Background(), "r1")
		return !ok
	}, time.Second, 10*time.Millisecond, "entry must expire after the TTL")
}
