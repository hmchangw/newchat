package main

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobCache_AddGetRemove(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	_, ok := c.get("k")
	assert.False(t, ok)

	c.add("k", cachedBlob{body: []byte("v"), contentType: "text/plain"})
	got, ok := c.get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), got.body)
	assert.Equal(t, "text/plain", got.contentType)

	c.remove("k")
	_, ok = c.get("k")
	assert.False(t, ok)
}

func TestBlobCache_AddRefusesOversized(t *testing.T) {
	c := newBlobCache(4, time.Hour, 3)
	c.add("k", cachedBlob{body: []byte("toolong")})
	_, ok := c.get("k")
	assert.False(t, ok, "body over maxObjectBytes must not be cached")
}

func TestBlobCache_TTLExpiry(t *testing.T) {
	c := newBlobCache(4, 50*time.Millisecond, 1024)
	c.add("k", cachedBlob{body: []byte("v")})
	_, ok := c.get("k")
	require.True(t, ok)
	assert.Eventually(t, func() bool {
		_, ok := c.get("k")
		return !ok
	}, time.Second, 10*time.Millisecond, "entry must expire after ttl")
}

func TestBlobCache_LRUEviction(t *testing.T) {
	c := newBlobCache(2, time.Hour, 1024)
	c.add("a", cachedBlob{body: []byte("a")})
	c.add("b", cachedBlob{body: []byte("b")})
	c.add("d", cachedBlob{body: []byte("d")}) // evicts "a"
	_, ok := c.get("a")
	assert.False(t, ok)
	_, ok = c.get("b")
	assert.True(t, ok)
	_, ok = c.get("d")
	assert.True(t, ok)
}

func TestBlobCache_Disabled(t *testing.T) {
	c := newBlobCache(0, time.Hour, 1024)
	c.add("k", cachedBlob{body: []byte("v")})
	_, ok := c.get("k")
	assert.False(t, ok)
	c.remove("k") // must not panic

	blob, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{body: []byte("loaded")}, true, nil
	})
	require.NoError(t, err)
	assert.True(t, cacheable)
	assert.Equal(t, []byte("loaded"), blob.body)
	_, ok = c.get("k") // disabled cache stored nothing
	assert.False(t, ok)
}

func TestBlobCache_LoadCacheable_CachesAndReuses(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	blob, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{body: []byte("v"), contentType: "x"}, true, nil
	})
	require.NoError(t, err)
	assert.True(t, cacheable)
	assert.Equal(t, []byte("v"), blob.body)
	got, ok := c.get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), got.body)
}

func TestBlobCache_LoadCacheable_NonCacheableNotStored(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	_, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{contentType: "x"}, false, nil
	})
	require.NoError(t, err)
	assert.False(t, cacheable)
	_, ok := c.get("k")
	assert.False(t, ok)
}

func TestBlobCache_LoadCacheable_PropagatesError(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	sentinel := errors.New("boom")
	_, _, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{}, false, sentinel
	})
	assert.ErrorIs(t, err, sentinel)
}

func TestBlobCache_LoadCacheable_SingleflightCollapses(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	var calls int
	var mu sync.Mutex
	release := make(chan struct{})
	start := make(chan struct{})

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				<-release // hold the flight open so peers coalesce
				return cachedBlob{body: []byte("v")}, true, nil
			})
			assert.NoError(t, err)
		}()
	}
	close(start)
	// allow goroutines to converge on the in-flight call, then release.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, calls, "singleflight must collapse concurrent misses into one loader call")
}
