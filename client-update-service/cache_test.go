package main

import (
	"errors"
	"sync"
	"sync/atomic"
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

// TestBlobCache_LoadCacheable_InvalidatedDuringFillNotStored simulates an upload
// (remove) landing while a download's loader is mid-flight: the loaded blob must
// NOT be cached, so a stale artifact can't survive the invalidation until TTL.
func TestBlobCache_LoadCacheable_InvalidatedDuringFillNotStored(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	blob, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		// An upload invalidates the key while we are "loading" the old body.
		c.remove("k")
		return cachedBlob{body: []byte("stale"), contentType: "x"}, true, nil
	})
	require.NoError(t, err)
	assert.True(t, cacheable, "the caller still receives the loaded blob")
	assert.Equal(t, []byte("stale"), blob.body)
	_, ok := c.get("k")
	assert.False(t, ok, "a fill invalidated mid-flight must not be stored")
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
	var calls int32
	entered := make(chan struct{}) // closed once, when the first loader is in-flight
	release := make(chan struct{})
	var once sync.Once

	loader := func() (cachedBlob, bool, error) {
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(entered) })
		<-release // hold the singleflight call open so peers coalesce or hit the cache
		return cachedBlob{body: []byte("v")}, true, nil
	}

	const n = 8
	var wg sync.WaitGroup

	// First caller occupies the in-flight singleflight slot for key "k".
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, err := c.loadCacheable("k", loader)
		assert.NoError(t, err)
	}()
	<-entered // deterministic: the first loader is now inside singleflight

	// Peers launched now either coalesce onto the in-flight call or, once it
	// completes and fills the cache, hit the cache — either way loader stays at 1.
	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := c.loadCacheable("k", loader)
			assert.NoError(t, err)
		}()
	}
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "singleflight must collapse concurrent misses into one loader call")
}
