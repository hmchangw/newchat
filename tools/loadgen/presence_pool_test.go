package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundRobinIndex(t *testing.T) {
	var rr roundRobin
	got := []int{rr.next(3), rr.next(3), rr.next(3), rr.next(3)}
	assert.Equal(t, []int{0, 1, 2, 0}, got)
}

func TestRoundRobinIndex_Concurrent(t *testing.T) {
	var rr roundRobin
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); _ = rr.next(4) }()
	}
	wg.Wait()
	// After n increments the counter has advanced by n; next index is deterministic.
	assert.Equal(t, int((uint64(n))%4), rr.next(4))
}

func TestDecodePresenceState(t *testing.T) {
	data := []byte(`{"account":"u-1","siteId":"s1","status":"online","timestamp":123}`)
	acc, status, ok := decodePresenceState(data)
	assert.True(t, ok)
	assert.Equal(t, "u-1", acc)
	assert.Equal(t, "online", string(status))

	_, _, ok = decodePresenceState([]byte(`not json`))
	assert.False(t, ok)
}
