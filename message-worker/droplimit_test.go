package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropLimiter_Allow(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	l := newDropLimiter(3, time.Minute, func() time.Time { return now })

	for i := 1; i <= 3; i++ {
		assert.True(t, l.Allow(), "drop %d is inside the per-minute cap", i)
	}
	assert.False(t, l.Allow(), "the 4th drop in the window must be refused")
	assert.False(t, l.Allow(), "and stay refused for the rest of the window")
}

func TestDropLimiter_WindowRolls(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	l := newDropLimiter(2, time.Minute, func() time.Time { return now })

	require.True(t, l.Allow())
	require.True(t, l.Allow())
	require.False(t, l.Allow())

	// Part-way through the window the cap still holds — no sleeping, the clock is injected.
	now = now.Add(59 * time.Second)
	assert.False(t, l.Allow(), "the window has not elapsed yet")

	now = now.Add(time.Second)
	assert.True(t, l.Allow(), "a fresh window restores the budget")
	assert.True(t, l.Allow())
	assert.False(t, l.Allow(), "and caps it again")
}

func TestDropLimiter_NilNeverAllows(t *testing.T) {
	// A policy built without a limiter has no brake, so the fail-safe answer is to
	// refuse the drop: a missing guard must cost retries, never messages.
	var l *dropLimiter
	assert.False(t, l.Allow())
}

func TestDropLimiter_ZeroMaxNeverAllows(t *testing.T) {
	l := newDropLimiter(0, time.Minute, nil)
	assert.False(t, l.Allow(), "a zero cap is a full stop, not an open door")
}

func TestDropLimiter_DefaultClock(t *testing.T) {
	l := newDropLimiter(1, time.Minute, nil)
	assert.True(t, l.Allow())
	assert.False(t, l.Allow())
}

func TestDropLimiter_ConcurrentAllowRespectsTheCap(t *testing.T) {
	// settle runs on MAX_WORKERS goroutines, so the counter is shared mutable state.
	// -race plus an exact total is what proves the cap is not racy.
	now := time.Unix(1700000000, 0).UTC()
	l := newDropLimiter(50, time.Minute, func() time.Time { return now })

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 50, allowed, "exactly the cap may pass, never more")
}

func TestDropLimiter_UsedByHandler(t *testing.T) {
	// End-to-end through settle: the cap bounds destruction per pod per window, and
	// a refused drop comes back as a NAK so it can drop in a later window.
	now := time.Unix(1700000000, 0).UTC()
	clock := func() time.Time { return now }
	h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, true, 2, clock))

	settleOne := func() *fakeJetStreamMsg {
		msg := &fakeJetStreamMsg{numDelivered: 40, data: []byte(`{"message":{"id":"m-1","roomId":"r-1"}}`)}
		h.settle(context.Background(), msg, requestClassErr())
		return msg
	}

	first, second := settleOne(), settleOne()
	assert.True(t, first.acked)
	assert.True(t, second.acked)

	third := settleOne()
	assert.False(t, third.acked, "the cap must convert the drop into a retry, not a destruction")
	assert.True(t, third.naked)

	now = now.Add(time.Minute)
	fourth := settleOne()
	assert.True(t, fourth.acked, "drops resume once the window rolls, so loss is spread rather than refused forever")
}
