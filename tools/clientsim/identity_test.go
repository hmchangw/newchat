package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshDelay_EightyPercentWithJitter(t *testing.T) {
	now := time.Now()
	expires := now.Add(2 * time.Hour)
	t.Run("midpoint rand is exactly 80 percent", func(t *testing.T) {
		d := refreshDelay(expires, now, func() float64 { return 0.5 })
		assert.InDelta(t, (96 * time.Minute).Seconds(), d.Seconds(), 1)
	})
	t.Run("jitter is multiplicative on the fraction, so 76-84 percent", func(t *testing.T) {
		// 0.80 * (1 +/- 0.05), matching useJwtRefresh.js. NOT 80 +/- 5
		// percentage points, which would be 75-85 and is a different rule.
		lo := refreshDelay(expires, now, func() float64 { return 0 })
		hi := refreshDelay(expires, now, func() float64 { return 1 })
		assert.InDelta(t, (2 * time.Hour * 76 / 100).Seconds(), lo.Seconds(), 1)
		assert.InDelta(t, (2 * time.Hour * 84 / 100).Seconds(), hi.Seconds(), 1)
	})
	t.Run("already expired means immediate", func(t *testing.T) {
		d := refreshDelay(now.Add(-time.Minute), now, func() float64 { return 0.5 })
		assert.LessOrEqual(t, d, time.Duration(0))
	})
}

func TestJWTCache_SingleMintInvariant_Proactive(t *testing.T) {
	// In proactive mode the connect callback must ONLY read the cache; a
	// refresh cycle costs exactly one Mint (spec §5.2 steps 3/6).
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-1", jwtModeProactive, mint)

	require.NoError(t, s.primeJWT(context.Background()))
	assert.Equal(t, int64(1), mint.calls.Load())

	// Three (re)connect callback invocations: no further mints.
	for i := 0; i < 3; i++ {
		jwtStr, err := s.userCB()
		require.NoError(t, err)
		assert.NotEmpty(t, jwtStr)
	}
	assert.Equal(t, int64(1), mint.calls.Load())

	// One proactive refresh cycle: exactly one more mint.
	require.NoError(t, s.refreshJWT(context.Background()))
	assert.Equal(t, int64(2), mint.calls.Load())
}

func TestJWTCache_ExpiryModeMintsOnExpiredCache(t *testing.T) {
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-1", jwtModeExpiry, mint)
	require.NoError(t, s.primeJWT(context.Background()))
	require.Equal(t, int64(1), mint.calls.Load())

	// Valid cache: callback reads it, no mint.
	_, err := s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(1), mint.calls.Load())

	// Expired cache: the next callback (the reconnect path) mints exactly once.
	s.cache.forceExpireForTest()
	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(2), mint.calls.Load())
}

func TestJWTCache_ProactiveCallbackNeverMintsEvenWhenExpired(t *testing.T) {
	// The proactive timer owns minting; an expired cache at callback time
	// still returns the stale token (the server will bounce it and the
	// refresh loop recovers) rather than minting from the callback.
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-1", jwtModeProactive, mint)
	require.NoError(t, s.primeJWT(context.Background()))
	s.cache.forceExpireForTest()

	_, err := s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(1), mint.calls.Load(), "proactive callback must not mint")
}

func TestSigCB_SignsWithClientNKey(t *testing.T) {
	s := newTestSimClient(t, "user-1", jwtModeProactive, &countingMinter{jwt: func() string { return "j" }})
	sig, err := s.sigCB([]byte("nonce"))
	require.NoError(t, err)
	assert.NotEmpty(t, sig)
}

func TestJWTCache_TracksExpiryFromClaims(t *testing.T) {
	expires := time.Now().Add(90 * time.Minute).Truncate(time.Second)
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, expires) }}
	s := newTestSimClient(t, "user-1", jwtModeProactive, mint)
	require.NoError(t, s.primeJWT(context.Background()))

	_, expAt := s.cache.get()
	assert.WithinDuration(t, expires, expAt, time.Second)
}

func TestSimClient_ProactiveRefreshLoop_MintsAndForcesReconnect(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(1200*time.Millisecond)) }}
	s := newTestSimClient(t, "user-lc", jwtModeProactive, mint)
	s.dial = func(context.Context) (simConn, error) { return fc, nil }
	s.resyncJitter = func() time.Duration { return 0 }
	startClient(t, s)

	// ~80% of 1.2s ≈ 0.9-1.0s to the first refresh.
	require.Eventually(t, func() bool { return mint.calls.Load() >= 2 }, 5*time.Second, 20*time.Millisecond,
		"proactive loop must re-mint")
	require.Eventually(t, func() bool { return fc.forceReconnects.Load() >= 1 }, 5*time.Second, 20*time.Millisecond,
		"refresh must force a reconnect to present the fresh JWT")
}
