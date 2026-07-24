package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// accessTokenServer returns an httptest server that replies with a J2 token
// response. calls (if non-nil) counts invocations.
func accessTokenServer(t *testing.T, token, expiresAt string, calls *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls != nil {
			atomic.AddInt32(calls, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"token":"`+token+`","expiresAt":"`+expiresAt+`","username":"u","jwtRequestId":"j"}`)
	}))
}

func rfc3339In(d time.Duration) string {
	return time.Now().Add(d).UTC().Format(time.RFC3339)
}

func TestTokenProvider_FetchAndCache(t *testing.T) {
	var calls int32
	srv := accessTokenServer(t, "J2-abc", rfc3339In(time.Hour), &calls)
	defer srv.Close()

	p := newTokenProvider(srv.URL, "J1-secret", 5*time.Second, time.Minute)
	tok, err := p.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "J2-abc", tok)

	tok2, err := p.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "J2-abc", tok2)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls)) // second call served from cache
}

func TestTokenProvider_RefetchWhenExpired(t *testing.T) {
	var calls int32
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	// expiresAt is 30s after base, but skew is 60s => cache validity = base-30s => already expired at base.
	exp := base.Add(30 * time.Second).Format(time.RFC3339)
	srv := accessTokenServer(t, "J2-x", exp, &calls)
	defer srv.Close()

	p := newTokenProvider(srv.URL, "J1", 5*time.Second, time.Minute)
	p.now = func() time.Time { return base }

	_, err := p.Token(context.Background())
	require.NoError(t, err)
	_, err = p.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls)) // expired => refetched
}

func TestTokenProvider_ForceRefresh(t *testing.T) {
	var calls int32
	srv := accessTokenServer(t, "J2-tok", rfc3339In(time.Hour), &calls)
	defer srv.Close()

	p := newTokenProvider(srv.URL, "J1", 5*time.Second, time.Minute)
	tok, err := p.Token(context.Background())
	require.NoError(t, err)

	// Refresh with the stale token we just used => forces a refetch.
	_, err = p.Refresh(context.Background(), tok)
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))

	// Refresh with a value that isn't the current cached token => another goroutine
	// already refreshed; return cached without a redundant fetch.
	_, err = p.Refresh(context.Background(), "some-old-token")
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestTokenProvider_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	p := newTokenProvider(srv.URL, "J1", 5*time.Second, time.Minute)
	_, err := p.Token(context.Background())
	require.Error(t, err)
}
