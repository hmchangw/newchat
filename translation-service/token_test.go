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

func TestTokenProvider_SendsJ1InBody(t *testing.T) {
	var gotBody, gotContentType, gotAuthorization string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			return
		}
		gotBody = string(b)
		gotContentType = r.Header.Get("Content-Type")
		gotAuthorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"token":"J2","expiresAt":"`+rfc3339In(time.Hour)+`"}`); err != nil {
			t.Errorf("write token response: %v", err)
		}
	}))
	defer srv.Close()

	p := newTokenProvider(srv.URL, "J1-secret", 5*time.Second, time.Minute)
	_, err := p.Token(context.Background())
	require.NoError(t, err)

	assert.JSONEq(t, `{"key":"J1-secret"}`, gotBody) // J1 is sent in the body...
	assert.Empty(t, gotAuthorization)                // ...not in an Authorization header
	assert.Equal(t, "application/json", gotContentType)
}

func TestTokenProvider_FetchErrors(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{"unauthorized status", http.StatusUnauthorized, "", "status 401"},
		{"malformed json", http.StatusOK, `{not json`, "decode access token response"},
		{"missing token", http.StatusOK, `{"token":"","expiresAt":"` + rfc3339In(time.Hour) + `"}`, "missing token"},
		{"invalid expiresAt", http.StatusOK, `{"token":"J2","expiresAt":"not-a-date"}`, "parse access token expiresAt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.status != http.StatusOK {
					w.WriteHeader(tc.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			p := newTokenProvider(srv.URL, "J1", 5*time.Second, time.Minute)
			_, err := p.Token(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
