package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
)

// fakeIncr is a minimal in-memory IncrEx stub. errKey scopes err to one counter.
type fakeIncr struct {
	counts map[string]int64
	err    error
	errKey string
	calls  int32
}

func newFakeIncr() *fakeIncr { return &fakeIncr{counts: map[string]int64{}} }

func (f *fakeIncr) IncrEx(_ context.Context, key string, _ time.Duration) (int64, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil && (f.errKey == "" || f.errKey == key) {
		return 0, f.err
	}
	f.counts[key]++
	return f.counts[key], nil
}

// mountRLTest wires a router with a fake bot principal and the rate-limit middleware.
func mountRLTest(t *testing.T, client incrExClient, perCaller, perGlobal int) (*gin.Engine, *int32) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var seen int32
	r.Use(func(c *gin.Context) {
		c.Set(ctxBotPrincipal, &session.Session{
			ID:      "hash-1",
			UserID:  "bot-user-id",
			Account: "myapp.bot",
			SiteID:  "site-a",
			Roles:   []string{"bot"},
		})
		c.Next()
	})
	r.POST("/bot", botRateLimit(client, perCaller, perGlobal), func(c *gin.Context) {
		atomic.AddInt32(&seen, 1)
		c.Status(http.StatusOK)
	})
	return r, &seen
}

func TestBotRateLimit(t *testing.T) {
	req := func() *http.Request { return httptest.NewRequest(http.MethodPost, "/bot", nil) }

	t.Run("under both limits admits request", func(t *testing.T) {
		client := newFakeIncr()
		r, seen := mountRLTest(t, client, 5, 100)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, int32(1), *seen)
		assert.Equal(t, int32(2), client.calls, "one IncrEx per counter (caller + global)")
	})

	t.Run("exceeding per-caller limit rejects with 429 and rate_limited_caller", func(t *testing.T) {
		client := newFakeIncr()
		r, seen := mountRLTest(t, client, 2, 100)

		for range 2 {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req())
			require.Equal(t, http.StatusOK, w.Code)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.NotEmpty(t, w.Header().Get("Retry-After"))
		var body map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "rate_limited_caller", body["reason"])
		// Per-caller rejection short-circuits; global counter was NOT touched on the rejected call.
		assert.Equal(t, int64(3), client.counts["botrl:caller:bot-user-id"])
		assert.Equal(t, int64(2), client.counts["botrl:global"])
		assert.Equal(t, int32(2), *seen, "handler must not run on rejected request")
	})

	t.Run("exceeding global limit rejects with 429 and rate_limited_global", func(t *testing.T) {
		client := newFakeIncr()
		r, _ := mountRLTest(t, client, 100, 2)

		for range 2 {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req())
			require.Equal(t, http.StatusOK, w.Code)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "rate_limited_global", body["reason"])
	})

	// Bots are critical: a Valkey outage must never take them down. Losing the ceiling
	// is the accepted cost — an unthrottled bot beats a bot that cannot send at all.
	t.Run("caller-counter error fails open and admits the request", func(t *testing.T) {
		client := newFakeIncr()
		client.err = errors.New("boom")
		r, seen := mountRLTest(t, client, 10, 100)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, int32(1), *seen, "handler must run when the limiter is unavailable")
	})

	t.Run("global-counter error fails open and admits the request", func(t *testing.T) {
		client := newFakeIncr()
		client.err = errors.New("boom")
		client.errKey = "botrl:global"
		r, seen := mountRLTest(t, client, 10, 100)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, int32(1), *seen)
		assert.Equal(t, int64(1), client.counts["botrl:caller:bot-user-id"],
			"caller counter succeeded before the global counter failed")
	})

	t.Run("outage does not mask a caller already over its limit", func(t *testing.T) {
		client := newFakeIncr()
		r, seen := mountRLTest(t, client, 1, 100)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		require.Equal(t, http.StatusOK, w.Code)

		// Valkey fails only from here on; the caller is already at its ceiling.
		client.err = errors.New("boom")
		w2 := httptest.NewRecorder()
		r.ServeHTTP(w2, req())
		assert.Equal(t, http.StatusOK, w2.Code, "fail-open admits — the counter is unreadable")
		assert.Equal(t, int32(2), *seen)
	})

	t.Run("zero limits disable the middleware (feature-off)", func(t *testing.T) {
		client := newFakeIncr()
		r, seen := mountRLTest(t, client, 0, 0)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, req())
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, int32(1), *seen)
		assert.Equal(t, int32(0), client.calls, "no IncrEx calls when both limits are 0")
	})
}
