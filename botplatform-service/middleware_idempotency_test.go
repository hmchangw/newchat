package main

import (
	"bytes"
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

// fakeSentinel records every SetNX so tests can assert on the composite opID structure.
type fakeSentinel struct {
	acquired map[string]bool
	setNXErr error
	delErr   error
	delCalls int32
	setCalls []string
}

func newFakeSentinel() *fakeSentinel { return &fakeSentinel{acquired: map[string]bool{}} }

func (f *fakeSentinel) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	f.setCalls = append(f.setCalls, key)
	if f.setNXErr != nil {
		return false, f.setNXErr
	}
	if f.acquired[key] {
		return false, nil
	}
	f.acquired[key] = true
	return true, nil
}

func (f *fakeSentinel) Del(_ context.Context, keys ...string) error {
	atomic.AddInt32(&f.delCalls, 1)
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.acquired, k)
	}
	return nil
}

type stubTime struct{ ns int64 }

func (s *stubTime) Now() time.Time { return time.Unix(0, s.ns) }

// mountIdemTest builds a router with a fake bot principal and idempotency middleware.
func mountIdemTest(t *testing.T, client sentinelClient, tp timeProvider, siteID string) (*gin.Engine, *int32) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var handlerCalls int32
	r.Use(func(c *gin.Context) {
		c.Set(ctxBotPrincipal, &session.Session{
			UserID:  "bot-user-id",
			Account: "myapp.bot",
			SiteID:  siteID,
			Roles:   []string{"bot"},
		})
		c.Next()
	})
	mw := botIdempotency(client, siteID, "sendRoom", 30*time.Second,
		func(c *gin.Context) string { return c.Param("roomID") }, tp)
	r.POST("/api/v1/rooms/:roomID/messages", mw, func(c *gin.Context) {
		atomic.AddInt32(&handlerCalls, 1)
		if s := c.Query("status"); s != "" {
			switch s {
			case "500":
				c.Status(http.StatusInternalServerError)
			case "400":
				c.Status(http.StatusBadRequest)
			default:
				c.Status(http.StatusOK)
			}
			return
		}
		c.Status(http.StatusOK)
	})
	return r, &handlerCalls
}

func idemPost(url string, body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestBotIdempotency(t *testing.T) {
	const siteID = "site-a"

	t.Run("first call acquires sentinel and handler runs", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, calls := mountIdemTest(t, client, tp, siteID)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, int32(1), *calls)
		require.Len(t, client.setCalls, 1)
		assert.True(t, len(client.setCalls[0]) > len("idem:"), "sentinel key must include opID hash")
	})

	t.Run("in-flight duplicate rejected with 409 and Retry-After", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, calls := mountIdemTest(t, client, tp, siteID)

		w1 := httptest.NewRecorder()
		r.ServeHTTP(w1, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		require.Equal(t, http.StatusOK, w1.Code)
		// Reinstate ownership to simulate the first call still in flight.
		for _, k := range client.setCalls {
			client.acquired[k] = true
		}

		w2 := httptest.NewRecorder()
		r.ServeHTTP(w2, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		assert.Equal(t, http.StatusConflict, w2.Code)
		assert.Equal(t, "1", w2.Header().Get("Retry-After"))
		var body map[string]any
		require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &body))
		assert.Equal(t, "in_flight", body["reason"])
		assert.Equal(t, int32(1), *calls, "handler must not run on rejected retry")
	})

	t.Run("same body different time bucket produces different opID", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, calls := mountIdemTest(t, client, tp, siteID)

		w1 := httptest.NewRecorder()
		r.ServeHTTP(w1, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		require.Equal(t, http.StatusOK, w1.Code)

		tp.ns += (61 * time.Second).Nanoseconds()

		w2 := httptest.NewRecorder()
		r.ServeHTTP(w2, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		assert.Equal(t, http.StatusOK, w2.Code)
		assert.Equal(t, int32(2), *calls, "both requests must reach the handler")
		require.Len(t, client.setCalls, 2)
		assert.NotEqual(t, client.setCalls[0], client.setCalls[1], "different bucket must yield different opID")
	})

	t.Run("different body produces different opID", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, calls := mountIdemTest(t, client, tp, siteID)

		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"a"}`)))
		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"b"}`)))

		assert.Equal(t, int32(2), *calls)
		require.Len(t, client.setCalls, 2)
		assert.NotEqual(t, client.setCalls[0], client.setCalls[1])
	})

	t.Run("success 200 releases sentinel", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, _ := mountIdemTest(t, client, tp, siteID)

		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"ok"}`)))
		assert.Equal(t, int32(1), atomic.LoadInt32(&client.delCalls))
	})

	t.Run("client-error 4xx releases sentinel", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, _ := mountIdemTest(t, client, tp, siteID)

		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages?status=400", []byte(`{"content":"bad"}`)))
		assert.Equal(t, int32(1), atomic.LoadInt32(&client.delCalls))
	})

	t.Run("server-error 5xx keeps sentinel", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, _ := mountIdemTest(t, client, tp, siteID)

		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages?status=500", []byte(`{"content":"boom"}`)))
		assert.Equal(t, int32(0), atomic.LoadInt32(&client.delCalls),
			"5xx must NOT release the sentinel — let it expire so the original handler is not raced")
	})

	t.Run("valkey error on SetNX surfaces as 500", func(t *testing.T) {
		client := newFakeSentinel()
		client.setNXErr = errors.New("boom")
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		r, calls := mountIdemTest(t, client, tp, siteID)

		w := httptest.NewRecorder()
		r.ServeHTTP(w, idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Equal(t, int32(0), *calls, "handler must not run when sentinel acquire fails")
	})

	t.Run("body available to downstream handler after middleware", func(t *testing.T) {
		client := newFakeSentinel()
		tp := &stubTime{ns: time.Second.Nanoseconds()}
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set(ctxBotPrincipal, &session.Session{UserID: "u1", SiteID: siteID, Roles: []string{"bot"}})
			c.Next()
		})
		mw := botIdempotency(client, siteID, "sendRoom", 30*time.Second,
			func(c *gin.Context) string { return c.Param("roomID") }, tp)
		var seen []byte
		r.POST("/x/:roomID", mw, func(c *gin.Context) {
			b, err := c.GetRawData()
			require.NoError(t, err)
			seen = b
			c.Status(http.StatusOK)
		})

		payload := []byte(`{"content":"body-must-round-trip"}`)
		r.ServeHTTP(httptest.NewRecorder(), idemPost("/x/r1", payload))
		assert.Equal(t, payload, seen, "middleware must not consume the body")
	})
}
