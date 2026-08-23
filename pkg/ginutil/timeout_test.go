package ginutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timeoutEngine builds a router with only the timeout middleware in front of h.
func timeoutEngine(t *testing.T, d time.Duration, h gin.HandlerFunc) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(d))
	r.GET("/x", h)
	return r
}

// Handlers derive their budget from the request context, so the deadline has
// to be on it rather than tracked alongside.
func TestTimeout_SetsDeadline(t *testing.T) {
	var deadline time.Time
	var ok bool
	r := timeoutEngine(t, 5*time.Second, func(c *gin.Context) {
		deadline, ok = c.Request.Context().Deadline()
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	require.True(t, ok, "the handler must run under a deadline")
	assert.WithinDuration(t, time.Now().Add(5*time.Second), deadline, time.Second)
}

// An overrun must cancel, not merely elapse.
func TestTimeout_CancelsExpiredRequest(t *testing.T) {
	var err error
	r := timeoutEngine(t, time.Millisecond, func(c *gin.Context) {
		<-c.Request.Context().Done()
		err = c.Request.Context().Err()
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// 0 disables the deadline; config rejects it so this stays a library concern.
func TestTimeout_NonPositiveDisables(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		var ok bool
		r := timeoutEngine(t, d, func(c *gin.Context) {
			_, ok = c.Request.Context().Deadline()
			c.Status(http.StatusOK)
		})
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
		assert.False(t, ok, "a non-positive timeout must leave the context alone")
	}
}

// The deadline must be released when the handler returns, not left to expire.
func TestTimeout_ReleasesContextAfterHandler(t *testing.T) {
	var reqCtx = make(chan error, 1)
	r := timeoutEngine(t, time.Hour, func(c *gin.Context) {
		ctx := c.Request.Context()
		go func() { <-ctx.Done(); reqCtx <- ctx.Err() }()
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	select {
	case err := <-reqCtx:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("context was never cancelled — the deadline leaked")
	}
}
