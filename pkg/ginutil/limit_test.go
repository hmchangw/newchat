package ginutil

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingEngine serves /x from a handler that announces its admission on the
// returned channel and then parks until release is closed.
func blockingEngine(t *testing.T, n int, opts ...LimiterOption) (r *gin.Engine, admitted <-chan struct{}, release chan struct{}) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rel := make(chan struct{})
	adm := make(chan struct{}, n)
	e := gin.New()
	e.Use(MaxConcurrency(n, opts...))
	e.GET("/x", func(c *gin.Context) {
		adm <- struct{}{}
		<-rel
		c.Status(http.StatusOK)
	})
	return e, adm, rel
}

// fillAndShed occupies every slot, waits until each is confirmed admitted, then
// issues one more request and returns its response. Waiting on the admission
// signal is what keeps this deterministic: probing before the slots are taken
// would let the probe win the slot and park instead.
func fillAndShed(t *testing.T, r *gin.Engine, admitted <-chan struct{}, n int) *httptest.ResponseRecorder {
	t.Helper()
	for i := 0; i < n; i++ {
		go func() {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-admitted:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d requests were admitted", i, n)
		}
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	return w
}

func TestMaxConcurrency_ShedsBeyondCap(t *testing.T) {
	r, admitted, release := blockingEngine(t, 1, WithRetryAfter(2*time.Second))
	defer close(release)

	w := fillAndShed(t, r, admitted, 1)

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "2", w.Header().Get("Retry-After"))
	assert.JSONEq(t,
		`{"code":"too_many_requests","reason":"overloaded","error":"server is at capacity, retry shortly"}`,
		w.Body.String())
}

func TestMaxConcurrency_DefaultRetryAfterIsOneSecond(t *testing.T) {
	r, admitted, release := blockingEngine(t, 1)
	defer close(release)

	assert.Equal(t, "1", fillAndShed(t, r, admitted, 1).Header().Get("Retry-After"))
}

func TestMaxConcurrency_ReleasesSlotAfterCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(MaxConcurrency(1))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		assert.Equal(t, http.StatusOK, w.Code, "each sequential request must reclaim the slot")
	}
}

func TestMaxConcurrency_ReleasesSlotOnPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), MaxConcurrency(1))
	r.GET("/boom", func(c *gin.Context) { panic("boom") })
	r.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ok", nil))
	assert.Equal(t, http.StatusOK, w.Code, "a panicking handler must not leak its slot")
}

func TestMaxConcurrency_NonPositiveDisablesLimiting(t *testing.T) {
	for _, n := range []int{0, -1} {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.Use(MaxConcurrency(n))
		r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := httptest.NewRecorder()
				r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
				assert.Equal(t, http.StatusOK, w.Code)
			}()
		}
		wg.Wait()
	}
}

func TestMaxConcurrency_OnShedObserverFires(t *testing.T) {
	var shed atomic.Int64
	r, admitted, release := blockingEngine(t, 1, WithOnShed(func() { shed.Add(1) }))
	defer close(release)

	require.Equal(t, http.StatusTooManyRequests, fillAndShed(t, r, admitted, 1).Code)
	assert.Equal(t, int64(1), shed.Load())
}

func TestMaxConcurrency_AdmitsUpToCapConcurrently(t *testing.T) {
	const capacity = 4
	r, admitted, release := blockingEngine(t, capacity)
	defer close(release)

	// fillAndShed only returns once all `capacity` requests were admitted at once.
	assert.Equal(t, http.StatusTooManyRequests, fillAndShed(t, r, admitted, capacity).Code)
}
