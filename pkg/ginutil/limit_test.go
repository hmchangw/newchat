package ginutil

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// blockingEngine serves /x from a handler that announces its admission on the
// returned channel and then parks until release is closed.
func blockingEngine(t *testing.T, n int, onShed func()) (r *gin.Engine, admitted <-chan struct{}, release chan struct{}) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rel := make(chan struct{})
	adm := make(chan struct{}, n)
	e := gin.New()
	e.Use(MaxConcurrency(n, onShed))
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

// Overflow is shed at the door rather than queued behind work whose client
// has already given up.
func TestMaxConcurrency_ShedsBeyondCap(t *testing.T) {
	r, admitted, release := blockingEngine(t, 1, nil)
	defer close(release)

	w := fillAndShed(t, r, admitted, 1)

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
	assert.JSONEq(t,
		`{"code":"too_many_requests","reason":"overloaded","error":"server is at capacity, retry shortly"}`,
		w.Body.String())
}

// A slot returns on the normal path, or the cap ratchets down to zero.
func TestMaxConcurrency_ReleasesSlotAfterCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(MaxConcurrency(1, nil))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		assert.Equal(t, http.StatusOK, w.Code, "each sequential request must reclaim the slot")
	}
}

// Release is deferred, so a panicking handler cannot leak its slot.
func TestMaxConcurrency_ReleasesSlotOnPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), MaxConcurrency(1, nil))
	r.GET("/boom", func(c *gin.Context) { panic("boom") })
	r.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ok", nil))
	assert.Equal(t, http.StatusOK, w.Code, "a panicking handler must not leak its slot")
}

// 0 disables the limiter, the documented escape hatch.
func TestMaxConcurrency_NonPositiveDisablesLimiting(t *testing.T) {
	for _, n := range []int{0, -1} {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.Use(MaxConcurrency(n, nil))
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

// Shedding is invisible without the counter; it must fire per rejection.
func TestMaxConcurrency_OnShedObserverFires(t *testing.T) {
	var shed atomic.Int64
	r, admitted, release := blockingEngine(t, 1, func() { shed.Add(1) })
	defer close(release)

	require.Equal(t, http.StatusTooManyRequests, fillAndShed(t, r, admitted, 1).Code)
	assert.Equal(t, int64(1), shed.Load())
}

// The cap is a floor on admitted work, not just a ceiling on rejections.
func TestMaxConcurrency_AdmitsUpToCapConcurrently(t *testing.T) {
	const capacity = 4
	r, admitted, release := blockingEngine(t, capacity, nil)
	defer close(release)

	// fillAndShed only returns once all `capacity` requests were admitted at once.
	assert.Equal(t, http.StatusTooManyRequests, fillAndShed(t, r, admitted, capacity).Code)
}

// The limiter adds no log line of its own. AccessLog still records the request —
// that is uniform across every response — but a burst must not also pay for
// Classify on each rejection.
func TestMaxConcurrency_ShedAddsNoLogLine(t *testing.T) {
	var logged bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r, admitted, release := blockingEngine(t, 1, nil)
	defer close(release)

	require.Equal(t, http.StatusTooManyRequests, fillAndShed(t, r, admitted, 1).Code)
	assert.Empty(t, logged.String(), "the limiter must not log on its own")
}

// The shed path writes the envelope itself rather than going through
// errhttp.Write (which would log per rejection), so pin that the two produce
// the same body — a client must not be able to tell them apart.
func TestMaxConcurrency_ShedEnvelopeMatchesErrhttp(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ref := gin.New()
	ref.GET("/x", func(c *gin.Context) {
		errhttp.Write(c.Request.Context(), c, errcode.TooManyRequests(
			"server is at capacity, retry shortly", errcode.WithReason(errcode.Overloaded)))
	})
	w := httptest.NewRecorder()
	ref.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	r, admitted, release := blockingEngine(t, 1, nil)
	defer close(release)
	shed := fillAndShed(t, r, admitted, 1)

	assert.Equal(t, w.Code, shed.Code)
	assert.JSONEq(t, w.Body.String(), shed.Body.String())
}
