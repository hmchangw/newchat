package ginutil

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyConfig_Validate(t *testing.T) {
	require.NoError(t, ConcurrencyConfig{MaxConcurrency: 16}.Validate())
	require.NoError(t, ConcurrencyConfig{MaxConcurrency: 0}.Validate(), "zero disables, not invalid")

	err := ConcurrencyConfig{MaxConcurrency: -1}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONCURRENCY")
}

// A request beyond the cap is shed with 429 + Retry-After rather than queued,
// which is the whole point on a bcrypt endpoint.
func TestConcurrencyConfig_Middleware_ShedsOverflow(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var shed int
	var mu sync.Mutex
	release := make(chan struct{})
	entered := make(chan struct{})

	r := gin.New()
	r.Use(ConcurrencyConfig{MaxConcurrency: 1}.Middleware(func() {
		mu.Lock()
		shed++
		mu.Unlock()
	}))
	r.POST("/login", func(c *gin.Context) {
		entered <- struct{}{}
		<-release
		c.Status(http.StatusOK)
	})

	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/login", nil))
		done <- w.Code
	}()
	<-entered // the one slot is now held

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/login", nil))
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))

	close(release)
	assert.Equal(t, http.StatusOK, <-done)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, shed, "onShed fires once per rejection")
}

func TestConcurrencyConfig_Middleware_ZeroDisables(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(ConcurrencyConfig{MaxConcurrency: 0}.Middleware(nil))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	assert.Equal(t, http.StatusOK, w.Code)
}
