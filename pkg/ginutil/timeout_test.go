package ginutil

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeout_SetsDeadlineOnRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(50 * time.Millisecond))

	var hadDeadline bool
	r.GET("/x", func(c *gin.Context) {
		_, hadDeadline = c.Request.Context().Deadline()
		c.Status(200)
	})

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	require.True(t, hadDeadline, "handler context must carry a deadline")
}

func TestTimeout_DoneFiresAfterExpiry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(20 * time.Millisecond))

	var cancelled bool
	r.GET("/x", func(c *gin.Context) {
		select {
		case <-c.Request.Context().Done():
			cancelled = true
		case <-time.After(2 * time.Second):
		}
		c.Status(200)
	})

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	assert.True(t, cancelled, "context must be cancelled after the timeout elapses")
}

// A handler that returns past its deadline without having flushed a response
// must not fall through to Gin's implicit 200 — that reports success over work
// the deadline cancelled mid-flight.
func TestTimeout_WritesUnavailableWhenDeadlineWinsAndNothingFlushed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(20 * time.Millisecond))
	r.GET("/x", func(c *gin.Context) {
		<-c.Request.Context().Done()
		c.Status(200)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/x", nil))
	assert.Equal(t, 503, w.Code, "an expired request must not report success")
	assert.Contains(t, w.Body.String(), "unavailable")
}

func TestTimeout_LeavesAFlushedResponseAlone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(20 * time.Millisecond))
	r.GET("/x", func(c *gin.Context) {
		<-c.Request.Context().Done()
		c.JSON(201, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/x", nil))
	assert.Equal(t, 201, w.Code, "a handler that already committed a response keeps it")
}

func TestTimeout_DoesNotTouchRequestsThatBeatTheDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(2 * time.Second))
	r.GET("/x", func(c *gin.Context) { c.Status(204) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/x", nil))
	assert.Equal(t, 204, w.Code)
}
