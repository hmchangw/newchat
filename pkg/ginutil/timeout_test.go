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
