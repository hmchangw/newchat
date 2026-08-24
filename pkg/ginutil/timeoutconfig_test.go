package ginutil

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeoutConfig_Validate(t *testing.T) {
	require.NoError(t, TimeoutConfig{RequestTimeout: 10 * time.Second}.Validate())
	require.NoError(t, TimeoutConfig{RequestTimeout: 0}.Validate(), "zero disables, not invalid")

	err := TimeoutConfig{RequestTimeout: -1}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REQUEST_TIMEOUT")
}

func TestTimeoutConfig_Middleware_SetsDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(TimeoutConfig{RequestTimeout: 50 * time.Millisecond}.Middleware())

	var hadDeadline bool
	r.GET("/x", func(c *gin.Context) {
		_, hadDeadline = c.Request.Context().Deadline()
		c.Status(200)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	require.True(t, hadDeadline)
}
