package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// The cross-site permission fanout budgets itself to min(FanoutTimeout, request
// deadline) (permissions.go:publishPermissionFanout). So admin-service's base
// middleware must NOT impose a per-request context deadline: a router timeout
// shorter than FanoutTimeout (e.g. the fleet's shared 10s REQUEST_TIMEOUT) would
// silently shrink the fanout and abort a multi-site permission change early.
// This guards against re-introducing such a timeout into applyBaseMiddleware.
func TestApplyBaseMiddleware_ImposesNoRequestDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	applyBaseMiddleware(r, nil) // nil observability chain — irrelevant to the deadline

	var sawDeadline bool
	r.GET("/probe", func(c *gin.Context) {
		_, sawDeadline = c.Request.Context().Deadline()
		c.Status(200)
	})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/probe", nil))

	assert.Equal(t, 200, rec.Code)
	assert.False(t, sawDeadline,
		"admin base middleware must not deadline the request: the permission fanout relies on the full FanoutTimeout being available")
}
