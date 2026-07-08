package ginutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

func newEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Metrics("auth-service"))
	return r
}

func do(r *gin.Engine, method, target string) {
	req := httptest.NewRequest(method, target, nil)
	r.ServeHTTP(httptest.NewRecorder(), req)
}

func TestGinMetrics_ErrcodeStatus(t *testing.T) {
	r := newEngine()
	r.GET("/api/rooms/:id", func(c *gin.Context) {
		errhttp.Write(context.Background(), c, errcode.NotFound("nope"))
	})
	before := rpcmetrics.CounterValue("auth-service", "/api/rooms/:id", "not_found")

	do(r, http.MethodGet, "/api/rooms/42")

	after := rpcmetrics.CounterValue("auth-service", "/api/rooms/:id", "not_found")
	assert.Equal(t, before+1, after)
}

func TestGinMetrics_HTTPClassFallback(t *testing.T) {
	r := newEngine()
	r.GET("/api/ping", func(c *gin.Context) { c.Status(http.StatusOK) })
	before := rpcmetrics.CounterValue("auth-service", "/api/ping", "ok")

	do(r, http.MethodGet, "/api/ping")

	after := rpcmetrics.CounterValue("auth-service", "/api/ping", "ok")
	assert.Equal(t, before+1, after)
}

func TestGinMetrics_SkipsHealthzAndUnmatched(t *testing.T) {
	r := newEngine()
	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })

	// /healthz is skipped: empty-route counter must not move.
	beforeHealth := rpcmetrics.CounterValue("auth-service", "/healthz", "ok")
	do(r, http.MethodGet, "/healthz")
	assert.Equal(t, beforeHealth, rpcmetrics.CounterValue("auth-service", "/healthz", "ok"))

	// Unmatched route (empty FullPath) is skipped: assert the "" route stays absent.
	beforeEmpty := rpcmetrics.CounterValue("auth-service", "", "not_found")
	do(r, http.MethodGet, "/does-not-exist")
	assert.Equal(t, beforeEmpty, rpcmetrics.CounterValue("auth-service", "", "not_found"))
}
