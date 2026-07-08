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

func TestGinMetrics_NonCanonicalErrcodeNormalizesToInternal(t *testing.T) {
	r := newEngine()
	r.GET("/api/normalize", func(c *gin.Context) {
		c.Set("errcode", "weird_code")
		c.Status(http.StatusOK)
	})

	before := rpcmetrics.CounterValue("auth-service", "/api/normalize", "internal")
	beforeWeird := rpcmetrics.CounterValue("auth-service", "/api/normalize", "weird_code")

	do(r, http.MethodGet, "/api/normalize")

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("auth-service", "/api/normalize", "internal") == before+1
	}, time.Second, 10*time.Millisecond, "non-canonical errcode status should normalize to internal")
	assert.Equal(t, beforeWeird, rpcmetrics.CounterValue("auth-service", "/api/normalize", "weird_code"),
		"raw non-canonical status label must never be recorded")
}

func TestGinMetrics_SkipsHealthzAndUnmatched(t *testing.T) {
	r := newEngine()
	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })

	// /healthz is skipped: empty-route counter must not move.
	beforeHealth := rpcmetrics.CounterValue("auth-service", "/healthz", "ok")
	do(r, http.MethodGet, "/healthz")
	assert.Equal(t, beforeHealth, rpcmetrics.CounterValue("auth-service", "/healthz", "ok"))

	// Unmatched route (empty FullPath) is skipped: assert the "" route stays absent.
	// A broken skip would still leave errCodeKey unset (no handler ran), so
	// statusForHTTP falls back to the HTTP-status class: 404 -> "bad_request",
	// not "not_found". Assert on "bad_request" so this test actually detects a
	// broken skip instead of trivially passing on an unrelated label.
	beforeEmptyBadRequest := rpcmetrics.CounterValue("auth-service", "", "bad_request")
	beforeEmptyNotFound := rpcmetrics.CounterValue("auth-service", "", "not_found")
	do(r, http.MethodGet, "/does-not-exist")
	assert.Equal(t, beforeEmptyBadRequest, rpcmetrics.CounterValue("auth-service", "", "bad_request"))
	assert.Equal(t, beforeEmptyNotFound, rpcmetrics.CounterValue("auth-service", "", "not_found"))
}
