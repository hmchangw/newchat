package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func newMiddlewareRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(corsMiddleware())
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })
	return r
}

func TestRequestIDMiddleware_MintsAndEchoes(t *testing.T) {
	r := newMiddlewareRouter()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get(natsutil.RequestIDHeader), "should mint+echo a request id")
}

func TestRequestIDMiddleware_PassesThroughInbound(t *testing.T) {
	r := newMiddlewareRouter()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	const id = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	req.Header.Set(natsutil.RequestIDHeader, id)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, id, w.Header().Get(natsutil.RequestIDHeader))
}

func TestRequestIDMiddleware_InvalidInboundIsReplaced(t *testing.T) {
	r := newMiddlewareRouter()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set(natsutil.RequestIDHeader, "not-a-valid-uuid")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	got := w.Header().Get(natsutil.RequestIDHeader)
	assert.NotEqual(t, "not-a-valid-uuid", got, "an invalid inbound id must be replaced, not echoed")
	assert.NotEmpty(t, got)
}

func TestCorsMiddleware_OptionsShortCircuits(t *testing.T) {
	r := newMiddlewareRouter()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodOptions, "/ping", nil))
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCorsMiddleware_SetsHeadersOnGet(t *testing.T) {
	r := newMiddlewareRouter()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping", nil))
	assert.Equal(t, "pong", w.Body.String())
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}
