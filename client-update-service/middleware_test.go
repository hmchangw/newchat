package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestRequestIDMiddleware_MintsWhenAbsent(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/healthz", nil)

	var seen string
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/healthz", func(c *gin.Context) {
		seen = c.GetString("request_id")
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(w, c.Request)

	assert.True(t, idgen.IsValidUUID(seen), "middleware must mint a valid UUID request id")
	assert.Equal(t, seen, w.Header().Get(natsutil.RequestIDHeader))
}

func TestRequestIDMiddleware_PreservesValidInbound(t *testing.T) {
	inbound := idgen.GenerateRequestID()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set(natsutil.RequestIDHeader, inbound)

	var seen string
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/healthz", func(c *gin.Context) {
		seen = c.GetString("request_id")
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(w, req)

	assert.Equal(t, inbound, seen, "a valid inbound request id must be preserved")
}

func TestAccessLogMiddleware_PassesThrough(t *testing.T) {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	r := gin.New()
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusTeapot) })
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusTeapot, w.Code, "access log must not alter the response")
}
