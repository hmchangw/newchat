package ginutil

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestRequestID_AttachesIDToRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestID())

	var fromCtx string
	var fromGin string
	r.GET("/test", func(c *gin.Context) {
		fromCtx = natsutil.RequestIDFromContext(c.Request.Context())
		fromGin = c.GetString("request_id")
		c.Status(http.StatusOK)
	})

	testID := "01893f8b-1c4a-7000-abcd-ef0123456789"
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set(natsutil.RequestIDHeader, testID)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, testID, fromGin, "Gin context still carries the ID under request_id")
	assert.Equal(t, testID, fromCtx, "request.Context() must also carry the ID via natsutil")
	assert.Equal(t, testID, w.Header().Get(natsutil.RequestIDHeader), "echoed in response header")
}

func TestRequestID_GeneratesAndAttachesWhenHeaderAbsent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestID())

	var fromCtx string
	var fromGin string
	r.GET("/test", func(c *gin.Context) {
		fromCtx = natsutil.RequestIDFromContext(c.Request.Context())
		fromGin = c.GetString("request_id")
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotEmpty(t, fromCtx, "minted request ID must be attached to request.Context()")
	assert.Equal(t, fromCtx, fromGin, "minted ID must be identical in ctx and Gin keys map")
	assert.Equal(t, fromCtx, w.Header().Get(natsutil.RequestIDHeader),
		"the same minted ID must be echoed in the response header")
}

func TestRequestID_RegeneratesOnMalformedHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestID())

	var fromCtx string
	r.GET("/test", func(c *gin.Context) {
		fromCtx = natsutil.RequestIDFromContext(c.Request.Context())
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set(natsutil.RequestIDHeader, "not-a-uuidv7")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotEqual(t, "not-a-uuidv7", fromCtx, "malformed inbound ID must be replaced with a freshly minted one")
	assert.True(t, idgen.IsValidUUID(fromCtx), "the regenerated ID must itself be a valid hyphenated UUID")
	assert.Equal(t, fromCtx, w.Header().Get(natsutil.RequestIDHeader),
		"echoed response header must match the regenerated ID, not the malformed input")
}

func TestAccessLog_LogsAndPassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// RequestID first, so the access log gets a non-empty request_id field.
	r.Use(RequestID())
	r.Use(AccessLog())

	handlerCalled := false
	r.GET("/test", func(c *gin.Context) {
		handlerCalled = true
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.True(t, handlerCalled, "downstream handler must run after AccessLog")
	assert.Equal(t, http.StatusOK, w.Code, "status passes through unchanged")
}

func TestCORS_AnyOrigin_GetsWildcardHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CORS())
	r.POST("/auth", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", nil)
	req.Header.Set("Origin", "http://anything.example.com")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Methods"), "POST")
}

func TestCORS_PreflightOptions_Returns204WithoutHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CORS())
	r.POST("/auth", func(c *gin.Context) {
		t.Fatal("downstream handler must NOT run on preflight")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/auth", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "POST")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}
