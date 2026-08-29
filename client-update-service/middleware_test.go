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

func TestLookupAccount(t *testing.T) {
	tokens := map[string]string{
		"admin-service": "0123456789abcdef",
		"ops-cli":       "fedcba9876543210",
	}
	tests := []struct {
		name        string
		token       string
		wantAccount string
		wantOK      bool
	}{
		{"first account", "0123456789abcdef", "admin-service", true},
		{"second account", "fedcba9876543210", "ops-cli", true},
		{"unknown token", "not-a-real-token", "", false},
		{"empty token", "", "", false},
		{"proper prefix of a valid token", "0123456789abcde", "", false},
		{"valid token plus a suffix", "0123456789abcdefX", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, ok := lookupAccount(tokens, tt.token)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantAccount, account)
		})
	}
}

func TestRequireServiceAccount(t *testing.T) {
	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantAcct   string
	}{
		{"valid bearer", "Bearer 0123456789abcdef", http.StatusOK, "admin-service"},
		{"valid bearer with padding", "Bearer   0123456789abcdef  ", http.StatusOK, "admin-service"},
		{"no header", "", http.StatusUnauthorized, ""},
		{"unknown token", "Bearer nope-nope-nope-nope", http.StatusUnauthorized, ""},
		{"empty token after prefix", "Bearer ", http.StatusUnauthorized, ""},
		{"basic scheme", "Basic 0123456789abcdef", http.StatusUnauthorized, ""},
		{"lowercase bearer scheme", "bearer 0123456789abcdef", http.StatusUnauthorized, ""},
		{"bare token, no scheme", "0123456789abcdef", http.StatusUnauthorized, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seenAccount string
			var handlerRan bool
			r := gin.New()
			r.Use(requireServiceAccount(tokens))
			r.POST("/api/v1/version", func(c *gin.Context) {
				handlerRan = true
				seenAccount = c.GetString(ctxServiceAccount)
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantAcct, seenAccount)
			assert.Equal(t, tt.wantStatus == http.StatusOK, handlerRan,
				"the handler must run only when the credential is accepted")
		})
	}
}

// Every rejection must be byte-identical, so a caller cannot tell an unknown
// token from a malformed header and probe the token table.
func TestRequireServiceAccount_RejectionsAreIndistinguishable(t *testing.T) {
	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	bodies := make([]string, 0, 3)
	for _, hdr := range []string{"", "Basic x", "Bearer wrong-token-value-here"} {
		r := gin.New()
		r.Use(requireServiceAccount(tokens))
		r.POST("/api/v1/version", func(c *gin.Context) { c.Status(http.StatusOK) })

		req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		bodies = append(bodies, w.Body.String())
	}
	assert.Equal(t, bodies[0], bodies[1])
	assert.Equal(t, bodies[1], bodies[2])
}

func TestRequireServiceAccount_NeverEchoesTheToken(t *testing.T) {
	// #nosec G101 -- fake fixture, not a live credential; the test asserts it never reaches the response. nosemgrep: gosec.G101-1
	const secret = "0123456789abcdef"
	r := gin.New()
	r.Use(requireServiceAccount(map[string]string{"admin-service": secret}))
	r.POST("/api/v1/version", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotContains(t, w.Body.String(), secret)
	assert.NotContains(t, w.Body.String(), "wrong", "the rejection must not echo the presented token either")
}
