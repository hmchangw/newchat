package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// TestHandleSetCookie_Partitioned drives HandleSetCookie with setCookiePartitioned
// true and false, asserting the Set-Cookie header includes/omits Partitioned while
// Secure, HttpOnly, and SameSite=None hold in both cases.
func TestHandleSetCookie_Partitioned(t *testing.T) {
	tests := []struct {
		name        string
		partitioned bool
	}{
		{"partitioned enabled", true},
		{"partitioned disabled", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(nil, nil, &fakeS3{}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, tt.partitioned)

			gin.SetMode(gin.TestMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/file/set-cookie", nil)
			req.Header.Set(ssoTokenName, "token-123")
			c.Request = req

			h.HandleSetCookie(c)

			assert.Equal(t, http.StatusOK, w.Code)
			cookie := w.Header().Get("Set-Cookie")
			assert.Contains(t, cookie, "Secure")
			assert.Contains(t, cookie, "HttpOnly")
			assert.Contains(t, cookie, "SameSite=None")
			if tt.partitioned {
				assert.Contains(t, cookie, "Partitioned")
			} else {
				assert.NotContains(t, cookie, "Partitioned")
			}
		})
	}
}
