package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
)

func TestHandleSetCookie_SessionCaller_400(t *testing.T) {
	// setCookie exists only for browser <img> downloads. A bot sends headers every
	// request, so there is no ssoToken to mirror.
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/file/setCookie", nil)
	p := principal.Principal{UserID: "u1", Account: "alerts.sa.bot", Roles: []string{"bot"}}
	c.Set(ctxUserKey, &AuthenticatedUser{User: model.User{Account: p.Account}, Session: &p})

	NewHandler(nil, nil, &fakeS3{}, testMaxImages, testMaxAttachments, testMaxImageSize,
		0, nil, nil, testCacheMaxAge, false, &fakeDrive{}).HandleSetCookie(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, w.Header().Get("Set-Cookie"), "no cookie may be issued to a session caller")
}

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
			h := NewHandler(nil, nil, &fakeS3{}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, tt.partitioned, &fakeDrive{})

			gin.SetMode(gin.TestMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/file/set-cookie", nil)
			req.Header.Set(ssoTokenName, "token-123")
			c.Request = req
			// In production this endpoint only runs behind authMiddleware, so a
			// context with no user is unreachable; supply the SSO caller it models.
			c.Set(ctxUserKey, okUser())

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
