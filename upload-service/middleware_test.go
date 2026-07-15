package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

type fakeValidator struct {
	claims pkgoidc.Claims
	err    error
}

func (f *fakeValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	return f.claims, f.err
}

func runAuth(t *testing.T, v TokenValidator, devMode bool, token string) (*httptest.ResponseRecorder, *AuthenticatedUser) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(v, devMode))
	var captured *AuthenticatedUser
	r.GET("/x", func(c *gin.Context) {
		if u, ok := userFromContext(c); ok {
			captured = u
		}
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if token != "" {
		req.Header.Set("ssoToken", token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, captured
}

func TestAuthMiddleware_MissingToken_401(t *testing.T) {
	w, _ := runAuth(t, &fakeValidator{}, false, "")
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_InvalidToken_401(t *testing.T) {
	w, _ := runAuth(t, &fakeValidator{err: errors.New("bad")}, false, "tok")
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_ExpiredToken_401(t *testing.T) {
	w, _ := runAuth(t, &fakeValidator{err: pkgoidc.ErrTokenExpired}, false, "tok")
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_ValidToken_PopulatesUser(t *testing.T) {
	v := &fakeValidator{claims: pkgoidc.Claims{
		PreferredUsername: "alice",
		Email:             "alice@x.com",
		Description:       "E123, Alice, 陳大文",
	}}
	w, u := runAuth(t, v, false, "tok")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alice", u.Account)
	assert.Equal(t, "alice@x.com", u.Email)
	assert.Equal(t, "Alice", u.EngName)
	assert.Equal(t, "陳大文", u.ChineseName)
	assert.Equal(t, "Alice 陳大文", u.DisplayName())
}

func TestAuthMiddleware_DevMode_SynthesizesUser(t *testing.T) {
	w, u := runAuth(t, nil, true, "alice")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alice", u.Account)
	assert.Equal(t, "alice@dev.local", u.Email)
}

func TestRequestIDMiddleware_MintsAndEchoes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/x", func(c *gin.Context) {
		assert.NotEmpty(t, c.GetString("request_id"))
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
}

func TestTokenFromRequest(t *testing.T) {
	tests := []struct {
		name, header, cookie, want string
	}{
		{"header only", "h-tok", "", "h-tok"},
		{"cookie only", "", "c-tok", "c-tok"},
		{"header wins over cookie", "h-tok", "c-tok", "h-tok"},
		{"neither", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "/x", nil)
			if tc.header != "" {
				c.Request.Header.Set("ssoToken", tc.header)
			}
			if tc.cookie != "" {
				c.Request.Header.Set("Cookie", "ssoToken="+tc.cookie)
			}
			assert.Equal(t, tc.want, tokenFromRequest(c))
		})
	}
}

func TestAuthMiddleware_CookieFallback_PopulatesUser(t *testing.T) {
	v := &fakeValidator{claims: pkgoidc.Claims{PreferredUsername: "bob", Email: "bob@x.com"}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(v, false))
	var captured *AuthenticatedUser
	r.GET("/x", func(c *gin.Context) {
		if u, ok := userFromContext(c); ok {
			captured = u
		}
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Cookie", "ssoToken=tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, captured)
	assert.Equal(t, "bob", captured.Account)
	assert.Equal(t, "bob@x.com", captured.Email)
}

func runCORS(t *testing.T, allowed []string, method, origin string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(corsMiddleware(allowed))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(method, "/x", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestCORSMiddleware_AllowedOrigin_EmitsCredentialedHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodGet, "https://app.example.com")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
	assert.Equal(t, "Origin", w.Header().Get("Vary"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "ssoToken")
}

func TestCORSMiddleware_NotAllowedOrigin_NoHeaders(t *testing.T) {
	tests := []struct{ name, origin string }{
		{"no origin", ""},
		{"disallowed origin", "https://evil.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runCORS(t, []string{"https://app.example.com"}, http.MethodGet, tc.origin)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
			// Vary: Origin must still be set so a shared cache keys on Origin and never
			// serves this no-Allow-Origin response to a later allowed-origin request.
			assert.Equal(t, "Origin", w.Header().Get("Vary"))
		})
	}
}

func TestCORSMiddleware_Disabled_NoVaryNoHeaders(t *testing.T) {
	w := runCORS(t, nil, http.MethodGet, "https://app.example.com")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("Vary"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddleware_Preflight_AllowedOrigin_204(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, "https://app.example.com")
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORSMiddleware_Preflight_NotAllowedOrigin_204_NoHeaders(t *testing.T) {
	tests := []struct{ name, origin string }{
		{"no origin", ""},
		{"disallowed origin", "https://evil.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, tc.origin)
			assert.Equal(t, http.StatusNoContent, w.Code)
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
		})
	}
}

func TestAccessLogMiddleware_PassThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	called := false
	r.GET("/x", func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	assert.True(t, called)
	assert.Equal(t, http.StatusOK, w.Code)
}
