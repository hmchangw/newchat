package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/principal"
)

type fakeValidator struct {
	claims pkgoidc.Claims
	err    error
}

func (f *fakeValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	return f.claims, f.err
}

// fakeBotValidator implements botauth.TokenValidator for middleware tests.
type fakeBotValidator struct {
	principal principal.Principal
	err       error
}

func (f *fakeBotValidator) Validate(_ context.Context, _ string) (principal.Principal, error) {
	return f.principal, f.err
}

// runAuthReq drives authMiddleware over a caller-built request, returning the
// recorder and whatever AuthenticatedUser reached the handler.
func runAuthReq(t *testing.T, d authDeps, req *http.Request) (*httptest.ResponseRecorder, *AuthenticatedUser) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(d))
	var captured *AuthenticatedUser
	r.GET("/x", func(c *gin.Context) {
		if u, ok := userFromContext(c); ok {
			captured = u
		}
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, captured
}

// runAuth preserves the original SSO-only call shape used by the existing tests.
func runAuth(t *testing.T, v TokenValidator, devMode bool, token string) (*httptest.ResponseRecorder, *AuthenticatedUser) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if token != "" {
		req.Header.Set("ssoToken", token)
	}
	return runAuthReq(t, authDeps{sso: v, devMode: devMode}, req)
}

// The SSO reasons are what distinguish these 401s from the session branch's
// uniform invalid_token. Asserting only the status would let a regression in the
// credential routing send every SSO rejection down the session path unnoticed —
// 401 equals 401.
func TestAuthMiddleware_SSORejectionReasons(t *testing.T) {
	tests := []struct {
		name   string
		v      TokenValidator
		token  string
		reason string
	}{
		{"no credential at all", &fakeValidator{}, "", "missing_fields"},
		{"unverifiable token", &fakeValidator{err: errors.New("bad")}, "tok", "invalid_sso_token"},
		{"expired token", &fakeValidator{err: pkgoidc.ErrTokenExpired}, "tok", "sso_token_expired"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, u := runAuth(t, tc.v, false, tc.token)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Nil(t, u)
			var body struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tc.reason, body.Reason)
		})
	}
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
	r.Use(authMiddleware(authDeps{sso: v}))
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

// botReq builds a request carrying the bot credential headers.
func botReq(userID, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	return req
}

func okPrincipal() principal.Principal {
	return principal.Principal{
		UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
	}
}

func TestAuthMiddleware_SessionToken_PopulatesUser(t *testing.T) {
	d := authDeps{bot: &fakeBotValidator{principal: okPrincipal()}}

	w, u := runAuthReq(t, d, botReq("u1", "tok"))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alerts.sa.bot", u.Account)
	assert.Equal(t, "site-a", u.SiteID)
	require.NotNil(t, u.Session, "session callers must carry their principal")
	assert.Equal(t, []string{"bot"}, u.Session.Roles)
	// No directory metadata on a session principal, so DisplayName falls back.
	assert.Equal(t, "alerts.sa.bot", u.DisplayName())
}

func TestAuthMiddleware_SessionToken_Failures(t *testing.T) {
	tests := []struct {
		name       string
		userID     string
		deps       authDeps
		wantStatus int
		wantReason string
	}{
		{
			name: "unknown token", userID: "u1",
			deps: authDeps{bot: &fakeBotValidator{err: errcode.Unauthenticated("nope",
				errcode.WithReason(errcode.BotplatformInvalidToken))}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "user id disagrees with session", userID: "someone-else",
			deps:       authDeps{bot: &fakeBotValidator{principal: okPrincipal()}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "missing user id header", userID: "",
			deps:       authDeps{bot: &fakeBotValidator{principal: okPrincipal()}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "botplatform unreachable", userID: "u1",
			deps:       authDeps{bot: &fakeBotValidator{err: errors.New("connection refused")}},
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
		{
			name: "session auth not configured", userID: "u1",
			deps:       authDeps{},
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, u := runAuthReq(t, tt.deps, botReq(tt.userID, "tok"))

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Nil(t, u, "a rejected request must not reach the handler")
			var body struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.wantReason, body.Reason)
		})
	}
}

func TestAuthMiddleware_BothHeaders_400Ambiguous(t *testing.T) {
	d := authDeps{sso: &fakeValidator{}, bot: &fakeBotValidator{principal: okPrincipal()}}
	req := botReq("u1", "tok")
	req.Header.Set("ssoToken", "sso-tok")

	w, u := runAuthReq(t, d, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, u)
	var body struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "ambiguous_token", body.Reason)
}

func TestAuthMiddleware_SessionTokenBeatsSSOCookie(t *testing.T) {
	// The cookie is ambient state a browser attaches automatically; the header is
	// an explicit act, so it wins rather than being ambiguous.
	d := authDeps{sso: &fakeValidator{}, bot: &fakeBotValidator{principal: okPrincipal()}}
	req := botReq("u1", "tok")
	req.Header.Set("Cookie", "ssoToken=c-tok")

	w, u := runAuthReq(t, d, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alerts.sa.bot", u.Account)
	assert.NotNil(t, u.Session)
}

func TestAuthMiddleware_SessionToken_Email(t *testing.T) {
	tests := []struct {
		name   string
		domain string
		want   string
	}{
		{name: "unset domain sends empty email", domain: "", want: ""},
		{name: "configured domain synthesizes an address", domain: "bots.example.com", want: "alerts.sa.bot@bots.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := authDeps{bot: &fakeBotValidator{principal: okPrincipal()}, botEmailDomain: tt.domain}

			w, u := runAuthReq(t, d, botReq("u1", "tok"))

			require.Equal(t, http.StatusOK, w.Code)
			require.NotNil(t, u)
			assert.Equal(t, tt.want, u.Email)
		})
	}
}

// TestAuthMiddleware_PartialSessionCredential pins the contract documented in
// client-api.md §2.4: a session credential that is missing a half is still a
// session attempt, so it gets the uniform 401 rather than the SSO branch's
// "missing ssoToken" / missing_fields.
func TestAuthMiddleware_PartialSessionCredential(t *testing.T) {
	tests := []struct {
		name   string
		userID string
		token  string
	}{
		{name: "user id without token", userID: "u1"},
		{name: "token without user id", token: "tok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := authDeps{sso: &fakeValidator{}, bot: &fakeBotValidator{principal: okPrincipal()}}

			w, u := runAuthReq(t, d, botReq(tt.userID, tt.token))

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Nil(t, u)
			var body struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, "invalid_token", body.Reason, "must not leak that one half was present")
		})
	}
}

// TestAuthMiddleware_SSOUnaffectedByStraySessionHeader proves the partial-credential
// routing does not hijack a caller who supplied a real ssoToken.
func TestAuthMiddleware_SSOUnaffectedByStraySessionHeader(t *testing.T) {
	v := &fakeValidator{claims: pkgoidc.Claims{PreferredUsername: "alice", Email: "alice@x.com"}}
	d := authDeps{sso: v, bot: &fakeBotValidator{principal: okPrincipal()}}
	req := botReq("u1", "") // stray x-user-id, no x-auth-token
	req.Header.Set("ssoToken", "sso-tok")

	w, u := runAuthReq(t, d, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alice", u.Account, "the SSO credential still wins")
	assert.Nil(t, u.Session)
}
