package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/principal"
)

// fakeSessionValidator implements botauth.TokenValidator for middleware tests.
type fakeSessionValidator struct {
	principal principal.Principal
	err       error
}

func (f *fakeSessionValidator) Validate(_ context.Context, _ string) (principal.Principal, error) {
	return f.principal, f.err
}

// testSiteID matches the config newEmojiTestRouter builds.
const testSiteID = "s1"

func botPrincipal(account string) principal.Principal {
	return principal.Principal{UserID: "u1", Account: account, SiteID: testSiteID, Roles: []string{"bot"}}
}

func adminPrincipal() principal.Principal {
	return principal.Principal{UserID: "u1", Account: "p_admin", SiteID: testSiteID, Roles: []string{"admin"}}
}

// runAvatarPUT drives the avatar-upload chain against /api/v1/avatar/bot/:botName.
func runAvatarPUT(t *testing.T, v botauth.TokenValidator, botName, userID, token string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.PUT("/api/v1/avatar/bot/:botName", requireSession(v, testSiteID), requireBotSelfOrAdmin(),
		func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodPut, "/api/v1/avatar/bot/"+botName, nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// runEmojiPUT drives the emoji-upload chain, capturing the principal that reaches the handler.
func runEmojiPUT(t *testing.T, v botauth.TokenValidator, userID, token string) (*httptest.ResponseRecorder, *principal.Principal) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var captured *principal.Principal
	r.PUT("/api/v1/emoji/:shortcode", requireSession(v, testSiteID), requireAdmin(), func(c *gin.Context) {
		captured = sessionFromContext(c)
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPut, "/api/v1/emoji/party", nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, captured
}

func reasonOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return body.Reason
}

func TestRequireSession_Rejections(t *testing.T) {
	tests := []struct {
		name       string
		validator  botauth.TokenValidator
		userID     string
		token      string
		wantStatus int
		wantReason string
	}{
		{
			name:      "anonymous caller",
			validator: &fakeSessionValidator{principal: botPrincipal("a.bot")},
			// No headers at all — the case that was allowed through before this change.
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:       "token but no user id",
			validator:  &fakeSessionValidator{principal: botPrincipal("a.bot")},
			token:      "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "unknown token",
			validator: &fakeSessionValidator{err: errcode.Unauthenticated("nope",
				errcode.WithReason(errcode.BotplatformInvalidToken))},
			userID: "u1", token: "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:      "user id disagrees with session",
			validator: &fakeSessionValidator{principal: botPrincipal("a.bot")},
			userID:    "someone-else", token: "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:      "botplatform unreachable",
			validator: &fakeSessionValidator{err: errors.New("connection refused")},
			userID:    "u1", token: "tok",
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := runAvatarPUT(t, tt.validator, "a.bot", tt.userID, tt.token)
			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantReason, reasonOf(t, w))
		})
	}
}

func TestRequireBotSelfOrAdmin(t *testing.T) {
	tests := []struct {
		name       string
		principal  principal.Principal
		botName    string
		wantStatus int
	}{
		{name: "bot uploads its own avatar", principal: botPrincipal("a.bot"), botName: "a.bot", wantStatus: http.StatusOK},
		{name: "admin uploads any bot avatar", principal: adminPrincipal(), botName: "a.bot", wantStatus: http.StatusOK},
		{name: "bot cannot upload another bot avatar", principal: botPrincipal("a.bot"), botName: "b.bot", wantStatus: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := runAvatarPUT(t, &fakeSessionValidator{principal: tt.principal}, tt.botName, "u1", "tok")
			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantStatus == http.StatusForbidden {
				assert.Equal(t, "not_admin", reasonOf(t, w))
			}
		})
	}
}

func TestRequireAdmin_EmojiUpload(t *testing.T) {
	t.Run("admin allowed and principal reaches the handler", func(t *testing.T) {
		w, p := runEmojiPUT(t, &fakeSessionValidator{principal: adminPrincipal()}, "u1", "tok")
		require.Equal(t, http.StatusOK, w.Code)
		require.NotNil(t, p)
		assert.Equal(t, "p_admin", p.Account)
	})

	t.Run("bot refused: a shortcode is a site-wide shared name", func(t *testing.T) {
		w, p := runEmojiPUT(t, &fakeSessionValidator{principal: botPrincipal("a.bot")}, "u1", "tok")
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Equal(t, "not_admin", reasonOf(t, w))
		assert.Nil(t, p)
	})
}

// TestRegisterRoutes_AuthWiring guards the production route table itself. The
// tests above build their own engines, so deleting auth from routes.go would slip past.
func TestRegisterRoutes_AuthWiring(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r, _, emojis, _ := newEmojiTestRouter(t)
	// The public GET below reaches the store; an unregistered shortcode is enough,
	// since the assertion is only that the request was not refused by auth.
	emojis.EXPECT().EmojiDoc(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, false, nil).AnyTimes()

	// Both PUTs reject an unauthenticated caller.
	for _, path := range []string{"/api/v1/avatar/bot/a.bot", "/api/v1/emoji/party"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, path, bytes.NewReader(nil)))
		assert.Equal(t, http.StatusUnauthorized, w.Code, path)
		assert.Equal(t, "invalid_token", reasonOf(t, w), path)
	}

	// The reads stay public: no credential, and nothing resembling an auth refusal.
	for _, path := range []string{"/healthz", "/api/v1/emoji/party"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		assert.NotEqual(t, http.StatusUnauthorized, w.Code, path)
		assert.NotEqual(t, http.StatusForbidden, w.Code, path)
	}
}

// TestRequireSession_ForeignSite pins locality: botplatform's login does not refuse a
// remote user, so a site-B session must not authorize a write on site A.
func TestRequireSession_ForeignSite(t *testing.T) {
	foreign := principal.Principal{UserID: "u1", Account: "p_admin", SiteID: "site-b", Roles: []string{"admin"}}

	w := runAvatarPUT(t, &fakeSessionValidator{principal: foreign}, "p_admin", "u1", "tok")

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "not_admin", reasonOf(t, w))
}

// TestRequireSession_LogsCarryRequestID pins the one field that ties an auth
// failure to the access log. Classify reads its logger from the ctx, so dropping
// the WithLogValues wrap silently strips request_id from these lines alone.
func TestRequireSession_LogsCarryRequestID(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.PUT("/api/v1/emoji/:shortcode",
		requireSession(&fakeSessionValidator{err: errcode.Unauthenticated("nope")}, testSiteID),
		func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodPut, "/api/v1/emoji/party", nil)
	req.Header.Set(botauth.HeaderUserID, "u1")
	req.Header.Set(botauth.HeaderAuthToken, "tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, buf.String(), `"msg":"request failed"`)
	assert.Regexp(t, `"request_id":"[0-9a-f-]{36}"`, buf.String())
}
