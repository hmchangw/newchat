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

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/principal"
)

// fakeSSO resolves a fixed set of tokens; anything else is invalid.
type fakeSSO struct{ claims map[string]pkgoidc.Claims }

func (f fakeSSO) Validate(_ context.Context, token string) (pkgoidc.Claims, error) {
	if token == "tok-expired" {
		return pkgoidc.Claims{}, pkgoidc.ErrTokenExpired
	}
	c, ok := f.claims[token]
	if !ok {
		return pkgoidc.Claims{}, errors.New("signature mismatch")
	}
	return c, nil
}

type fakeBot struct{ account string }

// Validate accepts one canned session token and rejects the rest.
func (f fakeBot) Validate(_ context.Context, token string) (principal.Principal, error) {
	if token != "tok-session" {
		// Typed, as real botplatform replies: a raw error would mean "could not
		// check", which botauth reports as 503 rather than 401.
		return principal.Principal{}, errcode.Unauthenticated("invalid token",
			errcode.WithReason(errcode.BotplatformInvalidToken))
	}
	return principal.Principal{Account: f.account, UserID: "u1", SiteID: "site-a"}, nil
}

// authEngine builds a router with only the auth middleware in front of h.
func authEngine(t *testing.T, d authDeps) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(d))
	r.GET("/x", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"account": accountFromContext(c)})
	})
	return r
}

// fullDeps wires both credential validators, the production arrangement.
func fullDeps() authDeps {
	return authDeps{
		sso: fakeSSO{claims: map[string]pkgoidc.Claims{
			"tok-good":      {PreferredUsername: "alice"},
			"tok-noaccount": {Name: "Display Only"},
		}},
		bot: fakeBot{account: "helper.bot"},
	}
}

// The account must come from the verified credential and never the request,
// which is the guarantee NATS subject scoping gave for free.
func TestAuthMiddleware(t *testing.T) {
	tests := []struct {
		name        string
		deps        authDeps
		headers     map[string]string
		wantStatus  int
		wantAccount string
		wantReason  errcode.Reason
	}{
		{
			name:        "valid sso token",
			deps:        fullDeps(),
			headers:     map[string]string{ssoTokenHeader: "tok-good"},
			wantStatus:  http.StatusOK,
			wantAccount: "alice",
		},
		{
			name:       "expired sso token",
			deps:       fullDeps(),
			headers:    map[string]string{ssoTokenHeader: "tok-expired"},
			wantStatus: http.StatusUnauthorized,
			wantReason: errcode.AuthTokenExpired,
		},
		{
			name:       "invalid sso token",
			deps:       fullDeps(),
			headers:    map[string]string{ssoTokenHeader: "tok-bad"},
			wantStatus: http.StatusUnauthorized,
			wantReason: errcode.AuthInvalidToken,
		},
		{
			name:       "sso token without preferred_username is rejected",
			deps:       fullDeps(),
			headers:    map[string]string{ssoTokenHeader: "tok-noaccount"},
			wantStatus: http.StatusUnauthorized,
			wantReason: errcode.AuthInvalidToken,
		},
		{
			name:        "valid session token",
			deps:        fullDeps(),
			headers:     map[string]string{botauth.HeaderUserID: "u1", botauth.HeaderAuthToken: "tok-session"},
			wantStatus:  http.StatusOK,
			wantAccount: "helper.bot",
		},
		{
			name:       "unknown session token",
			deps:       fullDeps(),
			headers:    map[string]string{botauth.HeaderUserID: "u1", botauth.HeaderAuthToken: "tok-nope"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "both credentials is ambiguous",
			deps:       fullDeps(),
			headers:    map[string]string{ssoTokenHeader: "tok-good", botauth.HeaderAuthToken: "tok-session"},
			wantStatus: http.StatusBadRequest,
			wantReason: errcode.BotplatformAmbiguousToken,
		},
		{
			name:       "no credential",
			deps:       fullDeps(),
			headers:    nil,
			wantStatus: http.StatusUnauthorized,
			wantReason: errcode.AuthMissingFields,
		},
		{
			name:       "user id without token is still a session attempt",
			deps:       fullDeps(),
			headers:    map[string]string{botauth.HeaderUserID: "u1"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "session branch unconfigured",
			deps:       authDeps{sso: fullDeps().sso},
			headers:    map[string]string{botauth.HeaderUserID: "u1", botauth.HeaderAuthToken: "tok-session"},
			wantStatus: http.StatusServiceUnavailable,
			wantReason: errcode.BotplatformUpstreamUnavailable,
		},
		{
			name:       "sso branch unconfigured",
			deps:       authDeps{bot: fullDeps().bot},
			headers:    map[string]string{ssoTokenHeader: "tok-good"},
			wantStatus: http.StatusServiceUnavailable,
			wantReason: errcode.BotplatformUpstreamUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			authEngine(t, tc.deps).ServeHTTP(w, req)

			require.Equal(t, tc.wantStatus, w.Code, "body: %s", w.Body.String())
			if tc.wantAccount != "" {
				assert.JSONEq(t, `{"account":"`+tc.wantAccount+`"}`, w.Body.String())
			}
			if tc.wantReason != "" {
				assert.Contains(t, w.Body.String(), string(tc.wantReason))
			}
			// A credential must never be echoed back to the caller.
			for k, v := range tc.headers {
				if k == botauth.HeaderUserID {
					continue // not a secret, and not echoed either way
				}
				assert.NotContains(t, w.Body.String(), v, "token value leaked into the response")
			}
		})
	}
}

// A handler reached without the middleware must read empty, not panic.
func TestAccountFromContext_EmptyWithoutMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	assert.Empty(t, accountFromContext(c))
}
