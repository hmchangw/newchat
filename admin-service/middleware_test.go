package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// newTestContext creates a gin.Context backed by a recorder, optionally setting
// an Authorization header, and returns the context plus the recorder.
func newTestContext(method, path, authHeader string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(method, path, nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	c.Request = req
	return c, w
}

func TestRequireAdmin(t *testing.T) {
	const (
		rawToken       = "valid-raw-token-abc123"
		configuredSite = "site-local"
	)
	adminSession := &session.Session{
		ID:      "hash-of-token",
		UserID:  "user-1",
		Account: "admin@example.com",
		SiteID:  configuredSite,
		Roles:   []string{"admin"},
	}
	nonAdminSession := &session.Session{
		ID:      "hash-of-token",
		UserID:  "user-2",
		Account: "user@example.com",
		SiteID:  configuredSite,
		Roles:   []string{"user"},
	}
	adminOtherSiteSession := &session.Session{
		ID:      "hash-of-token",
		UserID:  "user-3",
		Account: "admin2@example.com",
		SiteID:  "site-other",
		Roles:   []string{"admin"},
	}

	tests := []struct {
		name           string
		authHeader     string
		setupSessions  func(s *fakeSessionStore)
		wantStatus     int
		wantReason     string
		wantNextCalled bool
	}{
		{
			name:       "no Authorization header → 401 invalid_token, no store call",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
			wantReason: "invalid_token",
		},
		{
			name:       "Authorization without Bearer prefix → 401 invalid_token, no store call",
			authHeader: "Basic sometoken",
			wantStatus: http.StatusUnauthorized,
			wantReason: "invalid_token",
		},
		{
			name:       "Bearer present but FindByHash errors → 401 invalid_token",
			authHeader: "Bearer " + rawToken,
			setupSessions: func(s *fakeSessionStore) {
				s.FindByHashFn = func(_ context.Context, hash string) (*session.Session, error) {
					return nil, errNotFoundSentinel
				}
			},
			wantStatus: http.StatusUnauthorized,
			wantReason: "invalid_token",
		},
		{
			name:       "session found but lacks admin role → 403 not_admin",
			authHeader: "Bearer " + rawToken,
			setupSessions: func(s *fakeSessionStore) {
				s.FindByHashFn = func(_ context.Context, hash string) (*session.Session, error) {
					return nonAdminSession, nil
				}
			},
			wantStatus: http.StatusForbidden,
			wantReason: "not_admin",
		},
		{
			name:       "admin session from different site → 403 not_admin, next NOT called",
			authHeader: "Bearer " + rawToken,
			setupSessions: func(s *fakeSessionStore) {
				s.FindByHashFn = func(_ context.Context, hash string) (*session.Session, error) {
					return adminOtherSiteSession, nil
				}
			},
			wantStatus:     http.StatusForbidden,
			wantReason:     "not_admin",
			wantNextCalled: false,
		},
		{
			name:       "admin session matching configured site → next handler runs, principal in context",
			authHeader: "Bearer " + rawToken,
			setupSessions: func(s *fakeSessionStore) {
				s.FindByHashFn = func(_ context.Context, hash string) (*session.Session, error) {
					return adminSession, nil
				}
			},
			wantStatus:     http.StatusOK,
			wantNextCalled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			c, w := newTestContext(http.MethodGet, "/admin/test", tc.authHeader)

			nextCalled := false
			mw := requireAdmin(sess, configuredSite)

			// Simulate gin chain: run middleware, then check if next would run.
			// We replicate gin's chain logic manually: if the middleware calls
			// c.Next() the next handler runs; if it calls c.Abort() it doesn't.
			mw(c)
			if !c.IsAborted() {
				nextCalled = true
				// Write 200 to signal next was reached.
				c.JSON(http.StatusOK, gin.H{"status": "ok"})
			}

			assert.Equal(t, tc.wantNextCalled, nextCalled, "next handler called")
			assert.Equal(t, tc.wantStatus, w.Code, "HTTP status")

			if tc.wantReason != "" {
				var body map[string]any
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				assert.Equal(t, tc.wantReason, body["reason"], "reason field")
			}

			if tc.wantNextCalled {
				got := principalFrom(c)
				assert.Equal(t, adminSession.UserID, got.UserID, "principal UserID")
				assert.Equal(t, adminSession.Account, got.Account, "principal Account")
			}
		})
	}
}

// errNotFoundSentinel stands in for any session store miss (session.ErrNotFound).
var errNotFoundSentinel = session.ErrNotFound
