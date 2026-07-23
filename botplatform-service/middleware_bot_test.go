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
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

func TestRequireBot(t *testing.T) {
	const rawToken = "T3st-bot-tok"
	hash := sessiontoken.Hash(rawToken)

	botSess := &session.Session{
		ID:      hash,
		UserID:  "bot-user-id",
		Account: "myapp.bot",
		SiteID:  "site-a",
		Roles:   []string{"bot"},
	}
	humanSess := &session.Session{
		ID:      hash,
		UserID:  "human-user-id",
		Account: "alice",
		SiteID:  "site-a",
		Roles:   []string{"user"},
	}

	tests := []struct {
		name          string
		xUserID       string
		xAuthToken    string
		findByHashFn  func(ctx context.Context, gotHash string) (*session.Session, error)
		wantStatus    int
		wantReason    string
		wantNextCalls int
	}{
		{
			name:          "missing x-user-id header rejected",
			xUserID:       "",
			xAuthToken:    rawToken,
			wantStatus:    http.StatusUnauthorized,
			wantReason:    "invalid_token",
			wantNextCalls: 0,
		},
		{
			name:          "missing x-auth-token header rejected",
			xUserID:       "bot-user-id",
			xAuthToken:    "",
			wantStatus:    http.StatusUnauthorized,
			wantReason:    "invalid_token",
			wantNextCalls: 0,
		},
		{
			name:       "unknown session hash rejected",
			xUserID:    "bot-user-id",
			xAuthToken: rawToken,
			findByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
				return nil, session.ErrNotFound
			},
			wantStatus:    http.StatusUnauthorized,
			wantReason:    "invalid_token",
			wantNextCalls: 0,
		},
		{
			name:       "session found but userID mismatch rejected",
			xUserID:    "wrong-user-id",
			xAuthToken: rawToken,
			findByHashFn: func(_ context.Context, gotHash string) (*session.Session, error) {
				assert.Equal(t, hash, gotHash)
				return botSess, nil
			},
			wantStatus:    http.StatusUnauthorized,
			wantReason:    "invalid_token",
			wantNextCalls: 0,
		},
		{
			name:       "session without bot role rejected",
			xUserID:    "human-user-id",
			xAuthToken: rawToken,
			findByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
				return humanSess, nil
			},
			wantStatus:    http.StatusForbidden,
			wantReason:    "not_a_bot",
			wantNextCalls: 0,
		},
		{
			name:       "valid bot session passes through",
			xUserID:    "bot-user-id",
			xAuthToken: rawToken,
			findByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
				return botSess, nil
			},
			wantStatus:    http.StatusOK,
			wantNextCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			r := gin.New()
			sessions := &fakeSessionStore{FindByHashFn: tt.findByHashFn}
			nextCalls := 0
			r.GET("/bot", requireBot(sessions), func(c *gin.Context) {
				nextCalls++
				pr := botPrincipalFrom(c)
				require.NotNil(t, pr, "principal must be set on gin context after requireBot")
				assert.Equal(t, "bot-user-id", pr.UserID)
				assert.Equal(t, "myapp.bot", pr.Account)
				assert.Equal(t, "site-a", pr.SiteID)
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/bot", nil)
			if tt.xUserID != "" {
				req.Header.Set("x-user-id", tt.xUserID)
			}
			if tt.xAuthToken != "" {
				req.Header.Set("x-auth-token", tt.xAuthToken)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantNextCalls, nextCalls)
			if tt.wantReason != "" {
				var body map[string]any
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				assertReason(t, body, tt.wantReason)
			}
		})
	}
}

// assertReason compares body["reason"] against want.
func assertReason(t *testing.T, body map[string]any, want string) {
	t.Helper()
	assert.Equal(t, want, body["reason"], "envelope=%v", body)
}
