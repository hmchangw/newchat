package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/bcrypt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// testCfg returns a Config suitable for unit tests (low bcrypt cost).
func testCfg() Config {
	return Config{
		SiteID:     "site-A",
		BcryptCost: bcrypt.MinCost,
	}
}

// setupRouter wires h into a Gin engine with a fake requireAdmin middleware that
// injects a fixed principal, bypassing real session lookup.
func setupRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, Session{
			ID:      "sess-1",
			UserID:  "admin-user-id",
			Account: "p_admin",
			SiteID:  "site-A",
			Roles:   []string{"admin"},
		})
		c.Next()
	})
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/users/:account", h.getUser)
	r.PUT("/users/:account", h.updateUser)
	r.PUT("/users/:account/password", h.setPassword)
	return r
}

// bodyBytes returns request body bytes from any JSON-serialisable value.
func bodyBytes(t *testing.T, v any) *bytes.Buffer {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return bytes.NewBuffer(b)
}

// respBody reads and parses a JSON response body.
func respBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	return m
}

// assertNoSecret checks that the JSON response body contains no bcrypt or
// password material — the response must not leak credential fields.
func assertNoSecret(t *testing.T, body []byte) {
	t.Helper()
	lower := strings.ToLower(string(body))
	assert.NotContains(t, lower, "bcrypt", "response must not contain bcrypt material")
	assert.NotContains(t, lower, `"services"`, "response must not contain services field")
}

// sha256HexOf mirrors pwhash.Hash's sha256 step.
func sha256HexOf(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// -------------------------------------------------------------------------
// createUser tests
// -------------------------------------------------------------------------

func TestHandler_createUser(t *testing.T) {
	falseVal := false
	trueVal := true

	tests := []struct {
		name       string
		body       map[string]any
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name: "happy path – 201, projected, no bcrypt",
			body: map[string]any{
				"account":     "user1",
				"engName":     "User One",
				"chineseName": "用戶一",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assertNoSecret(t, raw)
				assert.Equal(t, "user1", body["account"])
				assert.Equal(t, "site-A", body["siteId"])
				id, ok := body["id"].(string)
				assert.True(t, ok && id != "", "id must be non-empty string")
			},
		},
		{
			name: "empty account → 400 missing_fields",
			body: map[string]any{
				"account":     "",
				"engName":     "User One",
				"chineseName": "用戶一",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AuthMissingFields),
		},
		{
			name: "empty password → 400 missing_fields",
			body: map[string]any{
				"account":     "user1",
				"engName":     "User One",
				"chineseName": "用戶一",
				"password":    "",
				"roles":       []string{"user"},
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AuthMissingFields),
		},
		{
			name: "duplicate account → 409 account_exists",
			body: map[string]any{
				"account":     "existing",
				"engName":     "Dup User",
				"chineseName": "重複",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(ErrAccountExists)
			},
			wantStatus: http.StatusConflict,
			wantReason: string(errcode.AdminAccountExists),
		},
		{
			name: "siteId is forced to cfg.SiteID regardless of body",
			body: map[string]any{
				"account":     "user2",
				"engName":     "User Two",
				"chineseName": "用戶二",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
				"siteId":      "injected-evil-site",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, u *model.User) error {
						assert.Equal(t, "site-A", u.SiteID, "siteId must be forced to cfg.SiteID")
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Equal(t, "site-A", body["siteId"])
			},
		},
		{
			name: "requirePasswordChange defaults to true when omitted",
			body: map[string]any{
				"account":     "user3",
				"engName":     "User Three",
				"chineseName": "用戶三",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
				// requirePasswordChange intentionally absent
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, u *model.User) error {
						assert.True(t, u.RequirePasswordChange, "should default requirePasswordChange to true")
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "requirePasswordChange explicit false is respected",
			body: func() map[string]any {
				type reqBody struct {
					Account               string   `json:"account"`
					EngName               string   `json:"engName"`
					ChineseName           string   `json:"chineseName"`
					Password              string   `json:"password"`
					Roles                 []string `json:"roles"`
					RequirePasswordChange *bool    `json:"requirePasswordChange"`
				}
				// #nosec G117 -- test fixture: marshaling a struct with a password field for HTTP body construction; not a secret leak
				b, _ := json.Marshal(reqBody{
					Account:               "user4",
					EngName:               "User Four",
					ChineseName:           "用戶四",
					Password:              "s3cr3t",
					Roles:                 []string{"user"},
					RequirePasswordChange: &falseVal,
				})
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				return m
			}(),
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, u *model.User) error {
						assert.False(t, u.RequirePasswordChange, "explicit false must be passed through")
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "requirePasswordChange explicit true is respected",
			body: func() map[string]any {
				type reqBody struct {
					Account               string   `json:"account"`
					EngName               string   `json:"engName"`
					ChineseName           string   `json:"chineseName"`
					Password              string   `json:"password"`
					Roles                 []string `json:"roles"`
					RequirePasswordChange *bool    `json:"requirePasswordChange"`
				}
				// #nosec G117 -- test fixture: marshaling a struct with a password field for HTTP body construction; not a secret leak
				b, _ := json.Marshal(reqBody{
					Account:               "user5",
					EngName:               "User Five",
					ChineseName:           "用戶五",
					Password:              "s3cr3t",
					Roles:                 []string{"user"},
					RequirePasswordChange: &trueVal,
				})
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				return m
			}(),
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, u *model.User) error {
						assert.True(t, u.RequirePasswordChange)
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "password stored as bcrypt(sha256_hex(plaintext)) – verifiable",
			body: map[string]any{
				"account":     "user6",
				"engName":     "User Six",
				"chineseName": "用戶六",
				"password":    "myPlaintext",
				"roles":       []string{"user"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, u *model.User) error {
						hash := u.Services.Password.Bcrypt
						assert.NotEmpty(t, hash, "bcrypt hash must be stored")
						expected := sha256HexOf("myPlaintext")
						err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(expected))
						assert.NoError(t, err, "stored hash must verify against bcrypt(sha256_hex(plaintext))")
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "audit triggered with action=user.create and no secret in details",
			body: map[string]any{
				"account":     "user7",
				"engName":     "User Seven",
				"chineseName": "用戶七",
				"password":    "s3cr3t",
				"roles":       []string{"user"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "user.create", e.Action)
						assert.Equal(t, "admin-user-id", e.ActorUserID)
						for k, v := range e.Details {
							assert.NotContains(t, strings.ToLower(k), "password", "detail key must not contain 'password'")
							assert.NotContains(t, strings.ToLower(k), "hash", "detail key must not contain 'hash'")
							assert.NotContains(t, v, "$2a$", "detail value must not contain bcrypt material")
							assert.NotContains(t, v, "$2b$", "detail value must not contain bcrypt material")
						}
						return nil
					})
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/users", bodyBytes(t, tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body, w.Body.Bytes())
			}
		})
	}
}

// -------------------------------------------------------------------------
// listUsers tests
// -------------------------------------------------------------------------

func TestHandler_listUsers(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		checkBody  func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name:  "passes siteID, q, paging to store",
			query: "?q=alice&page=2&limit=10",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), "site-A", "alice", 2, 10).
					Return([]model.User{
						{ID: "u1", Account: "alice", SiteID: "site-A"},
					}, int64(1), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assertNoSecret(t, raw)
				assert.Equal(t, float64(1), body["total"])
				users, ok := body["users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 1)
			},
		},
		{
			name:  "defaults page=1 limit=20",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), "site-A", "", 1, 20).
					Return([]model.User{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Equal(t, float64(0), body["total"])
				users, ok := body["users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 0)
			},
		},
		{
			name:  "limit is clamped to maxPageLimit when larger value is passed",
			query: "?limit=100000",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), "site-A", "", 1, 100).
					Return([]model.User{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "store error → 500",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, int64(0), fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/users"+tc.query, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body, w.Body.Bytes())
			}
		})
	}
}

// -------------------------------------------------------------------------
// getUser tests
// -------------------------------------------------------------------------

func TestHandler_getUser(t *testing.T) {
	tests := []struct {
		name       string
		userID     string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name:   "hit – returns projected user",
			userID: "u1",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().GetUserByAccount(gomock.Any(), "site-A", "u1").Return(&model.User{
					ID:      "u1",
					Account: "alice",
					SiteID:  "site-A",
					Roles:   []model.UserRole{model.UserRoleUser},
				}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assertNoSecret(t, raw)
				assert.Equal(t, "u1", body["id"])
				assert.Equal(t, "alice", body["account"])
			},
		},
		{
			name:   "miss – 404 user_not_found",
			userID: "no-such",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().GetUserByAccount(gomock.Any(), "site-A", "no-such").Return(nil, ErrUserNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.AdminUserNotFound),
		},
		{
			name:   "store error – 500",
			userID: "u2",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().GetUserByAccount(gomock.Any(), "site-A", "u2").Return(nil, fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/users/"+tc.userID, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body, w.Body.Bytes())
			}
		})
	}
}

// -------------------------------------------------------------------------
// updateUser tests
// -------------------------------------------------------------------------

func TestHandler_updateUser(t *testing.T) {
	trueVal := true

	tests := []struct {
		name       string
		userID     string
		body       any
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
	}{
		{
			name:   "update roles – no session revocation",
			userID: "u1",
			body: map[string]any{
				"roles": []string{"admin"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u1", gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "deactivating user – UpdateUser applies the flag (store revokes atomically)",
			userID: "u2",
			body: map[string]any{
				"deactivated": true,
			},
			setupMock: func(m *MockAdminStore) {
				// UpdateUser revokes sessions atomically with the update, so the
				// handler makes no separate DeleteSessionsByAccount call.
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u2", gomock.Any()).
					DoAndReturn(func(_ context.Context, siteID, id string, u UserUpdate) error {
						require.NotNil(t, u.Deactivated)
						assert.True(t, *u.Deactivated)
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "user.update", e.Action)
						assert.Equal(t, "true", e.Details["deactivated"])
						return nil
					})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "update names",
			userID: "u3",
			body: map[string]any{
				"engName":     "New Eng",
				"chineseName": "新中文",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u3", gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "store error – 500",
			userID: "u4",
			body: map[string]any{
				"engName": "X",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u4", gomock.Any()).Return(fmt.Errorf("db err"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:   "user not found – 404 user_not_found",
			userID: "no-such",
			body: map[string]any{
				"engName": "Ghost",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "no-such", gomock.Any()).Return(ErrUserNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.AdminUserNotFound),
		},
		{
			name:   "deactivated=false – plain update",
			userID: "u5",
			body: map[string]any{
				"deactivated": false,
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u5", gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "deactivated=true with explicit pointer serialised",
			userID: "u6",
			body: func() map[string]any {
				type body struct {
					Deactivated *bool `json:"deactivated"`
				}
				b, _ := json.Marshal(body{Deactivated: &trueVal})
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				return m
			}(),
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u6", gomock.Any()).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPut, "/users/"+tc.userID, bodyBytes(t, tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
		})
	}
}

// setupSessionRouter wires the session + audit handlers into a Gin engine.
func setupSessionRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, Session{
			ID:      "sess-1",
			UserID:  "admin-user-id",
			Account: "p_admin",
			SiteID:  "site-A",
			Roles:   []string{"admin"},
		})
		c.Next()
	})
	r.GET("/sessions", h.listSessions)
	r.DELETE("/sessions", h.revokeAllSessions)
	r.DELETE("/sessions/:sessionId", h.revokeSession)
	r.GET("/audit", h.listAudit)
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)
	return r
}

// -------------------------------------------------------------------------
// listSessions tests
// -------------------------------------------------------------------------

func TestHandler_listSessions(t *testing.T) {
	tests := []struct {
		name       string
		account    string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		checkBody  func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name:    "returns projected session fields only",
			account: "alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListSessionsByAccount(gomock.Any(), "site-A", "alice").Return([]Session{
					{
						ID:       "sess-abc",
						UserID:   "u1",
						Account:  "alice",
						SiteID:   "site-A",
						Roles:    []string{"admin"},
						IssuedAt: 1700000000000,
					},
				}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				sessions, ok := body["sessions"].([]any)
				require.True(t, ok, "sessions field must be an array")
				require.Len(t, sessions, 1)

				s := sessions[0].(map[string]any)
				assert.Equal(t, "sess-abc", s["id"], "id must be present")
				assert.Equal(t, "u1", s["userId"], "userId must be present")
				assert.Equal(t, "alice", s["account"], "account must be present")
				assert.Equal(t, "site-A", s["siteId"], "siteId must be present")
				assert.Equal(t, float64(1700000000000), s["issuedAt"], "issuedAt must be present")

				// Roles stay out of the wire projection.
				assert.NotContains(t, s, "roles", "roles must not be exposed")
				rawStr := strings.ToLower(string(raw))
				assert.NotContains(t, rawStr, "\"roles\"", "roles must not appear in response")
			},
		},
		{
			name:    "empty sessions list returns empty array",
			account: "bob",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListSessionsByAccount(gomock.Any(), "site-A", "bob").Return([]Session{}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				sessions, ok := body["sessions"].([]any)
				require.True(t, ok, "sessions field must be an array")
				assert.Len(t, sessions, 0)
			},
		},
		{
			name:    "store error returns 500",
			account: "carol",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListSessionsByAccount(gomock.Any(), "site-A", "carol").Return(nil, fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "missing account query returns 400",
			account:    "",
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupSessionRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/sessions?account="+tc.account, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body, w.Body.Bytes())
			}
		})
	}
}

// -------------------------------------------------------------------------
// revokeAllSessions tests
// -------------------------------------------------------------------------

func TestHandler_revokeAllSessions(t *testing.T) {
	tests := []struct {
		name       string
		account    string
		setupMock  func(m *MockAdminStore)
		wantStatus int
	}{
		{
			name:    "calls DeleteSessionsByAccount and audits session.revoke_all",
			account: "alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeleteSessionsByAccount(gomock.Any(), "site-A", "alice").Return(int64(3), nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "session.revoke_all", e.Action)
						assert.Equal(t, "alice", e.TargetAccount)
						assert.Equal(t, "admin-user-id", e.ActorUserID)
						assert.Equal(t, "site-A", e.SiteID)
						return nil
					})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "store error returns 500",
			account: "bob",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeleteSessionsByAccount(gomock.Any(), "site-A", "bob").Return(int64(0), fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "missing account query returns 400",
			account:    "",
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupSessionRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodDelete, "/sessions?account="+tc.account, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

// -------------------------------------------------------------------------
// revokeSession tests
// -------------------------------------------------------------------------

func TestHandler_revokeSession(t *testing.T) {
	tests := []struct {
		name      string
		account   string
		sessionID string
		setupMock func(m *MockAdminStore)
		wantCode  int
	}{
		{
			name:      "calls DeleteSession and audits session.revoke with sessionId detail",
			account:   "alice",
			sessionID: "sess-xyz",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeleteSession(gomock.Any(), "site-A", "alice", "sess-xyz").Return(int64(1), nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "session.revoke", e.Action)
						assert.Equal(t, "alice", e.TargetAccount)
						assert.Equal(t, "admin-user-id", e.ActorUserID)
						assert.Equal(t, "site-A", e.SiteID)
						assert.Equal(t, "sess-xyz", e.Details["sessionId"])
						return nil
					})
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "store error returns 500",
			account:   "bob",
			sessionID: "sess-abc",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeleteSession(gomock.Any(), "site-A", "bob", "sess-abc").Return(int64(0), fmt.Errorf("db offline"))
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name:      "missing account query returns 400",
			account:   "",
			sessionID: "sess-abc",
			setupMock: func(m *MockAdminStore) {},
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupSessionRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodDelete, "/sessions/"+tc.sessionID+"?account="+tc.account, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantCode, w.Code)
		})
	}
}

// -------------------------------------------------------------------------
// listAudit tests
// -------------------------------------------------------------------------

func TestHandler_listAudit(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name:  "passes siteID, filters, and paging to store",
			query: "?targetAccount=alice&actor=p_bob&action=user.create&page=2&limit=5",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListAudit(gomock.Any(), "site-A",
					AuditFilter{TargetAccount: "alice", Actor: "p_bob", Action: "user.create"},
					2, 5,
				).Return([]AuditEntry{
					{ID: "e1", Action: "user.create", ActorUserID: "admin-user-id", SiteID: "site-A", Timestamp: 1700000002000},
					{ID: "e2", Action: "user.create", ActorUserID: "admin-user-id", SiteID: "site-A", Timestamp: 1700000001000},
				}, int64(2), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(2), body["total"])
				entries, ok := body["entries"].([]any)
				require.True(t, ok)
				assert.Len(t, entries, 2)
				// First entry should have higher timestamp (newest-first)
				e0 := entries[0].(map[string]any)
				e1 := entries[1].(map[string]any)
				assert.Greater(t, e0["timestamp"], e1["timestamp"], "entries must be newest-first")
			},
		},
		{
			name:  "defaults page=1 limit=20 when params absent",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListAudit(gomock.Any(), "site-A", AuditFilter{}, 1, 20).
					Return([]AuditEntry{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(0), body["total"])
			},
		},
		{
			name:  "store error returns 500",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListAudit(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, int64(0), fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupSessionRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/audit"+tc.query, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body)
			}
		})
	}
}

// -------------------------------------------------------------------------
// healthz / readyz tests
// -------------------------------------------------------------------------

func TestHandler_healthz(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	h := newHandler(m, testCfg())
	r := setupSessionRouter(h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := respBody(t, w)
	assert.Equal(t, "ok", body["status"])
}

func TestHandler_readyz(t *testing.T) {
	t.Run("ping ok returns 200", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().Ping(gomock.Any()).Return(nil)

		h := newHandler(m, testCfg())
		r := setupSessionRouter(h)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		body := respBody(t, w)
		assert.Equal(t, "ok", body["status"])
	})

	t.Run("ping error returns 503", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().Ping(gomock.Any()).Return(fmt.Errorf("mongo unreachable"))

		h := newHandler(m, testCfg())
		r := setupSessionRouter(h)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// -------------------------------------------------------------------------
// setPassword tests
// -------------------------------------------------------------------------

func TestHandler_setPassword(t *testing.T) {
	tests := []struct {
		name       string
		userID     string
		body       any
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
	}{
		{
			name:   "happy path – hashes, sets requireChange (store revokes atomically)",
			userID: "u1",
			body:   map[string]any{"password": "newSecret123", "requirePasswordChange": true},
			setupMock: func(m *MockAdminStore) {
				// UpdateUserPassword revokes the account's sessions atomically
				// with the hash change, so the handler makes no separate call.
				m.EXPECT().UpdateUserPassword(gomock.Any(), "site-A", "u1", gomock.Any(), true).
					DoAndReturn(func(_ context.Context, siteID, id, hash string, requireChange bool) error {
						expected := sha256HexOf("newSecret123")
						err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(expected))
						assert.NoError(t, err, "stored hash must verify against bcrypt(sha256_hex(plaintext))")
						return nil
					})
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "user.password.set", e.Action)
						// Values must not contain credential material
						for _, v := range e.Details {
							assert.NotContains(t, v, "$2a$", "detail value must not contain bcrypt hash")
							assert.NotContains(t, v, "$2b$", "detail value must not contain bcrypt hash")
						}
						// The plaintext password itself must not be a value
						for _, v := range e.Details {
							assert.NotEqual(t, "newSecret123", v, "plaintext password must not appear in details")
						}
						return nil
					})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty password → 400 missing_fields",
			userID:     "u1",
			body:       map[string]any{"password": ""},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AuthMissingFields),
		},
		{
			name:       "missing password field → 400 missing_fields",
			userID:     "u1",
			body:       map[string]any{},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AuthMissingFields),
		},
		{
			name:   "store UpdateUserPassword error → 500",
			userID: "u2",
			body:   map[string]any{"password": "apassword"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPassword(gomock.Any(), "site-A", "u2", gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:   "user not found – 404 user_not_found",
			userID: "no-such",
			body:   map[string]any{"password": "somepass"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPassword(gomock.Any(), "site-A", "no-such", gomock.Any(), gomock.Any()).
					Return(ErrUserNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.AdminUserNotFound),
		},
		{
			name:   "requirePasswordChange defaults to true when omitted",
			userID: "u3",
			body:   map[string]any{"password": "somepass"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPassword(gomock.Any(), "site-A", "u3", gomock.Any(), true).Return(nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, testCfg())
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPut, "/users/"+tc.userID+"/password", bodyBytes(t, tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
		})
	}
}
