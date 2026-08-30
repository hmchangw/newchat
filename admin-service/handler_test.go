package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/bcrypt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/subject"
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

// emptySessionStore returns a fakeSessionStore with every method left unset —
// safe to pass into newHandler for tests whose code path never calls
// h.sessions (calling an unset Fn panics loudly if that assumption breaks).
func emptySessionStore() *fakeSessionStore {
	return &fakeSessionStore{}
}

// setupRouter wires h into a Gin engine with a fake requireAdmin middleware that
// injects a fixed principal, bypassing real session lookup.
func setupRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
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
	r.PATCH("/users/:account", h.updateUser)
	r.POST("/users/:account/password", h.setPassword)
	r.POST("/users/:account/resync", h.resyncUser)
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

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
			name:  "passes q and paging to store",
			query: "?q=alice&page=2&limit=10",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), "alice", 2, 10).
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
				m.EXPECT().SearchUsers(gomock.Any(), "", 1, 20).
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
				m.EXPECT().SearchUsers(gomock.Any(), "", 1, 100).
					Return([]model.User{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "store error → 500",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().SearchUsers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
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

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
				assert.Equal(t, true, body["active"], "missing active field must read as active")
				_, has := body["deactivated"]
				assert.False(t, has, "deactivated must be gone from the wire")
			},
		},
		{
			name:   "inactive user – active=false in view",
			userID: "u1b",
			setupMock: func(m *MockAdminStore) {
				inactive := false
				m.EXPECT().GetUserByAccount(gomock.Any(), "site-A", "u1b").Return(&model.User{
					ID:      "u1b",
					Account: "mallory",
					SiteID:  "site-A",
					Active:  &inactive,
				}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Equal(t, false, body["active"])
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

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
	falseVal := false

	tests := []struct {
		name          string
		userID        string
		body          any
		setupMock     func(m *MockAdminStore)
		setupSessions func(s *fakeSessionStore)
		wantStatus    int
		wantReason    string
	}{
		{
			name:   "update roles – no session revocation",
			userID: "u1",
			body: map[string]any{
				"roles": []string{"admin"},
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u1", gomock.Any()).Return(&model.User{Account: "u1"}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "deactivating user (active=false) – atomic DeactivateAndRevoke",
			userID: "u2",
			body: map[string]any{
				"active": false,
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeactivateAndRevoke(gomock.Any(), "site-A", "u2").Return(&model.User{Account: "u2"}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "user.update", e.Action)
						assert.Equal(t, "false", e.Details["active"])
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
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u3", gomock.Any()).Return(&model.User{Account: "u3"}, nil)
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
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u4", gomock.Any()).Return(nil, fmt.Errorf("db err"))
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
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "no-such", gomock.Any()).Return(nil, ErrUserNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.AdminUserNotFound),
		},
		{
			name:   "active=true – plain update (reactivate)",
			userID: "u5",
			body: map[string]any{
				"active": true,
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u5", gomock.Any()).Return(&model.User{Account: "u5"}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "active=false with explicit pointer serialised",
			userID: "u6",
			body: func() map[string]any {
				type body struct {
					Active *bool `json:"active"`
				}
				b, _ := json.Marshal(body{Active: &falseVal})
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				return m
			}(),
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeactivateAndRevoke(gomock.Any(), "site-A", "u6").Return(&model.User{Account: "u6"}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "active=false + engName rejected as mixed patch",
			userID: "u7",
			body: map[string]any{
				"active":  false,
				"engName": "Would Be Dropped",
			},
			// no DeactivateAndRevoke / UpdateUser / AppendAudit — must reject before any write
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AdminMixedDeactivatePatch),
		},
		{
			name:   "active=false + roles rejected as mixed patch",
			userID: "u8",
			body: map[string]any{
				"active": false,
				"roles":  []string{"admin"},
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.AdminMixedDeactivatePatch),
		},
		{
			name:   "active=true + engName still goes through UpdateUser (reactivate + edit is fine)",
			userID: "u9",
			body: func() map[string]any {
				type body struct {
					Active  *bool  `json:"active"`
					EngName string `json:"engName"`
				}
				b, _ := json.Marshal(body{Active: &trueVal, EngName: "Reactivated Rita"})
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				return m
			}(),
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-A", "u9", gomock.Any()).Return(&model.User{Account: "u9"}, nil)
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

			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			h := newHandler(m, sess, testCfg(), nil, nil)
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPatch, "/users/"+tc.userID, bodyBytes(t, tc.body))
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

// TestHandler_updateUser_DeactivateAndRevoke_TxError_Returns500 covers the
// atomic transaction's failure path: the user flag and the session revoke
// are now one Mongo transaction (DeactivateAndRevoke), so a failure there
// means nothing was applied — the handler surfaces it as a 500, and
// AppendAudit must not fire.
func TestHandler_updateUser_DeactivateAndRevoke_TxError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)
	m.EXPECT().DeactivateAndRevoke(gomock.Any(), "site-A", "u2b").Return(nil, fmt.Errorf("mongo dead"))
	// no AppendAudit expectation — must not fire

	h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
	r := setupRouter(h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/users/u2b", bodyBytes(t, map[string]any{"active": false}))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// setupSessionRouter wires the session + audit handlers into a Gin engine.
func setupSessionRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
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
		name          string
		account       string
		setupSessions func(s *fakeSessionStore)
		wantStatus    int
		checkBody     func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name:    "returns projected session fields only",
			account: "alice",
			setupSessions: func(s *fakeSessionStore) {
				s.ListForAccountFn = func(_ context.Context, siteID, account string) ([]session.Session, error) {
					assert.Equal(t, "site-A", siteID)
					assert.Equal(t, "alice", account)
					return []session.Session{
						{
							ID:       "sess-abc",
							UserID:   "u1",
							Account:  "alice",
							SiteID:   "site-A",
							Roles:    []string{"admin"},
							IssuedAt: 1700000000000,
						},
					}, nil
				}
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
			setupSessions: func(s *fakeSessionStore) {
				s.ListForAccountFn = func(_ context.Context, siteID, account string) ([]session.Session, error) {
					return []session.Session{}, nil
				}
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
			setupSessions: func(s *fakeSessionStore) {
				s.ListForAccountFn = func(_ context.Context, siteID, account string) ([]session.Session, error) {
					return nil, fmt.Errorf("db offline")
				}
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "missing account query returns 400",
			account:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			h := newHandler(m, sess, testCfg(), nil, nil)
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
		name          string
		account       string
		setupMock     func(m *MockAdminStore)
		setupSessions func(s *fakeSessionStore)
		wantStatus    int
	}{
		{
			name:    "calls DeleteForAccount and audits session.revoke_all",
			account: "alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, e *AuditEntry) error {
						assert.Equal(t, "session.revoke_all", e.Action)
						assert.Equal(t, "alice", e.TargetAccount)
						assert.Equal(t, "admin-user-id", e.ActorUserID)
						assert.Equal(t, "site-A", e.SiteID)
						return nil
					})
			},
			setupSessions: func(s *fakeSessionStore) {
				s.DeleteForAccountFn = func(_ context.Context, siteID, account string) ([]string, error) {
					assert.Equal(t, "site-A", siteID)
					assert.Equal(t, "alice", account)
					return []string{"h1", "h2", "h3"}, nil
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "store error returns 500",
			account: "bob",
			setupSessions: func(s *fakeSessionStore) {
				s.DeleteForAccountFn = func(_ context.Context, siteID, account string) ([]string, error) {
					return nil, fmt.Errorf("db offline")
				}
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "missing account query returns 400",
			account:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			if tc.setupMock != nil {
				tc.setupMock(m)
			}

			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			h := newHandler(m, sess, testCfg(), nil, nil)
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
		name          string
		account       string
		sessionID     string
		setupMock     func(m *MockAdminStore)
		setupSessions func(s *fakeSessionStore)
		wantCode      int
	}{
		{
			name:      "calls DeleteByID and audits session.revoke with sessionId detail",
			account:   "alice",
			sessionID: "sess-xyz",
			setupMock: func(m *MockAdminStore) {
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
			setupSessions: func(s *fakeSessionStore) {
				s.DeleteByIDFn = func(_ context.Context, siteID, account, id string) (int64, error) {
					assert.Equal(t, "site-A", siteID)
					assert.Equal(t, "alice", account)
					assert.Equal(t, "sess-xyz", id)
					return 1, nil
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "store error returns 500",
			account:   "bob",
			sessionID: "sess-abc",
			setupSessions: func(s *fakeSessionStore) {
				s.DeleteByIDFn = func(_ context.Context, siteID, account, id string) (int64, error) {
					return 0, fmt.Errorf("db offline")
				}
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name:      "missing account query returns 400",
			account:   "",
			sessionID: "sess-abc",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			if tc.setupMock != nil {
				tc.setupMock(m)
			}

			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			h := newHandler(m, sess, testCfg(), nil, nil)
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

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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

	h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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

		h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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

		h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
		name          string
		userID        string
		body          any
		setupMock     func(m *MockAdminStore)
		setupSessions func(s *fakeSessionStore)
		wantStatus    int
		wantReason    string
	}{
		{
			name:   "happy path – hashes, sets requireChange, atomically revokes all sessions",
			userID: "u1",
			body:   map[string]any{"password": "newSecret123", "requirePasswordChange": true},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "u1", gomock.Any(), true, "").
					DoAndReturn(func(_ context.Context, siteID, id, hash string, requireChange bool, exceptSessionID string) error {
						expected := sha256HexOf("newSecret123")
						err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(expected))
						assert.NoError(t, err, "stored hash must verify against bcrypt(sha256_hex(plaintext))")
						assert.Empty(t, exceptSessionID, "admin-forced reset must revoke every session, no exception")
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
			name:   "store UpdateUserPasswordAndRevoke error → 500",
			userID: "u2",
			body:   map[string]any{"password": "apassword"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "u2", gomock.Any(), gomock.Any(), "").
					Return(fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:   "user not found – 404 user_not_found",
			userID: "no-such",
			body:   map[string]any{"password": "somepass"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "no-such", gomock.Any(), gomock.Any(), "").
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
				m.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "u3", gomock.Any(), true, "").Return(nil)
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

			sess := emptySessionStore()
			if tc.setupSessions != nil {
				tc.setupSessions(sess)
			}

			h := newHandler(m, sess, testCfg(), nil, nil)
			r := setupRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/users/"+tc.userID+"/password", bodyBytes(t, tc.body))
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

// TestHandler_setPassword_TxError_Returns500 covers the atomic transaction's
// failure path: the password write and the all-sessions revoke are now one
// Mongo transaction (UpdateUserPasswordAndRevoke), so a failure there means
// nothing was applied — the handler surfaces it as a 500, and AppendAudit
// must not fire.
func TestHandler_setPassword_TxError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)
	m.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "u9", gomock.Any(), true, "").
		Return(fmt.Errorf("mongo dead"))
	// no AppendAudit expectation — must not fire

	h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
	r := setupRouter(h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users/u9/password", bodyBytes(t, map[string]any{"password": "newSecret123"}))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// -------------------------------------------------------------------------
// createUser / updateUser — cross-site account fanout (fanoutUserAccount)
// -------------------------------------------------------------------------

// fanoutCapture records every account-fanout publish, including the encoding
// header value the HR lane depends on. The INBOX lanes run one goroutine per
// destination, so recording is mutex-guarded; publishes whose subject starts
// with failSubjPrefix fail, standing in for one unreachable lane.
type fanoutCapture struct {
	mu    sync.Mutex
	calls []struct {
		Subj, Encoding string
		Data           []byte
	}
	failSubjPrefix string
}

func (f *fanoutCapture) publish(_ context.Context, subj string, data []byte, encoding string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct {
		Subj, Encoding string
		Data           []byte
	}{subj, encoding, data})
	if f.failSubjPrefix != "" && strings.HasPrefix(subj, f.failSubjPrefix) {
		return errors.New("publish failed")
	}
	return nil
}

// hrMsg is the minimal jetstream.Msg natsutil.DecodePayload reads — Data and
// Headers only; the embedded nil interface covers the rest of the surface.
type hrMsg struct {
	jetstream.Msg
	data    []byte
	headers nats.Header
}

func (m hrMsg) Data() []byte         { return m.data }
func (m hrMsg) Headers() nats.Header { return m.headers }

// decodeHRPayload decodes an HR-lane body the way hr-sync-worker does — zstd per
// the Nats-Encoding header — and unmarshals the bare users.upsert array. The raw
// JSON comes back too so a test can assert which keys never reach the wire.
func decodeHRPayload(t *testing.T, data []byte) (users []model.IUserWithChange, raw []byte) {
	t.Helper()
	hdr := nats.Header{}
	hdr.Set(natsutil.HeaderNatsEncoding, natsutil.EncodingZstd)
	raw, err := natsutil.DecodePayload(hrMsg{data: data, headers: hdr})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &users))
	return users, raw
}

// accountFanoutCfg is the 3-site fanout fixture plus the cheap test bcrypt cost —
// createUser hashes a password before it ever reaches the fanout.
func accountFanoutCfg() Config {
	cfg := fanoutTestCfg()
	cfg.BcryptCost = bcrypt.MinCost
	return cfg
}

// createUserBody is a valid POST /users body carrying the given roles.
func createUserBody(roles []string) map[string]any {
	return map[string]any{
		"account": "user1", "engName": "User One", "chineseName": "User One CN",
		"password": "s3cr3t", "roles": roles,
	}
}

// doUserRequest runs one request against h's user routes and returns the recorder.
func doUserRequest(t *testing.T, h *Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, bodyBytes(t, body))
	req.Header.Set("Content-Type", "application/json")
	setupRouter(h).ServeHTTP(w, req)
	return w
}

// hrSubjPrefix splits a capture into its two lanes.
const hrSubjPrefix = "chat.hr."

// hrCall returns the single HR-lane publish, or nil when the lane stayed silent.
func hrCall(f *fanoutCapture) (subj, encoding string, data []byte, found bool) {
	for _, c := range f.calls {
		if strings.HasPrefix(c.Subj, hrSubjPrefix) {
			return c.Subj, c.Encoding, c.Data, true
		}
	}
	return "", "", nil, false
}

// assertSnapshotLanes checks that each expected destination got exactly one
// uncompressed user_account_updated envelope and returns the decoded snapshots.
func assertSnapshotLanes(t *testing.T, f *fanoutCapture, dests []string) map[string]model.UserAccountUpdated {
	t.Helper()
	got := make(map[string]model.UserAccountUpdated, len(dests))
	for _, c := range f.calls {
		if strings.HasPrefix(c.Subj, hrSubjPrefix) {
			continue
		}
		var evt model.InboxEvent
		require.NoError(t, json.Unmarshal(c.Data, &evt))
		var snap model.UserAccountUpdated
		require.NoError(t, json.Unmarshal(evt.Payload, &snap))

		assert.Equal(t, model.InboxUserAccountUpdated, evt.Type)
		assert.Equal(t, subject.InboxExternal(evt.DestSiteID, model.InboxUserAccountUpdated), c.Subj)
		assert.Equal(t, "site-a", evt.SiteID)
		assert.Empty(t, c.Encoding, "the INBOX lane is uncompressed")
		assert.NotZero(t, snap.Timestamp, "payload timestamp is the remote watermark")
		assert.Equal(t, evt.Timestamp, snap.Timestamp)
		assert.Contains(t, string(evt.Payload), `"roles":[`, "roles must never be null on the wire")
		got[evt.DestSiteID] = snap
	}
	require.Len(t, got, len(dests))
	for _, d := range dests {
		require.Contains(t, got, d)
	}
	return got
}

// assertNoFanoutFields asserts the response omits both fanout failure keys.
func assertNoFanoutFields(t *testing.T, body map[string]any) {
	t.Helper()
	_, hasSync := body["syncFailures"]
	assert.False(t, hasSync, "syncFailures must be omitted when every publish lands")
	_, hasHR := body["hrSyncFailed"]
	assert.False(t, hasHR, "hrSyncFailed must be omitted when the HR publish lands")
}

func TestHandler_createUser_Fanout(t *testing.T) {
	t.Run("publishes the HR identity bootstrap and one snapshot per remote site", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users", createUserBody([]string{"bot"}))
		require.Equal(t, http.StatusCreated, w.Code)
		body := respBody(t, w)
		assertNoSecret(t, w.Body.Bytes())
		assertNoFanoutFields(t, body)

		require.Len(t, pub.calls, 3, "1 HR bootstrap + 1 snapshot per remote site")

		subj, encoding, data, found := hrCall(&pub)
		require.True(t, found, "the create must bootstrap identity on the HR lane")
		assert.Equal(t, subject.OrgSyncUsersUpsert("site-a"), subj)
		assert.Equal(t, natsutil.EncodingZstd, encoding)
		users, raw := decodeHRPayload(t, data)
		require.Len(t, users, 1)
		assert.Equal(t, body["id"], users[0].ID)
		assert.Equal(t, "user1", users[0].Account)
		assert.Equal(t, "site-a", users[0].SiteID)
		assert.Equal(t, "User One", users[0].EngName)
		assert.Equal(t, "User One CN", users[0].ChineseName)
		assert.Empty(t, users[0].Roles, "the HR lane carries identity only")
		assert.Empty(t, users[0].ChangeType)
		assert.NotContains(t, string(raw), "roles")
		assert.NotContains(t, string(raw), "changeType")
		assert.NotContains(t, string(raw), "services")

		for dest, snap := range assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"}) {
			assert.Equal(t, body["id"], snap.ID, dest)
			assert.Equal(t, "user1", snap.Account, dest)
			assert.Equal(t, "site-a", snap.SiteID, dest)
			assert.Equal(t, "User One", snap.EngName, dest)
			assert.Equal(t, "User One CN", snap.ChineseName, dest)
			assert.Equal(t, []model.UserRole{model.UserRoleBot}, snap.Roles, dest)
			assert.True(t, snap.Active, dest)
		}
	})

	t.Run("HR publish fails → hrSyncFailed, snapshots still land", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		pub := fanoutCapture{failSubjPrefix: hrSubjPrefix}
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users", createUserBody([]string{"user"}))
		require.Equal(t, http.StatusCreated, w.Code)
		body := respBody(t, w)
		assert.Equal(t, true, body["hrSyncFailed"])
		_, hasSync := body["syncFailures"]
		assert.False(t, hasSync, "a failed HR bootstrap is not a per-site sync failure")
		assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"})
	})

	t.Run("one INBOX dest fails → syncFailures names only it", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		pub := fanoutCapture{failSubjPrefix: "chat.inbox.site-c."}
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users", createUserBody([]string{"user"}))
		require.Equal(t, http.StatusCreated, w.Code)
		body := respBody(t, w)
		assert.Equal(t, []any{"site-c"}, body["syncFailures"])
		_, hasHR := body["hrSyncFailed"]
		assert.False(t, hasHR)
		// site-b was still attempted and delivered.
		assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"})
	})

	t.Run("nil publish → no fanout, plain view", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, nil)

		w := doUserRequest(t, h, http.MethodPost, "/users", createUserBody([]string{"user"}))
		require.Equal(t, http.StatusCreated, w.Code)
		body := respBody(t, w)
		assert.Equal(t, "user1", body["account"])
		assertNoFanoutFields(t, body)
	})

	t.Run("a canceled client ctx still replicates every lane", func(t *testing.T) {
		// The create already committed and nothing retries this path, so the fanout
		// must be severed from the client's cancellation (context.WithoutCancel) and
		// run on its own deadline — same contract as publishPermissionFanout.
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		var pub fanoutCapture
		var mu sync.Mutex
		var callErr error
		var callDeadline time.Time
		var callHasDeadline bool
		publish := func(ctx context.Context, subj string, data []byte, encoding string) error {
			mu.Lock()
			callErr = errors.Join(callErr, ctx.Err())
			callDeadline, callHasDeadline = ctx.Deadline()
			mu.Unlock()
			return pub.publish(ctx, subj, data, encoding)
		}
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, publish)

		reqCtx, cancelReq := context.WithCancel(context.Background())
		cancelReq() // the client hung up before the handler even ran
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/users", bodyBytes(t, createUserBody([]string{"user"}))).WithContext(reqCtx)
		req.Header.Set("Content-Type", "application/json")
		setupRouter(h).ServeHTTP(w, req)

		require.Equal(t, http.StatusCreated, w.Code, "a canceled client must not fail a create that already committed")
		assertNoFanoutFields(t, respBody(t, w))
		mu.Lock()
		defer mu.Unlock()
		assert.NoError(t, callErr, "the publish ctx must not inherit the client's cancellation")
		require.True(t, callHasDeadline, "the fanout must carry its own deadline, not run unbounded")
		assert.WithinDuration(t, time.Now().Add(h.cfg.FanoutTimeout), callDeadline, 2*time.Second,
			"the fanout deadline must be the configured fanout budget")
		require.Len(t, pub.calls, 3, "every lane is still attempted")
		assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"})
	})

	t.Run("fanout deadline is capped by the whole-request budget, not a fresh FANOUT_TIMEOUT", func(t *testing.T) {
		assertFanoutCappedByRequestBudget(t, func(publish func(context.Context, string, []byte, string) error) *Handler {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(nil)
			m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			cfg := accountFanoutCfg()
			cfg.FanoutTimeout = time.Hour // far past the request budget: the request budget must win
			return newHandler(m, emptySessionStore(), cfg, nil, publish)
		}, http.MethodPost, "/users", createUserBody([]string{"user"}), http.StatusCreated)
	})

	t.Run("local write fails → nothing is published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().CreateUser(gomock.Any(), gomock.Any()).Return(ErrAccountExists)
		// no AppendAudit expectation — must not fire

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users", createUserBody([]string{"user"}))
		assert.Equal(t, http.StatusConflict, w.Code)
		assert.Equal(t, string(errcode.AdminAccountExists), respBody(t, w)["reason"])
		assert.Empty(t, pub.calls, "a failed local write must not fan out")
	})
}

func TestHandler_updateUser_Fanout(t *testing.T) {
	inactive := false
	active := true

	tests := []struct {
		name      string
		body      map[string]any
		setupMock func(m *MockAdminStore)
		check     func(t *testing.T, snap model.UserAccountUpdated)
	}{
		{
			name: "roles patch → snapshot carries the new roles",
			body: map[string]any{"roles": []string{"admin"}},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-a", "alice", gomock.Any()).
					Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice",
						ChineseName: "Alice CN", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			check: func(t *testing.T, snap model.UserAccountUpdated) {
				assert.Equal(t, []model.UserRole{model.UserRoleAdmin}, snap.Roles)
				assert.Equal(t, "Alice", snap.EngName)
				assert.Equal(t, "Alice CN", snap.ChineseName)
				assert.True(t, snap.Active)
			},
		},
		{
			name: "names patch → snapshot carries the new name",
			body: map[string]any{"engName": "New"},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-a", "alice", gomock.Any()).
					Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a", EngName: "New"}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			check: func(t *testing.T, snap model.UserAccountUpdated) {
				assert.Equal(t, "New", snap.EngName)
				assert.Empty(t, snap.Roles, "a doc with no roles still sends an empty array")
			},
		},
		{
			name: "deactivate → snapshot carries active:false",
			body: map[string]any{"active": false},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().DeactivateAndRevoke(gomock.Any(), "site-a", "alice").
					Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a", Active: &inactive}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			check: func(t *testing.T, snap model.UserAccountUpdated) {
				assert.False(t, snap.Active)
			},
		},
		{
			name: "reactivate → snapshot carries active:true",
			body: map[string]any{"active": true},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().UpdateUser(gomock.Any(), "site-a", "alice", gomock.Any()).
					Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a", Active: &active}, nil)
				m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			},
			check: func(t *testing.T, snap model.UserAccountUpdated) {
				assert.True(t, snap.Active)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			var pub fanoutCapture
			h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

			w := doUserRequest(t, h, http.MethodPatch, "/users/alice", tc.body)
			require.Equal(t, http.StatusOK, w.Code)
			body := respBody(t, w)
			assert.Equal(t, "ok", body["status"])
			assertNoFanoutFields(t, body)

			_, _, _, hrFound := hrCall(&pub)
			assert.False(t, hrFound, "identity bootstrap is a create-only lane")
			require.Len(t, pub.calls, 2, "one snapshot per remote site")
			for dest, snap := range assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"}) {
				assert.Equal(t, "u1", snap.ID, dest)
				assert.Equal(t, "alice", snap.Account, dest)
				assert.Equal(t, "site-a", snap.SiteID, dest)
				tc.check(t, snap)
			}
		})
	}

	t.Run("fanout deadline is capped by the whole-request budget, not a fresh FANOUT_TIMEOUT", func(t *testing.T) {
		assertFanoutCappedByRequestBudget(t, func(publish func(context.Context, string, []byte, string) error) *Handler {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			m.EXPECT().UpdateUser(gomock.Any(), "site-a", "alice", gomock.Any()).
				Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
			m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)
			cfg := accountFanoutCfg()
			cfg.FanoutTimeout = time.Hour // far past the request budget: the request budget must win
			return newHandler(m, emptySessionStore(), cfg, nil, publish)
		}, http.MethodPatch, "/users/alice", map[string]any{"engName": "New"}, http.StatusOK)
	})

	t.Run("empty patch → no fanout", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().UpdateUser(gomock.Any(), "site-a", "alice", gomock.Any()).Return(nil, nil)
		m.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPatch, "/users/alice", map[string]any{})
		require.Equal(t, http.StatusOK, w.Code)
		body := respBody(t, w)
		assert.Equal(t, "ok", body["status"])
		assertNoFanoutFields(t, body)
		assert.Empty(t, pub.calls, "nothing was written, so nothing is replicated")
	})
}

// assertFanoutCappedByRequestBudget drives one request through a handler whose
// FanoutTimeout is far past the request budget and asserts the captured fanout
// deadline is the request budget measured from handler entry. net/http's
// WriteTimeout runs from the request, not from the fanout: the local write and
// audit that run first eat into it. A fanout budget that always started fresh
// could finish after net/http closed the connection, losing syncFailures. So
// each handler pins one absolute request deadline at entry (withRequestBudget)
// and the fanout takes the earlier of it and now+FanoutTimeout.
func assertFanoutCappedByRequestBudget(t *testing.T, mkHandler func(publish func(context.Context, string, []byte, string) error) *Handler, method, path string, body any, wantStatus int) {
	t.Helper()
	var mu sync.Mutex
	var callDeadline time.Time
	publish := func(ctx context.Context, _ string, _ []byte, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		callDeadline, _ = ctx.Deadline()
		return nil
	}
	h := mkHandler(publish)

	start := time.Now()
	require.Equal(t, wantStatus, doUserRequest(t, h, method, path, body).Code)

	mu.Lock()
	defer mu.Unlock()
	assert.WithinDuration(t, start.Add(requestBudget), callDeadline, 2*time.Second,
		"fanout deadline must be bounded by the request budget measured from handler entry")
	assert.Less(t, requestBudget, httpWriteTimeout, "the request budget must leave room to write the response before net/http's WriteTimeout")
}

// -------------------------------------------------------------------------
// resyncUser — POST /users/:account/resync
// -------------------------------------------------------------------------

// resyncStoredUser is the home-site doc the resync re-delivers.
func resyncStoredUser() *model.User {
	active := true
	return &model.User{
		ID: "id-user1", Account: "user1", SiteID: "site-a",
		EngName: "User One", ChineseName: "User One CN",
		Roles: []model.UserRole{model.UserRoleBot}, Active: &active,
	}
}

func TestHandler_resyncUser(t *testing.T) {
	t.Run("fanout deadline is capped by the whole-request budget, not a fresh FANOUT_TIMEOUT", func(t *testing.T) {
		assertFanoutCappedByRequestBudget(t, func(publish func(context.Context, string, []byte, string) error) *Handler {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "user1").Return(resyncStoredUser(), nil)
			cfg := accountFanoutCfg()
			cfg.FanoutTimeout = time.Hour // far past the request budget: the request budget must win
			return newHandler(m, emptySessionStore(), cfg, nil, publish)
		}, http.MethodPost, "/users/user1/resync", nil, http.StatusOK)
	})

	t.Run("re-fires both lanes from the stored doc, no writes, no audit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		// GetUserByAccount is the only store touch: no CreateUser/UpdateUser,
		// and no AppendAudit — resync is re-delivery, not re-recording.
		m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "user1").Return(resyncStoredUser(), nil)

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users/user1/resync", nil)
		require.Equal(t, http.StatusOK, w.Code)
		body := respBody(t, w)
		assert.Equal(t, "ok", body["status"])
		assertNoFanoutFields(t, body)

		require.Len(t, pub.calls, 3, "1 HR bootstrap + 1 snapshot per remote site")

		subj, encoding, data, found := hrCall(&pub)
		require.True(t, found, "resync must re-publish the durable HR identity bootstrap")
		assert.Equal(t, subject.OrgSyncUsersUpsert("site-a"), subj)
		assert.Equal(t, natsutil.EncodingZstd, encoding)
		users, _ := decodeHRPayload(t, data)
		require.Len(t, users, 1)
		assert.Equal(t, "id-user1", users[0].ID)
		assert.Equal(t, "user1", users[0].Account)

		for dest, snap := range assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"}) {
			assert.Equal(t, "id-user1", snap.ID, dest)
			assert.Equal(t, "user1", snap.Account, dest)
			assert.Equal(t, "site-a", snap.SiteID, dest)
			assert.Equal(t, "User One", snap.EngName, dest)
			assert.Equal(t, []model.UserRole{model.UserRoleBot}, snap.Roles, dest)
			assert.True(t, snap.Active, dest)
		}
	})

	t.Run("unknown or foreign account → 404 user_not_found, nothing published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "ghost").Return(nil, ErrUserNotFound)

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users/ghost/resync", nil)
		require.Equal(t, http.StatusNotFound, w.Code)
		assert.Equal(t, string(errcode.AdminUserNotFound), respBody(t, w)["reason"])
		assert.Empty(t, pub.calls)
	})

	t.Run("store error → 500, nothing published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "user1").Return(nil, errors.New("mongo down"))

		var pub fanoutCapture
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users/user1/resync", nil)
		require.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Empty(t, pub.calls)
	})

	t.Run("one INBOX lane fails → syncFailures names it, status stays ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "user1").Return(resyncStoredUser(), nil)

		pub := fanoutCapture{failSubjPrefix: "chat.inbox.site-c."}
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users/user1/resync", nil)
		require.Equal(t, http.StatusOK, w.Code)
		body := respBody(t, w)
		assert.Equal(t, "ok", body["status"])
		assert.Equal(t, []any{"site-c"}, body["syncFailures"])
		_, hasHR := body["hrSyncFailed"]
		assert.False(t, hasHR)
	})

	t.Run("HR publish fails → hrSyncFailed true, snapshots still land", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "user1").Return(resyncStoredUser(), nil)

		pub := fanoutCapture{failSubjPrefix: hrSubjPrefix}
		h := newHandler(m, emptySessionStore(), accountFanoutCfg(), nil, pub.publish)

		w := doUserRequest(t, h, http.MethodPost, "/users/user1/resync", nil)
		require.Equal(t, http.StatusOK, w.Code)
		body := respBody(t, w)
		assert.Equal(t, true, body["hrSyncFailed"])
		assertSnapshotLanes(t, &pub, []string{"site-b", "site-c"})
	})
}
