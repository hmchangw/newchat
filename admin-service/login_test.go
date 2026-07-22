package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

func loginRouter(t *testing.T, adminStore AdminStore, sessions session.Store, cfg Config) *gin.Engine { //nolint:gocritic // hugeParam: Config is a test-local value, cheap to copy per call
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := newHandler(adminStore, sessions, cfg)
	r.POST("/v1/login", h.handleLogin)
	return r
}

func mustHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := pwhash.Hash(pw, 4) // low cost for tests
	require.NoError(t, err)
	return h
}

func postJSON(t *testing.T, r *gin.Engine, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHandleLogin_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID:      "u1",
		Account: "p_alice",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "correct-horse"),
		}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)

	inserted := false
	sessions := &fakeSessionStore{
		InsertFn: func(_ context.Context, s *session.Session) error {
			inserted = true
			assert.Equal(t, "u1", s.UserID)
			assert.Equal(t, "p_alice", s.Account)
			assert.Contains(t, s.Roles, string(model.UserRoleAdmin))
			return nil
		},
		DeleteBeyondCapFn: func(_ context.Context, account string, _ int) (int64, error) {
			assert.Equal(t, "p_alice", account)
			return 0, nil
		},
	}

	r := loginRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "correct-horse"})

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, inserted)
	var body struct {
		AuthToken             string `json:"authToken"`
		Account               string `json:"account"`
		SiteID                string `json:"siteId"`
		RequirePasswordChange bool   `json:"requirePasswordChange"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotEmpty(t, body.AuthToken)
	assert.Equal(t, "p_alice", body.Account)
	assert.Equal(t, "site-a", body.SiteID)
	assert.False(t, body.RequirePasswordChange)
}

func TestHandleLogin_InvalidCredentials_Cases(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (AdminStore, session.Store)
		body  map[string]string
	}{
		{
			name: "user not found",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "ghost").Return(nil, ErrUserNotFound)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "ghost", "password": "x"},
		},
		{
			name: "not admin (bot with correct password)",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "bob.bot").Return(&model.User{
					ID: "u2", Account: "bob.bot", SiteID: "site-a",
					Roles:    []model.UserRole{model.UserRoleBot},
					Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "hunter2")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "bob.bot", "password": "hunter2"},
		},
		{
			name: "wrong password",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(&model.User{
					ID: "u1", Account: "p_alice", SiteID: "site-a",
					Roles:    []model.UserRole{model.UserRoleAdmin},
					Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "right")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "p_alice", "password": "wrong"},
		},
		{
			name: "deactivated admin",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(&model.User{
					ID: "u1", Account: "p_alice", SiteID: "site-a",
					Roles:       []model.UserRole{model.UserRoleAdmin},
					Deactivated: true,
					Services:    model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "right")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "p_alice", "password": "right"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, sess := tc.setup(t)
			r := loginRouter(t, st, sess, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
			w := postJSON(t, r, "/v1/login", tc.body)
			require.Equal(t, http.StatusUnauthorized, w.Code)
			var env struct {
				Code   string `json:"code"`
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
			assert.Equal(t, string(errcode.CodeUnauthenticated), env.Code)
			assert.Equal(t, string(errcode.AdminInvalidCredentials), env.Reason)
		})
	}
}

func TestHandleLogin_BadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	r := loginRouter(t, st, &fakeSessionStore{}, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})

	w := postJSON(t, r, "/v1/login", map[string]string{"username": ""})
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleLogin_GetUserError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	// GetUserForAuth returns a non-ErrUserNotFound error (e.g., MongoDB down)
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(nil, errors.New("mongo dead"))

	sessions := &fakeSessionStore{
		// Should never be called
		InsertFn: func(_ context.Context, _ *session.Session) error {
			assert.Fail(t, "Insert should not be called on GetUserForAuth error")
			return nil
		},
	}

	r := loginRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "anything"})

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.CodeInternal), env.Code)
}

func TestHandleLogin_SessionInsertError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID:      "u1",
		Account: "p_alice",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "correct-horse"),
		}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)

	sessions := &fakeSessionStore{
		InsertFn: func(_ context.Context, _ *session.Session) error {
			// Simulate a session store error (e.g., duplicate key, connection lost)
			return errors.New("dup key or connection lost")
		},
	}

	r := loginRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "correct-horse"})

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.CodeInternal), env.Code)
}

func TestHandleLogin_DeleteBeyondCapError_StillReturns200(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID:      "u1",
		Account: "p_alice",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "correct-horse"),
		}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)

	inserted := false
	sessions := &fakeSessionStore{
		InsertFn: func(_ context.Context, _ *session.Session) error {
			inserted = true
			return nil
		},
		DeleteBeyondCapFn: func(_ context.Context, userID string, _ int) (int64, error) {
			// DeleteBeyondCap fails but the handler should still return 200 and log the error
			return 0, errors.New("mongo dead during eviction")
		},
	}

	r := loginRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "correct-horse"})

	// Critical: despite DeleteBeyondCap error, login succeeds with 200 (best-effort eviction)
	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, inserted, "session Insert should have succeeded")

	var body struct {
		AuthToken             string `json:"authToken"`
		Account               string `json:"account"`
		SiteID                string `json:"siteId"`
		RequirePasswordChange bool   `json:"requirePasswordChange"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotEmpty(t, body.AuthToken)
	assert.Equal(t, "p_alice", body.Account)
}

// TestHandleLogin_TimingConsistent proves the dummy-bcrypt guard (Fix 1) is
// actually running on the user-not-found and not-admin denial arms: it times
// all three denial paths — unknown user, bot with correct password (role
// gate fails), admin with wrong password (real bcrypt compare fails) — and
// asserts they land within a lenient factor of each other. Without the dummy
// bcrypt, the first two arms would return near-instantly while the third
// burns a full bcrypt-cost-10 compare, leaking "this account exists and is
// an admin" via latency. Skipped under -short: bcrypt cost 10 is slow by
// design (matches the prod default).
func TestHandleLogin_TimingConsistent(t *testing.T) {
	if testing.Short() {
		t.Skip("bcrypt cost 10 timing test is slow; skipped under -short")
	}

	// Matches dummyBcrypt's cost in login.go so all three arms do comparable
	// bcrypt work.
	const timingCost = 10

	measure := func(t *testing.T, st AdminStore, sess session.Store, body map[string]string) time.Duration {
		t.Helper()
		r := loginRouter(t, st, sess, Config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: timingCost})
		start := time.Now()
		w := postJSON(t, r, "/v1/login", body)
		elapsed := time.Since(start)
		require.Equal(t, http.StatusUnauthorized, w.Code)
		return elapsed
	}

	var durations []time.Duration

	t.Run("unknown user", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		st := NewMockAdminStore(ctrl)
		st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "ghost").Return(nil, ErrUserNotFound)

		d := measure(t, st, &fakeSessionStore{}, map[string]string{"username": "ghost", "password": "whatever-password"})
		durations = append(durations, d)
	})

	t.Run("bot with correct password", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		st := NewMockAdminStore(ctrl)
		hash, err := pwhash.Hash("hunter2", timingCost)
		require.NoError(t, err)
		st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "bob.bot").Return(&model.User{
			ID: "u2", Account: "bob.bot", SiteID: "site-a",
			Roles:    []model.UserRole{model.UserRoleBot},
			Services: model.Services{Password: model.PasswordCredentials{Bcrypt: hash}},
		}, nil)

		d := measure(t, st, &fakeSessionStore{}, map[string]string{"username": "bob.bot", "password": "hunter2"})
		durations = append(durations, d)
	})

	t.Run("admin with wrong password", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		st := NewMockAdminStore(ctrl)
		hash, err := pwhash.Hash("right-password", timingCost)
		require.NoError(t, err)
		st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(&model.User{
			ID: "u1", Account: "p_alice", SiteID: "site-a",
			Roles:    []model.UserRole{model.UserRoleAdmin},
			Services: model.Services{Password: model.PasswordCredentials{Bcrypt: hash}},
		}, nil)

		d := measure(t, st, &fakeSessionStore{}, map[string]string{"username": "p_alice", "password": "wrong-password"})
		durations = append(durations, d)
	})

	require.Len(t, durations, 3)
	minD, maxD := durations[0], durations[0]
	for _, d := range durations[1:] {
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	assert.LessOrEqual(t, float64(maxD)/float64(minD), 3.0,
		"timing spread too large: durations=%v — dummy bcrypt may not be running on a denial path", durations)
}

func changePasswordRouter(t *testing.T, adminStore AdminStore, sessions session.Store, cfg Config) *gin.Engine { //nolint:gocritic // hugeParam: Config is a test-local value, cheap to copy per call
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := newHandler(adminStore, sessions, cfg)
	r.POST("/v1/password/change", requireAdmin(sessions, cfg.SiteID), h.handleChangePassword)
	return r
}

// authFor returns a Bearer header for a caller-session already installed in
// `sessions` via FindByHashFn.
func authFor(sess *session.Session, sessions *fakeSessionStore) (authHeader, sessionID string) {
	raw := "raw-token"
	hash := sessiontoken.Hash(raw)
	sess.ID = hash
	sessions.FindByHashFn = func(_ context.Context, h string) (*session.Session, error) {
		if h != hash {
			return nil, session.ErrNotFound
		}
		return sess, nil
	}
	return "Bearer " + raw, hash
}

func TestHandleChangePassword_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles: []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "old-pw"),
		}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)
	st.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, callerID := authFor(caller, sessions)

	// The password write and sibling-session revoke are one atomic store call
	// now — assert the caller's own session id is passed as exceptSessionID
	// so they stay logged in.
	st.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-a", "p_alice", gomock.Any(), false, callerID).Return(nil)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: 4})
	body := map[string]string{"oldPassword": "old-pw", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleChangePassword_OldPasswordMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(&model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "old-pw")}},
	}, nil)
	// no UpdateUserPasswordAndRevoke expectation — must not fire

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "WRONG", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	var env struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.AdminOldPasswordMismatch), env.Reason)
}

func TestHandleChangePassword_MissingBearer(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	r := changePasswordRouter(t, st, &fakeSessionStore{}, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "x", "newPassword": "y"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleChangePassword_MissingFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	// GetUserForAuth must never be reached — validation happens first.

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.AuthMissingFields), env.Reason)
}

func TestHandleChangePassword_GetUserError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(nil, errors.New("mongo dead"))

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "old-pw", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestHandleChangePassword_UpdatePasswordAndRevokeError_Returns500 covers the
// atomic transaction's failure path: password write and session revoke now
// run as one Mongo transaction inside UpdateUserPasswordAndRevoke, so a
// failure there means nothing was applied — the handler surfaces it as a
// 500, and AppendAudit must not fire (the transaction, not a best-effort
// side call, owns both writes now).
func TestHandleChangePassword_UpdatePasswordAndRevokeError_Returns500(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	user := &model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "old-pw")}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)
	st.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-a", "p_alice", gomock.Any(), false, gomock.Any()).
		Return(errors.New("mongo dead"))
	// no AppendAudit expectation — must not fire

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "old-pw", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleChangePassword_AppendAuditError_StillReturns204(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	user := &model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "old-pw")}},
	}
	st.EXPECT().GetUserForAuth(gomock.Any(), "site-a", "p_alice").Return(user, nil)
	st.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-a", "p_alice", gomock.Any(), false, gomock.Any()).Return(nil)
	st.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(errors.New("mongo dead during audit"))

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "old-pw", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code, "audit failure must not fail the handler (best-effort)")
}
