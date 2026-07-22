package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/bcrypt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

// bcryptOf returns the legacy Rocket.Chat bcrypt(sha256_hex(plaintext)) hash
// at the cheapest cost so tests stay fast.
func bcryptOf(t *testing.T, plaintext string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(plaintext))
	h, err := bcrypt.GenerateFromPassword([]byte(hex.EncodeToString(sum[:])), bcrypt.MinCost)
	require.NoError(t, err)
	return string(h)
}

// fixedTokenGen returns the same raw token on every call — lets tests assert
// on exact session IDs and authToken values.
func fixedTokenGen(raw string) func() (string, error) {
	return func() (string, error) { return raw, nil }
}

// stubClock returns a fixed unix-ms timestamp.
func stubClock(ms int64) func() int64 { return func() int64 { return ms } }

// testHandlerConfig is the config used by handler unit tests. BcryptCost is
// MinCost so per-test bcrypt-compare against the dummy hash stays fast.
func testHandlerConfig() *config {
	return &config{
		SiteID:                "site-a",
		SessionsMaxPerAccount: 100,
		BcryptCost:            bcrypt.MinCost,
	}
}

// newTestRouter builds a router with a fresh mock store. The returned handler
// is exposed so tests can assign tokenGen/now field-by-field when they need
// determinism — same-package field access is the test seam.
func newTestRouter(t *testing.T) (*gin.Engine, *MockBotplatformStore, *handler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	st := NewMockBotplatformStore(ctrl)
	h := newHandler(st, testHandlerConfig())
	r := gin.New()
	registerRoutes(r, h)
	return r, st, h
}

// botUser builds a bot-role user fixture with a freshly bcrypted password.
func botUser(t *testing.T, id, account, site, password string) *model.User {
	t.Helper()
	return &model.User{
		ID: id, Account: account, SiteID: site,
		Roles:    []model.UserRole{model.UserRoleBot},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, password)}},
	}
}

func post(t *testing.T, r http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHandleHealth(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().Ping(gomock.Any()).Return(nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleHealth_MongoDown(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().Ping(gomock.Any()).Return(errors.New("mongo down"))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestHandleLogin_Bot_HappyPath(t *testing.T) {
	rawToken := strings.Repeat("a", 43)
	r, st, h := newTestRouter(t)
	h.tokenGen = fixedTokenGen(rawToken)
	h.now = stubClock(1_700_000_000_000)
	u := botUser(t, "abcdef1234567890x", "name.shortcode.bot", "site-a", "secret")
	st.EXPECT().FindUserByAccount(gomock.Any(), "name.shortcode.bot").Return(u, nil)
	st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, s *session.Session) error {
			assert.Equal(t, sessiontoken.Hash(rawToken), s.ID)
			assert.Equal(t, u.ID, s.UserID)
			assert.Equal(t, u.Account, s.Account)
			assert.Equal(t, u.SiteID, s.SiteID)
			assert.Equal(t, []string{"bot"}, s.Roles)
			assert.Equal(t, int64(1_700_000_000_000), s.IssuedAt)
			return nil
		})
	st.EXPECT().DeleteSessionsBeyondCap(gomock.Any(), u.Account, 100).Return(int64(0), nil)

	w := post(t, r, "/api/v1/login", map[string]string{"username": "name.shortcode.bot", "password": "secret"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp loginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "success", resp.Status)
	assert.Equal(t, u.ID, resp.Data.UserID)
	assert.Equal(t, rawToken, resp.Data.AuthToken)
	assert.Equal(t, u.ID, resp.Data.Me.ID)
	assert.Equal(t, u.Account, resp.Data.Me.Username)
	assert.Equal(t, []string{"bot"}, resp.Data.Me.Roles)
	assert.False(t, resp.Data.Me.RequirePasswordChange)
}

func TestHandleLogin_Admin_HappyPath(t *testing.T) {
	rawToken := strings.Repeat("b", 43)
	r, st, h := newTestRouter(t)
	h.tokenGen = fixedTokenGen(rawToken)
	h.now = stubClock(1_700_000_000_000)
	u := &model.User{
		ID: "admin0000000000ab", Account: "p_admin", SiteID: "site-a",
		Roles:                 []model.UserRole{model.UserRoleAdmin},
		Services:              model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "adminpass")}},
		RequirePasswordChange: true,
	}
	st.EXPECT().FindUserByAccount(gomock.Any(), "p_admin").Return(u, nil)
	st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).Return(nil)
	st.EXPECT().DeleteSessionsBeyondCap(gomock.Any(), u.Account, 100).Return(int64(0), nil)

	w := post(t, r, "/api/v1/login", map[string]string{"username": "p_admin", "password": "adminpass"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp loginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, []string{"admin"}, resp.Data.Me.Roles)
	assert.True(t, resp.Data.Me.RequirePasswordChange, "first-login flag should pass through")
}

func TestHandleLogin_ApiV1Path(t *testing.T) {
	// /api/v1/login is the sole login route (the legacy /v1/login route was
	// dropped as part of the /api/v1 migration).
	rawToken := strings.Repeat("c", 43)
	r, st, h := newTestRouter(t)
	h.tokenGen = fixedTokenGen(rawToken)
	h.now = stubClock(1)
	u := botUser(t, "user000000000000a", "x.y.bot", "site-a", "p")
	st.EXPECT().FindUserByAccount(gomock.Any(), "x.y.bot").Return(u, nil)
	st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).Return(nil)
	st.EXPECT().DeleteSessionsBeyondCap(gomock.Any(), u.Account, 100).Return(int64(0), nil)

	w := post(t, r, "/api/v1/login", map[string]string{"username": "x.y.bot", "password": "p"})
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleLogin_400_EmptyUsername(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := post(t, r, "/api/v1/login", map[string]string{"username": "", "password": "secret"})
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleLogin_400_EmptyPassword(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := post(t, r, "/api/v1/login", map[string]string{"username": "x.bot", "password": ""})
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleLogin_401_UnknownAccount(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().FindUserByAccount(gomock.Any(), "ghost.bot").Return(nil, mongo.ErrNoDocuments)
	w := post(t, r, "/api/v1/login", map[string]string{"username": "ghost.bot", "password": "secret"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	// Uniform body — must not reveal which arm took the path.
	assert.Contains(t, w.Body.String(), "invalid_credentials")
}

func TestHandleLogin_401_WrongPassword(t *testing.T) {
	r, st, _ := newTestRouter(t)
	u := &model.User{
		ID: "abc", Account: "x.bot", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleBot},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "right")}},
	}
	st.EXPECT().FindUserByAccount(gomock.Any(), "x.bot").Return(u, nil)
	w := post(t, r, "/api/v1/login", map[string]string{"username": "x.bot", "password": "wrong"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_credentials")
}

func TestHandleLogin_401_RoleGate_RegularUser(t *testing.T) {
	// User exists, password is correct, BUT roles don't include bot/admin.
	// Uniform 401, no session created.
	r, st, _ := newTestRouter(t)
	u := &model.User{
		ID: "u1", Account: "alice", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleUser},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "secret")}},
	}
	st.EXPECT().FindUserByAccount(gomock.Any(), "alice").Return(u, nil)
	// No InsertSession / CountSessions expectations — must be rejected before.

	w := post(t, r, "/api/v1/login", map[string]string{"username": "alice", "password": "secret"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_credentials")
}

func TestHandleLogin_500_MongoFindError(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().FindUserByAccount(gomock.Any(), "x.bot").Return(nil, errors.New("mongo dead"))
	w := post(t, r, "/api/v1/login", map[string]string{"username": "x.bot", "password": "p"})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleLogin_500_MongoInsertError(t *testing.T) {
	r, st, _ := newTestRouter(t)
	u := &model.User{
		ID: "u1", Account: "x.bot", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleBot},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "p")}},
	}
	st.EXPECT().FindUserByAccount(gomock.Any(), "x.bot").Return(u, nil)
	st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).Return(errors.New("dup key"))
	w := post(t, r, "/api/v1/login", map[string]string{"username": "x.bot", "password": "p"})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleLogin_CapEviction(t *testing.T) {
	// After the InsertSession write, the handler asks the store to drop
	// every session row beyond the cap. The store returns the count of
	// rows it deleted; the handler treats this as best-effort and only
	// logs on error. We assert the handler always calls
	// DeleteSessionsBeyondCap with the configured cap and never crashes
	// for the over-cap return values it might see.
	tests := []struct {
		name       string
		cap        int
		evictedRet int64 // what the store reports back; 0 = was under cap
	}{
		{"below cap — store deletes 0", 100, 0},
		{"just over cap — store deletes 1", 100, 1},
		{"way over cap — store deletes many", 3, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			st := NewMockBotplatformStore(ctrl)
			cfg := testHandlerConfig()
			cfg.SessionsMaxPerAccount = tc.cap
			h := newHandler(st, cfg)
			h.tokenGen = fixedTokenGen(strings.Repeat("z", 43))
			h.now = stubClock(1)
			r := gin.New()
			registerRoutes(r, h)

			u := botUser(t, "u1", "x.bot", "site-a", "p")
			st.EXPECT().FindUserByAccount(gomock.Any(), "x.bot").Return(u, nil)
			st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).Return(nil)
			st.EXPECT().DeleteSessionsBeyondCap(gomock.Any(), u.Account, tc.cap).Return(tc.evictedRet, nil)

			w := post(t, r, "/api/v1/login", map[string]string{"username": "x.bot", "password": "p"})
			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestHandleValidate_HappyPath(t *testing.T) {
	r, st, _ := newTestRouter(t)
	rawToken := strings.Repeat("t", 43)
	hash := sessiontoken.Hash(rawToken)
	st.EXPECT().FindSessionByHash(gomock.Any(), hash).Return(&session.Session{
		ID:       hash,
		UserID:   "u1",
		Account:  "x.bot",
		SiteID:   "site-a",
		Roles:    []string{"bot"},
		IssuedAt: 1,
	}, nil)
	w := post(t, r, "/api/v1/auth/validate", map[string]string{"authToken": rawToken})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var resp validateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.Valid)
	assert.Equal(t, "u1", resp.Principal.UserID)
	assert.Equal(t, "x.bot", resp.Principal.Account)
	assert.Equal(t, "site-a", resp.Principal.SiteID)
	assert.Equal(t, []string{"bot"}, resp.Principal.Roles)
}

func TestHandleValidate_UnknownToken(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().FindSessionByHash(gomock.Any(), gomock.Any()).Return(nil, session.ErrNotFound)
	w := post(t, r, "/api/v1/auth/validate", map[string]string{"authToken": "nope"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_token")
}

func TestHandleValidate_EmptyToken(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := post(t, r, "/api/v1/auth/validate", map[string]string{"authToken": ""})
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleValidate_MongoError(t *testing.T) {
	r, st, _ := newTestRouter(t)
	st.EXPECT().FindSessionByHash(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo dead"))
	w := post(t, r, "/api/v1/auth/validate", map[string]string{"authToken": "x"})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// Generated tokens must be 43-char base64url (no padding) — wire-compat with
// legacy Rocket.Chat opaque tokens, no bp_ prefix.
func TestGenerateToken_Format(t *testing.T) {
	for range 50 {
		tok, err := sessiontoken.New()
		require.NoError(t, err)
		assert.Len(t, tok, 43, "token must be 43 chars")
		assert.False(t, strings.HasPrefix(tok, "bp_"), "must NOT use bp_ prefix")
		_, err = base64.RawURLEncoding.DecodeString(tok)
		assert.NoError(t, err, "must be base64url")
	}
}

func TestGenerateToken_Unique(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		tok, err := sessiontoken.New()
		require.NoError(t, err)
		require.False(t, seen[tok], "generated duplicate token within 200 calls: %s", tok)
		seen[tok] = true
	}
}

// Uniform-timing guard: unknown account, wrong password, and not-role-eligible
// should all return the same wire body modulo dynamic fields. The handler is
// expected to run a bcrypt-compare against a dummy hash on the unknown-account
// arm to flatten timing — assert the body shape is identical so we don't
// silently break the uniformity invariant.
func TestHandleLogin_UniformErrorBody_NoEnumeration(t *testing.T) {
	cases := []struct {
		name      string
		setupMock func(st *MockBotplatformStore)
		username  string
		password  string
	}{
		{
			name: "unknown account",
			setupMock: func(st *MockBotplatformStore) {
				st.EXPECT().FindUserByAccount(gomock.Any(), "ghost.bot").
					Return(nil, mongo.ErrNoDocuments)
			},
			username: "ghost.bot", password: "secret",
		},
		{
			name: "wrong password",
			setupMock: func(st *MockBotplatformStore) {
				st.EXPECT().FindUserByAccount(gomock.Any(), "x.bot").
					Return(&model.User{
						ID: "u1", Account: "x.bot", SiteID: "site-a",
						Roles:    []model.UserRole{model.UserRoleBot},
						Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "right")}},
					}, nil)
			},
			username: "x.bot", password: "wrong",
		},
		{
			name: "role gate fail",
			setupMock: func(st *MockBotplatformStore) {
				st.EXPECT().FindUserByAccount(gomock.Any(), "alice").
					Return(&model.User{
						ID: "u1", Account: "alice", SiteID: "site-a",
						Roles:    []model.UserRole{model.UserRoleUser},
						Services: model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "secret")}},
					}, nil)
			},
			username: "alice", password: "secret",
		},
		{
			name: "deactivated account",
			setupMock: func(st *MockBotplatformStore) {
				u := &model.User{
					ID: "u2", Account: "deact.uniform.bot", SiteID: "site-a",
					Roles:       []model.UserRole{model.UserRoleBot},
					Services:    model.Services{Password: model.PasswordCredentials{Bcrypt: bcryptOf(t, "correct")}},
					Deactivated: true,
				}
				st.EXPECT().FindUserByAccount(gomock.Any(), "deact.uniform.bot").Return(u, nil)
			},
			username: "deact.uniform.bot", password: "correct",
		},
	}
	bodies := make(map[string]string, len(cases))
	for _, tc := range cases {
		r, st, _ := newTestRouter(t)
		tc.setupMock(st)
		w := post(t, r, "/api/v1/login", map[string]string{"username": tc.username, "password": tc.password})
		assert.Equal(t, http.StatusUnauthorized, w.Code, tc.name)
		bodies[tc.name] = w.Body.String()
	}
	first := ""
	for name, b := range bodies {
		// Strip request_id which varies per request.
		stripped := stripRequestID(b)
		if first == "" {
			first = stripped
		} else {
			assert.Equal(t, first, stripped,
				"body for %q differs from first — wire-level enumeration leak", name)
		}
	}
}

// stripRequestID removes the request_id field (a UUID that varies per request)
// from a JSON error body so equality assertions can compare the rest.
func stripRequestID(body string) string {
	var generic map[string]any
	if err := json.Unmarshal([]byte(body), &generic); err != nil {
		return body
	}
	delete(generic, "requestId")
	delete(generic, "request_id")
	out, _ := json.Marshal(generic)
	return string(out)
}

// Compile-time guard that the bcryptOf test helper actually produces a hash
// that bcrypt.CompareHashAndPassword accepts. Catches a stale fixture if the
// legacy sha256-hex preprocessing changes.
func TestBcryptOf_RoundTrip(t *testing.T) {
	stored := bcryptOf(t, "secret")
	sum := sha256.Sum256([]byte("secret"))
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(stored), []byte(hex.EncodeToString(sum[:]))))
}

// Sanity: make sure session hashing matches what FindSessionByHash will look
// for. If anyone changes the recipe, this catches the divergence at unit-test
// time rather than waiting for an integration failure.
func TestSessionHash_Stable(t *testing.T) {
	want := sessiontoken.Hash("alpha")
	assert.Equal(t, want, sessiontoken.Hash("alpha"))
	assert.NotEqual(t, want, sessiontoken.Hash("beta"))
	// Hash must be the same 44-char base64 std encoding the production code uses.
	assert.Len(t, want, 44)
	// Ensure it's parseable as std base64.
	_, err := base64.StdEncoding.DecodeString(want)
	assert.NoError(t, err)
}

// TestHandleLogin_DeactivatedAccountRejected verifies that a deactivated
// account with otherwise-valid credentials (correct role, site, and password)
// is rejected with the same uniform 401 invalid_credentials as wrong-password /
// unknown-account, and that no session is inserted. It also verifies that an
// active (non-deactivated) account's login response carries me.active == true.
func TestHandleLogin_DeactivatedAccountRejected(t *testing.T) {
	t.Run("deactivated account returns uniform 401, no session", func(t *testing.T) {
		r, st, _ := newTestRouter(t)
		u := botUser(t, "deact0000000000ab", "deact.bot", "site-a", "correct")
		u.Deactivated = true
		st.EXPECT().FindUserByAccount(gomock.Any(), "deact.bot").Return(u, nil)
		// No InsertSession call expected — must be rejected before session creation.

		w := post(t, r, "/api/v1/login", map[string]string{"username": "deact.bot", "password": "correct"})
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Contains(t, w.Body.String(), "invalid_credentials")
	})

	t.Run("active account me.active is true", func(t *testing.T) {
		rawToken := strings.Repeat("m", 43)
		r, st, h := newTestRouter(t)
		h.tokenGen = fixedTokenGen(rawToken)
		h.now = stubClock(1)
		u := botUser(t, "active000000000ab", "active.bot", "site-a", "correct")
		// Deactivated defaults to false — active account.
		st.EXPECT().FindUserByAccount(gomock.Any(), "active.bot").Return(u, nil)
		st.EXPECT().InsertSession(gomock.Any(), gomock.Any()).Return(nil)
		st.EXPECT().DeleteSessionsBeyondCap(gomock.Any(), u.Account, 100).Return(int64(0), nil)

		w := post(t, r, "/api/v1/login", map[string]string{"username": "active.bot", "password": "correct"})
		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

		var resp loginResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.True(t, resp.Data.Me.Active, "me.active must be true for a non-deactivated account")
	})
}
