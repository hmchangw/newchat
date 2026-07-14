package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
	"github.com/hmchangw/chat/pkg/model"
)

func setupRouter(t *testing.T, h *PortalHandler) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Minimal request-id propagation so the handler sees the inbound
	// X-Request-ID under the same key the production ginutil.RequestID
	// middleware uses. The login forwarder reads it via c.GetString.
	r.Use(func(c *gin.Context) {
		if id := c.GetHeader("X-Request-ID"); id != "" {
			c.Set("request_id", id)
		}
		c.Next()
	})
	registerRoutes(r, h)
	return r
}

// getUserInfo issues GET /api/userInfo with account as a query parameter. An
// empty account is sent with no query parameter at all (the missing-param case).
func getUserInfo(t *testing.T, r *gin.Engine, account string) *httptest.ResponseRecorder {
	t.Helper()
	target := "/api/userInfo"
	if account != "" {
		target += "?account=" + url.QueryEscape(account)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

func getPath(t *testing.T, r *gin.Engine, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

// cacheWith returns a directory cache populated with the given entries.
func cacheWith(emps ...employee) *directoryCache {
	c := newDirectoryCache()
	c.replace(emps)
	return c
}

// testSites is the per-site URL registry used by the handler tests, with a
// distinct domain per site to prove URLs are looked up, not templated.
var testSites = map[string]siteURL{
	"site-a":     {BaseURL: "https://site-a.example.com", NATSURL: "wss://nats-3.site-a.example.com"},
	"site-b":     {BaseURL: "https://site-b.example.com", NATSURL: "wss://nats.site-b.example.com"},
	"site-local": {BaseURL: "http://localhost:3000", NATSURL: "ws://localhost:9222"},
}

// testSettings is the settings payload used by the handler tests.
var testSettings = settingsResponse{
	APIVersion:  "v2",
	OTELBaseURL: "https://otel.example.com/v1",
}

// newTestHandler builds a PortalHandler with the test site registry and the
// local dev-fallback coordinates.
func newTestHandler(cache *directoryCache, devMode bool) *PortalHandler {
	return NewPortalHandler(cache, devMode, "site-local", "ws://localhost:9222", testSites, testSettings)
}

func TestHandleUserInfo_HappyPath(t *testing.T) {
	h := newTestHandler(cacheWith(alice), false)
	w := getUserInfo(t, setupRouter(t, h), "alice")

	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, userInfoResponse{
		Account:    "alice",
		EmployeeID: "E001",
		BaseURL:    "https://site-a.example.com",
		NATSURL:    "wss://nats-3.site-a.example.com",
		SiteID:     "site-a",
	}, resp)
}

func TestHandleUserInfo_PerSiteURLs(t *testing.T) {
	t.Run("each site resolves its own base URL from the registry", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice, bob), false)
		r := setupRouter(t, h)

		var alice userInfoResponse
		require.NoError(t, json.Unmarshal(getUserInfo(t, r, "alice").Body.Bytes(), &alice))
		assert.Equal(t, "https://site-a.example.com", alice.BaseURL)

		var bob userInfoResponse
		require.NoError(t, json.Unmarshal(getUserInfo(t, r, "bob").Body.Bytes(), &bob))
		assert.Equal(t, "https://site-b.example.com", bob.BaseURL)
	})

	t.Run("a site missing from the registry is a server error, not a client error", func(t *testing.T) {
		orphan := employee{Account: "carol", EmployeeID: "E003", SiteID: "site-unknown"}
		h := newTestHandler(cacheWith(orphan), false)
		w := getUserInfo(t, setupRouter(t, h), "carol")
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeInternal)
	})
}

func TestHandleUserInfo_InvalidAccountFormat(t *testing.T) {
	// Same token invariant auth-service enforces at minting — refusing here
	// keeps the portal from blessing an account the next step must reject.
	for _, tt := range []struct{ name, account string }{
		{"dotted account refused", "john.doe"},
		{"wildcard account refused", "mal*ory"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(cacheWith(alice), false)
			w := getUserInfo(t, setupRouter(t, h), tt.account)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
	}
}

func TestHandleUserInfo_MissingAccount(t *testing.T) {
	h := newTestHandler(cacheWith(alice), false)
	w := getUserInfo(t, setupRouter(t, h), "")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthMissingFields)
}

func TestHandleUserInfo_AccountNotReady(t *testing.T) {
	t.Run("account absent from a populated cache", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), false)
		w := getUserInfo(t, setupRouter(t, h), "mallory")

		assert.Equal(t, http.StatusForbidden, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeForbidden)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.PortalAccountNotReady)
	})

	t.Run("cache not yet loaded", func(t *testing.T) {
		h := newTestHandler(newDirectoryCache(), false)
		w := getUserInfo(t, setupRouter(t, h), "alice")

		assert.Equal(t, http.StatusForbidden, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeForbidden)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.PortalAccountNotReady)
	})
}

func TestHandleUserInfo_DevMode(t *testing.T) {
	t.Run("known account resolves normally", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), true)
		w := getUserInfo(t, setupRouter(t, h), "alice")
		require.Equal(t, http.StatusOK, w.Code)
		var resp userInfoResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "site-a", resp.SiteID)
	})

	t.Run("unknown account falls back to the dev site", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), true)
		w := getUserInfo(t, setupRouter(t, h), "newdev")
		require.Equal(t, http.StatusOK, w.Code)
		var resp userInfoResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, userInfoResponse{
			Account: "newdev", EmployeeID: "",
			BaseURL: "http://localhost:3000",
			NATSURL: "ws://localhost:9222", SiteID: "site-local",
		}, resp)
	})

	t.Run("fallback works before the cache is loaded", func(t *testing.T) {
		h := newTestHandler(newDirectoryCache(), true)
		w := getUserInfo(t, setupRouter(t, h), "newdev")
		require.Equal(t, http.StatusOK, w.Code)
		var resp userInfoResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "site-local", resp.SiteID)
	})

	t.Run("bad account format is refused even in dev mode", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), true)
		w := getUserInfo(t, setupRouter(t, h), "mal*ory")
		assert.Equal(t, http.StatusBadRequest, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
	})

	t.Run("missing account is bad request", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), true)
		w := getUserInfo(t, setupRouter(t, h), "")
		assert.Equal(t, http.StatusBadRequest, w.Code)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthMissingFields)
	})
}

func TestHandleHealth_AlwaysOK(t *testing.T) {
	// Liveness must not depend on directory data being loaded.
	h := newTestHandler(newDirectoryCache(), false)
	w := getPath(t, setupRouter(t, h), "/healthz")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestHandleReady(t *testing.T) {
	tests := []struct {
		name     string
		cache    *directoryCache
		wantCode int
	}{
		{"cache never loaded", newDirectoryCache(), http.StatusServiceUnavailable},
		{"cache loaded but empty", cacheWith(), http.StatusServiceUnavailable},
		{"cache holds directory data", cacheWith(alice), http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(tt.cache, false)
			w := getPath(t, setupRouter(t, h), "/readyz")
			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// bot is a bot account fixture for role-aware tests. It carries no NATSURL —
// natsUrl is site-level now (see siteURL), which is the whole point of this
// fixture: bot accounts have no hr_employee row and must still resolve a
// working natsUrl from the site registry.
var bot = employee{
	Account: "name.shortcode.bot",
	SiteID:  "site-a",
	UserID:  "userbot0000000001",
	Roles:   []model.UserRole{model.UserRoleBot},
}

// admin is an admin account fixture.
var admin = employee{
	Account: "p_admin",
	SiteID:  "site-a",
	UserID:  "userad0000000001a",
	Roles:   []model.UserRole{model.UserRoleAdmin},
}

// Note: /api/userInfo's existing strict validator rejects dotted accounts
// (see subject.IsValidAccountToken). Bot accounts in production carry dots
// (`name.shortcode.bot`) so they cannot be served through /userInfo — but the
// bot SDK does not call portal anyway, so this is not a real production gap.
// Admin accounts (e.g. `p_admin`) are single-token and pass the validator.

func TestHandleUserInfo_RoleAwareShape_Admin(t *testing.T) {
	h := newTestHandler(cacheWith(admin), false)
	w := getUserInfo(t, setupRouter(t, h), "p_admin")
	require.Equal(t, http.StatusOK, w.Code)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &generic))
	_, hasEmpID := generic["employeeId"]
	assert.False(t, hasEmpID, "admin response should NOT carry employeeId field")
	assert.Equal(t, "p_admin", generic["account"])
	assert.Equal(t, "site-a", generic["siteId"])
	// Regression: admin has no hr_employee row and (pre-fix) NATSURL was
	// sourced per-account from it, so admin/bot accounts got a blank
	// natsUrl. natsUrl is now sourced from the site registry, so admin gets
	// the same non-blank value as any human on site-a (see alice in
	// TestHandleUserInfo_HappyPath).
	assert.Equal(t, "wss://nats-3.site-a.example.com", generic["natsUrl"], "admin must get the site's natsUrl, not a blank one")
}

func TestHandleUserInfo_RoleAwareShape_AdminAndBot(t *testing.T) {
	// Multi-role: either role triggers the minimal shape.
	emp := admin
	emp.Roles = []model.UserRole{model.UserRoleAdmin, model.UserRoleBot}
	h := newTestHandler(cacheWith(emp), false)
	w := getUserInfo(t, setupRouter(t, h), emp.Account)
	require.Equal(t, http.StatusOK, w.Code)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &generic))
	_, hasEmpID := generic["employeeId"]
	assert.False(t, hasEmpID)
}

func TestHandleUserInfo_RoleAwareShape_RegularUser_UnchangedShape(t *testing.T) {
	// Backward compatibility: a regular user (no bot/admin role) still gets
	// the existing rich shape including employeeId.
	h := newTestHandler(cacheWith(alice), false)
	w := getUserInfo(t, setupRouter(t, h), "alice")
	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "E001", resp.EmployeeID, "regular user must still get employeeId")
}

// ----- /api/v1/login forwarder tests --------------------------------------

// upstreamCapture records whether the mock was hit and what the inbound
// request looked like. Path stays "" when the mock was never called.
type upstreamCapture struct {
	calls   int
	path    string
	headers http.Header
	body    []byte
}

// upstreamMock builds an httptest.Server that returns the given status and
// body verbatim, recording the inbound request for assertion. Used to mock
// botplatform's /api/v1/login.
func upstreamMock(t *testing.T, status int, body string, captured *upstreamCapture) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if captured != nil {
			captured.calls++
			captured.path = r.URL.Path
			captured.headers = r.Header.Clone()
			b, _ := io.ReadAll(r.Body)
			captured.body = b
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

// loginHandler builds a PortalHandler whose botplatform Resty client points at
// the per-test upstream URL (botplatform is a single cluster-internal endpoint,
// no longer per site).
func loginHandler(t *testing.T, cache *directoryCache, upstreamURL string) *PortalHandler {
	t.Helper()
	rc := resty.New().SetTimeout(2 * time.Second).SetBaseURL(upstreamURL)
	return NewPortalHandler(cache, false, "site-local", "ws://localhost:9222", testSites, testSettings, WithRestyClient(rc))
}

func postLogin(t *testing.T, r *gin.Engine, body any, headers ...[2]string) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	for _, h := range headers {
		req.Header.Set(h[0], h[1])
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestHandleLogin_CacheMiss_StoreFallback covers an account created since the
// last cache refresh: absent from the in-memory cache, it is found via the live
// directory store and login proceeds — no restart / refresh wait required.
func TestHandleLogin_CacheMiss_StoreFallback(t *testing.T) {
	const upstreamBody = `{"status":"success","data":{"userId":"u9","authToken":"bp-tok","me":{"_id":"u9","roles":["bot"],"requirePasswordChange":false}}}`
	srv := upstreamMock(t, http.StatusOK, upstreamBody, nil)
	defer srv.Close()

	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().GetByAccount(gomock.Any(), "late.bot").
		Return(employee{Account: "late.bot", SiteID: "site-a", Roles: []model.UserRole{model.UserRoleBot}}, true, nil)

	rc := resty.New().SetTimeout(2 * time.Second).SetBaseURL(srv.URL)
	// Empty cache (never loaded) -> Get misses -> store fallback resolves it.
	h := NewPortalHandler(newDirectoryCache(), false, "site-local", "ws://localhost:9222",
		testSites, testSettings, WithRestyClient(rc), WithDirectoryStore(store))
	r := setupRouter(t, h)

	w := postLogin(t, r, loginRequest{Username: "late.bot", Password: "secret"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp portalLoginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "late.bot", resp.Account)
	assert.Equal(t, "site-a", resp.SiteID)
	assert.Equal(t, "bp-tok", resp.AuthToken)
}

// TestHandleLogin_CacheMiss_StoreMissDenies confirms the fallback is not a
// bypass: an account the store also doesn't know still gets 401.
func TestHandleLogin_CacheMiss_StoreMissDenies(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().GetByAccount(gomock.Any(), "ghost").
		Return(employee{}, false, nil)

	rc := resty.New().SetTimeout(2 * time.Second)
	h := NewPortalHandler(newDirectoryCache(), false, "site-local", "ws://localhost:9222",
		testSites, testSettings, WithRestyClient(rc), WithDirectoryStore(store))
	r := setupRouter(t, h)

	w := postLogin(t, r, loginRequest{Username: "ghost", Password: "secret"})
	require.Equal(t, http.StatusUnauthorized, w.Code, "body=%s", w.Body.String())
}

func TestHandleLogin_HappyPath_Bot(t *testing.T) {
	const upstreamBody = `{"status":"success","data":{"userId":"u1","authToken":"bp-tok","me":{"_id":"u1","username":"name.shortcode.bot","name":"","active":true,"roles":["bot"],"requirePasswordChange":false}}}`
	var captured upstreamCapture
	srv := upstreamMock(t, http.StatusOK, upstreamBody, &captured)
	defer srv.Close()

	h := loginHandler(t, cacheWith(bot), srv.URL)
	r := setupRouter(t, h)

	w := postLogin(t, r,
		loginRequest{Username: "name.shortcode.bot", Password: "secret"},
		[2]string{"X-Request-ID", "rid-bot-1"},
	)
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp portalLoginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "u1", resp.UserID)
	assert.Equal(t, "bp-tok", resp.AuthToken)
	assert.Equal(t, "name.shortcode.bot", resp.Account)
	assert.Equal(t, "site-a", resp.SiteID)
	assert.Equal(t, "https://site-a.example.com", resp.BaseURL)
	// Regression: pre-fix, natsUrl came from the bot's (nonexistent)
	// hr_employee row and was blank. It's now sourced from the site
	// registry, so the bot gets the same site-a natsUrl as any human
	// account homed on site-a.
	assert.Equal(t, "wss://nats-3.site-a.example.com", resp.NATSURL, "bot must get the site's natsUrl, not a blank one")
	assert.False(t, resp.RequirePasswordChange)

	// Upstream call must propagate the inbound X-Request-ID.
	assert.Equal(t, 1, captured.calls)
	assert.Equal(t, "/api/v1/login", captured.path)
	assert.Equal(t, "rid-bot-1", captured.headers.Get("X-Request-ID"))
}

func TestHandleLogin_RequirePasswordChange_PassthroughFromUpstream(t *testing.T) {
	// The upstream me.requirePasswordChange flag is the fresh, authoritative
	// value — right after /v1/password/change flips it, a login within the
	// cache-refresh window must still see true.
	const upstreamBody = `{"status":"success","data":{"userId":"u1","authToken":"tok","me":{"requirePasswordChange":true}}}`
	srv := upstreamMock(t, http.StatusOK, upstreamBody, nil)
	defer srv.Close()

	h := loginHandler(t, cacheWith(bot), srv.URL)
	w := postLogin(t, setupRouter(t, h), loginRequest{Username: bot.Account, Password: "p"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp portalLoginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.RequirePasswordChange, "portal must pass requirePasswordChange from the upstream me block")
}

func TestHandleLogin_400_MissingFields(t *testing.T) {
	h := loginHandler(t, cacheWith(bot), "")
	for _, tc := range []struct {
		name string
		body loginRequest
	}{
		{"empty username", loginRequest{Username: "", Password: "p"}},
		{"empty password", loginRequest{Username: "x.bot", Password: ""}},
		{"both empty", loginRequest{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := postLogin(t, setupRouter(t, h), tc.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandleSettings(t *testing.T) {
	t.Run("returns the configured settings with exact field names", func(t *testing.T) {
		h := newTestHandler(cacheWith(alice), false)
		w := getPath(t, setupRouter(t, h), "/api/settings")

		require.Equal(t, http.StatusOK, w.Code)
		assert.JSONEq(t,
			`{"apiVersion":"v2","otelBaseUrl":"https://otel.example.com/v1"}`,
			w.Body.String())
		// Deployment config must not be cached by intermediaries.
		assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	})

	t.Run("serves before the directory cache is loaded", func(t *testing.T) {
		// Static config — must not depend on the directory refresh.
		h := newTestHandler(newDirectoryCache(), false)
		w := getPath(t, setupRouter(t, h), "/api/settings")

		require.Equal(t, http.StatusOK, w.Code)
		assert.JSONEq(t,
			`{"apiVersion":"v2","otelBaseUrl":"https://otel.example.com/v1"}`,
			w.Body.String())
	})
}

func TestParseOTELBaseURL(t *testing.T) {
	for _, tt := range []struct{ name, raw, want string }{
		{"https URL passes through", "https://otel.example.com/v1", "https://otel.example.com/v1"},
		{"http URL passes through", "http://localhost:4318", "http://localhost:4318"},
		{"trailing slash trimmed", "https://otel.example.com/v1/", "https://otel.example.com/v1"},
		{"multiple trailing slashes trimmed", "https://otel.example.com/v1//", "https://otel.example.com/v1"},
		{"bare root slash trimmed", "https://otel.example.com/", "https://otel.example.com"},
		{"uppercase scheme normalized", "HTTPS://otel.example.com/v1", "https://otel.example.com/v1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOTELBaseURL(tt.raw)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	for _, tt := range []struct{ name, raw string }{
		{"empty string", ""},
		{"garbage", "not a url"},
		{"relative URL", "/v1"},
		{"missing scheme", "otel.example.com/v1"},
		{"scheme without host", "https://"},
		{"non-http scheme", "ftp://otel.example.com"},
		{"control character", "https://otel.example.com/\n"},
		// The client appends /trace and /log — a base URL carrying a query,
		// fragment, or credentials would break or leak on that append.
		{"query string", "https://otel.example.com/v1?x=1"},
		{"force query", "https://otel.example.com/v1?"},
		{"fragment", "https://otel.example.com/v1#frag"},
		{"embedded credentials", "https://user:pass@otel.example.com/v1"},
	} {
		t.Run(tt.name+" is rejected", func(t *testing.T) {
			_, err := parseOTELBaseURL(tt.raw)
			assert.Error(t, err)
		})
	}
}

func TestHandleLogin_401_UnknownUser_NoUpstreamCall(t *testing.T) {
	var captured upstreamCapture
	srv := upstreamMock(t, http.StatusInternalServerError, "should not be called", &captured)
	defer srv.Close()
	h := loginHandler(t, cacheWith(bot), srv.URL)

	w := postLogin(t, setupRouter(t, h), loginRequest{Username: "ghost.bot", Password: "p"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_credentials")
	assert.Equal(t, 0, captured.calls, "portal must NOT call upstream when account is unknown")
}

func TestHandleLogin_401_RoleGate_RegularUserRejectedLocally(t *testing.T) {
	// alice has no roles → not bot/admin → portal rejects without east-west hop.
	var captured upstreamCapture
	srv := upstreamMock(t, http.StatusInternalServerError, "should not be called", &captured)
	defer srv.Close()
	h := loginHandler(t, cacheWith(alice), srv.URL)

	w := postLogin(t, setupRouter(t, h), loginRequest{Username: "alice", Password: "p"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_credentials")
	assert.Equal(t, 0, captured.calls, "role-gate failure must NOT call upstream")
}

func TestHandleLogin_UpstreamReturns401_PropagateVerbatim(t *testing.T) {
	const envelope = `{"status":401,"code":"unauthenticated","reason":"invalid_credentials","message":"invalid credentials"}`
	srv := upstreamMock(t, http.StatusUnauthorized, envelope, nil)
	defer srv.Close()
	h := loginHandler(t, cacheWith(bot), srv.URL)

	w := postLogin(t, setupRouter(t, h), loginRequest{Username: bot.Account, Password: "wrong"})
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, envelope, w.Body.String())
}

func TestHandleLogin_UpstreamError_PropagateVerbatim(t *testing.T) {
	// Portal relays any non-2xx botplatform envelope byte-for-byte, without
	// interpreting the code/reason.
	const envelope = `{"status":403,"code":"forbidden","reason":"account_disabled"}`
	srv := upstreamMock(t, http.StatusForbidden, envelope, nil)
	defer srv.Close()
	h := loginHandler(t, cacheWith(bot), srv.URL)

	w := postLogin(t, setupRouter(t, h), loginRequest{Username: bot.Account, Password: "p"})
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, envelope, w.Body.String())
}

func TestHandleLogin_503_UpstreamUnreachable(t *testing.T) {
	// Point at a closed port to force a connection-refused error.
	h := loginHandler(t, cacheWith(bot), "http://127.0.0.1:1")
	w := postLogin(t, setupRouter(t, h), loginRequest{Username: bot.Account, Password: "p"})
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "upstream_unavailable")
}

func TestHandleLogin_500_SiteUnknown(t *testing.T) {
	// Bot account homed on a site missing from the registry.
	orphan := bot
	orphan.SiteID = "site-orphan"
	h := loginHandler(t, cacheWith(orphan), "http://127.0.0.1:1")
	w := postLogin(t, setupRouter(t, h), loginRequest{Username: orphan.Account, Password: "p"})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// setSettingsEnv sets a complete valid environment for config parsing;
// individual subtests override single vars to probe the notEmpty tags.
func setSettingsEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PORTAL_SITE_URLS", `{"site-a":{"baseUrl":"https://a.com","natsUrl":"wss://nats.a.com"}}`)
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("PORTAL_API_VERSION", "v2")
	t.Setenv("PORTAL_OTEL_BASE_URL", "https://otel.example.com/v1")
}

func TestConfig_SettingsEnvVars(t *testing.T) {
	t.Run("parses both settings fields", func(t *testing.T) {
		setSettingsEnv(t)
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, "v2", cfg.APIVersion)
		assert.Equal(t, "https://otel.example.com/v1", cfg.OTELBaseURL)
	})

	for _, name := range []string{"PORTAL_API_VERSION", "PORTAL_OTEL_BASE_URL"} {
		t.Run("empty "+name+" is rejected", func(t *testing.T) {
			setSettingsEnv(t)
			t.Setenv(name, "")
			_, err := env.ParseAs[config]()
			assert.ErrorContains(t, err, name)
		})
	}
}

func TestParseOTELBaseURL_RejectionErrorRedactsPassword(t *testing.T) {
	// The rejection error is wrapped into the fatal startup log, so it must
	// not echo the credential the rule exists to reject.
	_, err := parseOTELBaseURL("https://user:hunter2@otel.example.com/v1")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hunter2")
}

func TestParseSiteURLs(t *testing.T) {
	t.Run("valid registry decodes per-site URLs", func(t *testing.T) {
		sites, err := parseSiteURLs(`{"site-a":{"baseUrl":"https://a.com","natsUrl":"wss://nats.a.com"},"site-b":{"baseUrl":"https://b.com","natsUrl":"wss://nats.b.com"}}`)
		require.NoError(t, err)
		assert.Equal(t, siteURL{BaseURL: "https://a.com", NATSURL: "wss://nats.a.com"}, sites["site-a"])
		assert.Equal(t, siteURL{BaseURL: "https://b.com", NATSURL: "wss://nats.b.com"}, sites["site-b"])
	})

	for _, tt := range []struct{ name, raw string }{
		{"not JSON", "site-a=https://a.com"},
		{"empty object", "{}"},
		{"missing baseUrl", `{"site-a":{"natsUrl":"wss://nats.a.com"}}`},
		{"missing natsUrl", `{"site-a":{"baseUrl":"https://a.com"}}`},
	} {
		t.Run(tt.name+" is rejected", func(t *testing.T) {
			_, err := parseSiteURLs(tt.raw)
			assert.Error(t, err)
		})
	}
}
