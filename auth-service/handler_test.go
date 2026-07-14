package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/principal"
)

// fakeValidator implements TokenValidator for testing.
type fakeValidator struct {
	account     string
	subject     string
	email       string
	name        string
	description string
	deptName    string
	deptId      string
	expired     bool
	invalid     bool
}

func (f *fakeValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	if f.expired {
		return pkgoidc.Claims{}, pkgoidc.ErrTokenExpired
	}
	if f.invalid {
		return pkgoidc.Claims{}, fmt.Errorf("oidc token verification failed: invalid signature")
	}
	return pkgoidc.Claims{
		Subject:           f.subject,
		Email:             f.email,
		Name:              f.name,
		PreferredUsername: f.account,
		Description:       f.description,
		DeptName:          f.deptName,
		DeptID:            f.deptId,
	}, nil
}

// helper: create a fresh account signing key pair for tests. Returns both the
// keypair and its public key — every AuthHandler needs the account pubkey to
// stamp issuer_account on the JWT.
func mustAccountKP(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	require.NoError(t, err, "create account key")
	pub, err := kp.PublicKey()
	require.NoError(t, err, "account public key")
	return kp, pub
}

// helper: create a fresh user nkey public key for tests.
func mustUserNKey(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err, "create user key")
	pub, err := kp.PublicKey()
	require.NoError(t, err, "public key")
	return pub
}

// helper: set up gin engine with auth handler using a fake validator.
func setupRouter(t *testing.T, handler *AuthHandler) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, handler)
	return r
}

func TestHandleAuth_ValidToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	validator := &fakeValidator{
		account:     "alice",
		subject:     "uuid-alice",
		email:       "alice@example.com",
		description: "E001, Alice Wang, 王小明",
		deptName:    "Engineering",
		deptId:      "ABC123",
	}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	// Verify user info in response.
	assert.Equal(t, "alice@example.com", resp.UserInfo.Email)
	assert.Equal(t, "alice", resp.UserInfo.Account)
	assert.Equal(t, "E001", resp.UserInfo.EmployeeID)
	assert.Equal(t, "Alice Wang", resp.UserInfo.EngName)
	assert.Equal(t, "王小明", resp.UserInfo.ChineseName)
	assert.Equal(t, "Engineering", resp.UserInfo.DeptName)
	assert.Equal(t, "ABC123", resp.UserInfo.DeptID)

	// Decode and verify the NATS JWT.
	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)
	assert.Equal(t, userPub, claims.Subject)

	// Check expiration is set (within 2 hours).
	require.NotZero(t, claims.Expires)
	expiresAt := time.Unix(claims.Expires, 0)
	assert.LessOrEqual(t, time.Until(expiresAt), 2*time.Hour+time.Minute)

	// Perms and limits live on the scoped signing key template; the JWT
	// only stamps the account tag for {{tag(account)}} substitution.
	assert.Contains(t, claims.Tags, "account:alice")
	assert.Empty(t, claims.Pub.Allow)
	assert.Empty(t, claims.Sub.Allow)
	assert.Equal(t, jwt.UserPermissionLimits{}, claims.UserPermissionLimits,
		"non-zero per-user limits trigger auth violation under a scoped SK")
}

func TestHandleAuth_ExpiredToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	validator := &fakeValidator{expired: true}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"expired-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthTokenExpired)
}

func TestHandleAuth_InvalidToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	validator := &fakeValidator{invalid: true}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"bad-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthInvalidToken)
}

func TestHandleAuth_InvalidNKey(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"NOT-A-VALID-NKEY"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleAuth_MissingFields(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	validator := &fakeValidator{account: "alice"}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	tests := []struct {
		name string
		body string
	}{
		{"missing ssoToken", `{"natsPublicKey":"somekey"}`},
		{"missing natsPublicKey", `{"ssoToken":"tok"}`},
		{"empty body", `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
	}
}

func TestHandleAuth_PermissionsPerUser(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)

	accounts := []string{"alice", "bob", "charlie"}
	for _, account := range accounts {
		t.Run(account, func(t *testing.T) {
			validator := &fakeValidator{account: account, subject: "uuid-" + account}
			handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false)
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"token-` + account + `","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)

			var resp authResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

			claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
			require.NoError(t, err)

			assert.Contains(t, claims.Tags, "account:"+account)
			assert.Equal(t, accPub, claims.IssuerAccount)
			assert.Empty(t, claims.Pub.Allow)
			assert.Empty(t, claims.Sub.Allow)
		})
	}
}

func TestHandleAuth_DevMode_ValidRequest(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"account":"alice","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.Equal(t, "alice", resp.UserInfo.Account)
	assert.Equal(t, "alice", resp.UserInfo.EngName)
	assert.Equal(t, "alice@dev.local", resp.UserInfo.Email)

	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)
	assert.Equal(t, userPub, claims.Subject)
	assert.Contains(t, claims.Tags, "account:alice")
}

// TestHandleAuth_DevMode_NoToken_DoesNotValidate confirms the tokenless dev
// branch mints directly without calling either SSO or botplatform
// validation, even when both validators are configured.
func TestHandleAuth_DevMode_NoToken_DoesNotValidate(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	bp := &fakeBPValidator{}
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"account":"alice","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, 0, bp.calls, "tokenless dev request must not call botplatform")

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "alice", resp.UserInfo.Account)
}

// TestHandleAuth_DevMode_WithSSOToken_UsesSSO ensures dev mode's tokenless
// short-circuit does not swallow a request that does carry an ssoToken —
// it must still route through the ordinary OIDC validation path.
func TestHandleAuth_DevMode_WithSSOToken_UsesSSO(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "alice", resp.UserInfo.Account)
}

// TestHandleAuth_DevMode_WithAuthToken_UsesSession ensures dev mode's
// tokenless short-circuit does not swallow a request that does carry an
// authToken — it must still validate via botplatform, not dev-mint.
func TestHandleAuth_DevMode_WithAuthToken_UsesSession(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	bp := &fakeBPValidator{principal: principal.Principal{
		UserID:  "u1",
		Account: "p_admin",
		SiteID:  "site-a",
		Roles:   []string{"admin"},
	}}
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"authToken":"session-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, 1, bp.calls, "authToken must route to botplatform validation, not dev mint")
	assert.Equal(t, "session-token", bp.lastToken)

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "p_admin", resp.UserInfo.Account)
}

func TestHandleAuth_DevMode_MissingAccount(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleAuth_DevMode_InvalidNKey(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)

	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"account":"alice","natsPublicKey":"NOT-VALID"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleAuth_DevMode_TokenGenerationFailure(t *testing.T) {
	// Force signNATSJWT (uc.Encode) to fail by supplying a non-account
	// signing key. A user key pair cannot sign a NATS user JWT, so Encode
	// returns an error, exercising the 500 internal-error path. The real
	// cause is logged via Classify and must NOT appear in the response body.
	userKP, err := nkeys.CreateUser()
	require.NoError(t, err, "create user key")
	_, accPub := mustAccountKP(t)

	handler := NewAuthHandler(nil, userKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"account":"alice","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeInternal)

	var env errcode.Error
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, "internal error", env.Message)
	assert.NotContains(t, w.Body.String(), "generating NATS token")
}

func TestHandleHealth(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	handler := NewAuthHandler(&fakeValidator{}, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestWithJitter_Clamping(t *testing.T) {
	kp, accPub := mustAccountKP(t)
	cases := []struct {
		name string
		in   float64
		want float64
	}{
		{"negative clamps to zero", -0.5, 0},
		{"zero stays zero", 0, 0},
		{"mid passes through", 0.5, 0.5},
		{"upper bound stays", 0.9, 0.9},
		{"above max clamps to 0.9", 1.5, 0.9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewAuthHandler(nil, kp, accPub, time.Hour, true, WithJitter(tc.in))
			assert.Equal(t, tc.want, h.jwtJitter)
		})
	}
}

func TestSignNATSJWT_LifetimeJitter(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	base := 100 * time.Minute

	tests := []struct {
		name      string
		rnd       float64
		wantRatio float64 // expected multiple of base
	}{
		{"low end", 0.0, 0.9},
		{"midpoint", 0.5, 1.0},
		{"high end", 1.0, 1.1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewAuthHandler(validator, signingKP, accPub, base, false,
				WithJitter(0.1), WithRandFloat(func() float64 { return tt.rnd }))
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"valid","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			before := time.Now()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			var resp authResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
			require.NoError(t, err)

			wantLifeSec := (time.Duration(float64(base) * tt.wantRatio)).Seconds()
			gotLifeSec := time.Unix(claims.Expires, 0).Sub(before).Seconds()
			assert.InDelta(t, wantLifeSec, gotLifeSec, 5) // 5s slack for exec time
		})
	}
}

func TestHandleAuth_MissingAccountClaim(t *testing.T) {
	// Prod-mode guard: a token with no usable account claim must be refused
	// before minting — the JWT would otherwise grant chat.user..> permissions.
	signingKP, accPub := mustAccountKP(t)
	handler := NewAuthHandler(&fakeValidator{}, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"` + mustUserNKey(t) + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthInvalidToken)
}

func TestHandleAuth_InvalidAccountFormat(t *testing.T) {
	// The account becomes a NATS subject token (chat.user.{account}.>): dots
	// nest namespaces, wildcards broaden grants — refuse before gate and sign.
	signingKP, accPub := mustAccountKP(t)
	cases := []struct{ name, account string }{
		{"dotted account nests subjects", "john.doe"},
		{"single-token wildcard", "mal*ory"},
		{"multi-token wildcard", "mal>ory"},
		{"whitespace", "mal ory"},
	}
	for _, tt := range cases {
		t.Run("prod: "+tt.name, func(t *testing.T) {
			handler := NewAuthHandler(&fakeValidator{account: tt.account}, signingKP, accPub, 2*time.Hour, false)
			router := setupRouter(t, handler)

			body := `{"ssoToken":"valid-token","natsPublicKey":"` + mustUserNKey(t) + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
		t.Run("dev: "+tt.name, func(t *testing.T) {
			handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
			router := setupRouter(t, handler)

			payload, err := json.Marshal(map[string]string{"account": tt.account, "natsPublicKey": mustUserNKey(t)})
			require.NoError(t, err)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(string(payload)))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
	}

	// Any account the routing layer can serve must pass the mint gate too:
	// the rule is exactly pkg/subject's token invariant, not an ASCII allowlist.
	for _, account := range []string{"alice@corp", "júlio"} {
		t.Run("routable account accepted: "+account, func(t *testing.T) {
			handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
			router := setupRouter(t, handler)

			payload, err := json.Marshal(map[string]string{"account": account, "natsPublicKey": mustUserNKey(t)})
			require.NoError(t, err)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(string(payload)))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

// ----- session-token branch tests --------------------------------------

// fakeBPValidator implements BotplatformValidator for unit tests.
type fakeBPValidator struct {
	principal principal.Principal
	err       error
	calls     int
	lastToken string
}

func (f *fakeBPValidator) Validate(_ context.Context, authToken string) (principal.Principal, error) {
	f.calls++
	f.lastToken = authToken
	if f.err != nil {
		return principal.Principal{}, f.err
	}
	return f.principal, nil
}

func TestHandleAuth_SessionToken_Bot_HappyPath(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{principal: principal.Principal{
		UserID:  "u1",
		Account: "name.shortcode.bot",
		SiteID:  "site-a",
		Roles:   []string{"bot"},
	}}
	// SSO validator must NOT be called for session-token requests; nil
	// validator would panic if the wrong branch fires.
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, false, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"authToken":"bp-session","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	// Bot account dots collapse to underscores for the NATS subject slot.
	assert.Equal(t, "name_shortcode_bot", resp.UserInfo.Account)
	assert.Empty(t, resp.UserInfo.EmployeeID, "bot session: no employee fields")

	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)
	// Perms live on the scoped signing key template; the JWT only carries
	// the account tag used for {{tag(account)}} substitution.
	assert.Contains(t, claims.Tags, "account:name_shortcode_bot")
	assert.Empty(t, claims.Pub.Allow)
	assert.Empty(t, claims.Sub.Allow)
	assert.Equal(t, 1, bp.calls)
	assert.Equal(t, "bp-session", bp.lastToken)
}

func TestHandleAuth_SessionToken_Admin(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{principal: principal.Principal{
		UserID:  "u1",
		Account: "p_admin",
		SiteID:  "site-a",
		Roles:   []string{"admin"},
	}}
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, false, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"authToken":"admin-session","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)
	assert.Contains(t, claims.Tags, "account:p_admin")
	assert.Empty(t, claims.Pub.Allow)
	assert.Empty(t, claims.Sub.Allow)
}

func TestHandleAuth_SessionToken_InvalidToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{err: errcode.Unauthenticated("session token invalid",
		errcode.WithReason(errcode.BotplatformInvalidToken))}
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, false, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"authToken":"bad","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.BotplatformInvalidToken)
}

func TestHandleAuth_SessionToken_UpstreamUnavailable(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{err: errors.New("connection refused")}
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, false, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"authToken":"x","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.BotplatformUpstreamUnavailable)
}

func TestHandleAuth_SessionToken_WithoutBPValidator_503(t *testing.T) {
	// No WithBotplatformValidator option -> session-token requests must
	// fail with 503 upstream_unavailable, not run the OIDC path with a
	// non-OIDC payload.
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"authToken":"any","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.BotplatformUpstreamUnavailable)
}

func TestHandleAuth_AmbiguousToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{}
	handler := NewAuthHandler(&fakeValidator{account: "alice", subject: "u"}, signingKP, accPub, 2*time.Hour, false,
		WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"ssoToken":"a","authToken":"b","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.BotplatformAmbiguousToken)
	assert.Equal(t, 0, bp.calls, "ambiguous request must not call botplatform")
}

func TestHandleAuth_MissingToken(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	handler := NewAuthHandler(&fakeValidator{}, signingKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.BotplatformMissingToken)
}

// Regression: existing SSO path must NOT call botplatform even when a
// validator is configured.
func TestHandleAuth_SSO_DoesNotCallBotplatform(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)
	bp := &fakeBPValidator{}
	validator := &fakeValidator{account: "alice", subject: "u", description: "E1,Alice,A"}
	handler := NewAuthHandler(validator, signingKP, accPub, 2*time.Hour, false, WithBotplatformValidator(bp))
	router := setupRouter(t, handler)

	body := `{"ssoToken":"ok","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, 0, bp.calls, "SSO path must never hit botplatform")
}

func TestHandleAuth_TokenGenerationFailure(t *testing.T) {
	// Prod-mode twin of the dev-mode test: a user key cannot sign, so the prod path returns 500.
	userKP, err := nkeys.CreateUser()
	require.NoError(t, err, "create user key")
	_, accPub := mustAccountKP(t)

	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	handler := NewAuthHandler(validator, userKP, accPub, 2*time.Hour, false)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeInternal)
	assert.NotContains(t, w.Body.String(), "generating NATS token")
}
