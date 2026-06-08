package main

import (
	"context"
	"encoding/json"
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

// helper: create a fresh account signing key pair for tests.
func mustAccountKP(t *testing.T) nkeys.KeyPair {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	require.NoError(t, err, "create account key")
	return kp
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
	signingKP := mustAccountKP(t)
	userPub := mustUserNKey(t)

	validator := &fakeValidator{
		account:     "alice",
		subject:     "uuid-alice",
		email:       "alice@example.com",
		description: "E001, Alice Wang, 王小明",
		deptName:    "Engineering",
		deptId:      "ABC123",
	}
	handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
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

	// Check publish permissions: chat.user.alice.> and _INBOX.>
	assert.Contains(t, []string(claims.Pub.Allow), "chat.user.alice.>")
	assert.Contains(t, []string(claims.Pub.Allow), "_INBOX.>")

	// Check subscribe permissions: chat.user.alice.>, chat.room.>, _INBOX.>
	assert.Contains(t, []string(claims.Sub.Allow), "chat.user.alice.>")
	assert.Contains(t, []string(claims.Sub.Allow), "chat.room.>")
	assert.Contains(t, []string(claims.Sub.Allow), "_INBOX.>")
}

func TestHandleAuth_ExpiredToken(t *testing.T) {
	signingKP := mustAccountKP(t)
	validator := &fakeValidator{expired: true}
	handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"expired-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthTokenExpired)
}

func TestHandleAuth_InvalidToken(t *testing.T) {
	signingKP := mustAccountKP(t)
	validator := &fakeValidator{invalid: true}
	handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"bad-token","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthInvalidToken)
}

func TestHandleAuth_InvalidNKey(t *testing.T) {
	signingKP := mustAccountKP(t)
	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"valid-token","natsPublicKey":"NOT-A-VALID-NKEY"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleAuth_MissingFields(t *testing.T) {
	signingKP := mustAccountKP(t)
	validator := &fakeValidator{account: "alice"}
	handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
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
			req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
	}
}

func TestHandleAuth_PermissionsPerUser(t *testing.T) {
	signingKP := mustAccountKP(t)

	accounts := []string{"alice", "bob", "charlie"}
	for _, account := range accounts {
		t.Run(account, func(t *testing.T) {
			validator := &fakeValidator{account: account, subject: "uuid-" + account}
			handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false)
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"token-` + account + `","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)

			var resp authResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

			claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
			require.NoError(t, err)

			wantPub := "chat.user." + account + ".>"
			assert.Contains(t, []string(claims.Pub.Allow), wantPub)

			wantSub := "chat.user." + account + ".>"
			assert.Contains(t, []string(claims.Sub.Allow), wantSub)
		})
	}
}

func TestHandleAuth_DevMode_ValidRequest(t *testing.T) {
	signingKP := mustAccountKP(t)
	userPub := mustUserNKey(t)

	handler := NewAuthHandler(nil, signingKP, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"account":"alice","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.Equal(t, "alice", resp.UserInfo.Account)
	assert.Equal(t, "alice", resp.UserInfo.EngName)
	assert.Equal(t, "alice@dev.local", resp.UserInfo.Email)

	// Verify NATS JWT is valid and scoped to alice.
	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)
	assert.Equal(t, userPub, claims.Subject)
	assert.Contains(t, []string(claims.Pub.Allow), "chat.user.alice.>")
	assert.Contains(t, []string(claims.Sub.Allow), "chat.user.alice.>")
}

func TestHandleAuth_DevMode_MissingAccount(t *testing.T) {
	signingKP := mustAccountKP(t)
	userPub := mustUserNKey(t)

	handler := NewAuthHandler(nil, signingKP, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleAuth_DevMode_InvalidNKey(t *testing.T) {
	signingKP := mustAccountKP(t)

	handler := NewAuthHandler(nil, signingKP, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"account":"alice","natsPublicKey":"NOT-VALID"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
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

	handler := NewAuthHandler(nil, userKP, 2*time.Hour, true)
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"account":"alice","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
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
	signingKP := mustAccountKP(t)
	handler := NewAuthHandler(&fakeValidator{}, signingKP, 2*time.Hour, false)
	router := setupRouter(t, handler)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestWithJitter_Clamping(t *testing.T) {
	kp := mustAccountKP(t)
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
			h := NewAuthHandler(nil, kp, time.Hour, true, WithJitter(tc.in))
			assert.Equal(t, tc.want, h.jwtJitter)
		})
	}
}

func TestSignNATSJWT_LifetimeJitter(t *testing.T) {
	signingKP := mustAccountKP(t)
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
			handler := NewAuthHandler(validator, signingKP, base, false,
				WithJitter(0.1), WithRandFloat(func() float64 { return tt.rnd }))
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"valid","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
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
