package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/svcjwt"
)

// Aliases so a rename of either reason is caught at compile time.
var (
	errcodeReasonInvalidToken  = errcode.ClientUpdateInvalidToken
	errcodeReasonNotAuthorized = errcode.ClientUpdateNotAuthorized
)

const (
	authTestIssuer   = "admin-service"
	authTestAudience = "client-update-service"
	authTestAccount  = "svc-updater"
)

// authTestPair returns a signer plus the verifier that trusts it.
func authTestPair(t *testing.T) (*svcjwt.Signer, *svcjwt.Verifier) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	s, err := svcjwt.NewSigner(enc.EncodeToString(priv.Seed()), authTestIssuer)
	require.NoError(t, err)
	v, err := svcjwt.NewVerifier(enc.EncodeToString(pub), authTestIssuer, authTestAudience)
	require.NoError(t, err)
	return s, v
}

// stubVerifier lets a test force a verification failure without constructing a
// token that breaks a specific rule — those rules are pkg/svcjwt's own tests.
type stubVerifier struct {
	claims *svcjwt.Claims
	err    error
}

func (s stubVerifier) Verify(string) (*svcjwt.Claims, error) { return s.claims, s.err }

// authRouter mounts the middleware on a probe route and reports whether the
// handler ran and what service account it saw.
func authRouter(v tokenVerifier, allowed []string, reached *bool, seen *string) *gin.Engine {
	r := gin.New()
	r.POST("/api/v1/version", requireServiceAccount(v, allowed), func(c *gin.Context) {
		*reached = true
		*seen = c.GetString(ctxServiceAccount)
		c.JSON(http.StatusOK, gin.H{"result": "success"})
	})
	return r
}

// envelope decodes the errcode error envelope: {"code":…,"reason":…,"error":…}.
func envelope(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func TestRequireServiceAccount_AllowsAllowlistedSubject(t *testing.T) {
	signer, verifier := authTestPair(t)
	token, _, err := signer.Sign(authTestAccount, authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	r := authRouter(verifier, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached, "an allowlisted service account must reach the handler")
	assert.Equal(t, authTestAccount, seen, "the verified subject must be on the context for the access log")
}

func TestRequireServiceAccount_RejectsUnallowlistedSubject(t *testing.T) {
	signer, verifier := authTestPair(t)
	// A perfectly valid token — signed by the trusted key, right issuer and
	// audience — for an account that is simply not permitted.
	token, _, err := signer.Sign("svc-someone-else", authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	r := authRouter(verifier, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.False(t, reached)
	assert.Equal(t, string(errcodeReasonNotAuthorized), envelope(t, w.Body.Bytes())["reason"])
}

func TestRequireServiceAccount_RejectsBadCredentials(t *testing.T) {
	tests := []struct {
		name     string
		header   string // "" means send no Authorization header
		verifier tokenVerifier
	}{
		{"no header", "", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"empty bearer", "Bearer ", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"wrong scheme", "Basic abc123", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"raw token, no scheme", "sometoken", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"verifier rejects", "Bearer whatever", stubVerifier{err: svcjwt.ErrInvalidToken}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reached bool
			var seen string
			r := authRouter(tc.verifier, []string{authTestAccount}, &reached, &seen)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.False(t, reached, "the handler must not run for a bad credential")
			assert.Equal(t, string(errcodeReasonInvalidToken), envelope(t, w.Body.Bytes())["reason"])
		})
	}
}

// TestRequireServiceAccount_ResponseHidesTheCause guards the rule that a
// verification cause is logged server-side but never serialized.
func TestRequireServiceAccount_ResponseHidesTheCause(t *testing.T) {
	v := stubVerifier{err: errors.New("signature mismatch on segment two")}
	var reached bool
	var seen string
	r := authRouter(v, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer sometoken")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.NotContains(t, w.Body.String(), "segment two", "the cause must never reach the client")
	assert.NotContains(t, w.Body.String(), "sometoken", "the token must never be echoed")
}

func TestRequireServiceAccount_IgnoresBlankAllowlistEntries(t *testing.T) {
	signer, verifier := authTestPair(t)
	token, _, err := signer.Sign(authTestAccount, authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	// Whitespace and empty entries are what a sloppy env var looks like:
	// "svc-updater, , other". They must be trimmed, and blanks must never
	// become a permitted empty subject.
	r := authRouter(verifier, []string{" " + authTestAccount + " ", "", "  "}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached)
}

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name, header, want string
	}{
		{"bearer token", "Bearer abc", "abc"},
		{"bearer with padding", "Bearer   abc  ", "abc"},
		{"no header", "", ""},
		{"lowercase scheme is not accepted", "bearer abc", ""},
		{"other scheme", "Basic abc", ""},
		{"scheme only", "Bearer", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				c.Request.Header.Set("Authorization", tc.header)
			}
			assert.Equal(t, tc.want, bearerToken(c))
		})
	}
}
