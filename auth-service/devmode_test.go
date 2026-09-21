package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Dev mode wires a nil validator (main.go: NewAuthHandler(nil, ...)), but
// HandleAuth still routes any request carrying an ssoToken into handleSSO.
// Before the guard that dereferenced nil and panicked the process — reachable
// by any caller, with no token needed to trigger it. handleSession has had the
// equivalent guard all along; this pins the same contract for handleSSO.
func TestHandleAuth_DevMode_SSOTokenWithNilValidator_DoesNotPanic(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	// Exactly what main.go constructs when DEV_MODE=true.
	handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
	router := setupRouter(t, handler)

	body := `{"ssoToken":"anything","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	require.NotPanics(t, func() { router.ServeHTTP(w, req) })
	assert.Equal(t, http.StatusServiceUnavailable, w.Code,
		"SSO auth is not configured in dev mode; say so instead of crashing")
}

// The dev-auth path itself still works with a nil validator — the guard must
// not break the mode it exists inside.
func TestHandleAuth_DevMode_NoTokenStillMintsJWT(t *testing.T) {
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
}
