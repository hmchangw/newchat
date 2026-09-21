package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/ginutil"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

// POST /api/v1/auth is unauthenticated and spends bcrypt-scale CPU before any
// credential is known to be valid, so it must shed rather than queue. Without a
// cap, a caller with no credentials at all is an unmetered load generator.
func TestAuthRoute_ShedsBeyondTheConcurrencyCap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	signingKP, accPub := mustAccountKP(t)
	userPub := mustUserNKey(t)

	release := make(chan struct{})
	entered := make(chan struct{})
	handler := NewAuthHandler(&blockingValidator{entered: entered, release: release},
		signingKP, accPub, 2*time.Hour, false)

	r := gin.New()
	registerRoutes(r, handler, ginutil.ConcurrencyConfig{MaxConcurrency: 1}, nil)

	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		return req
	}

	go func() { r.ServeHTTP(httptest.NewRecorder(), newReq()) }()
	<-entered // the single slot is held

	w := httptest.NewRecorder()
	r.ServeHTTP(w, newReq())
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
	close(release)
}

// /healthz must stay answerable while the auth route is saturated, or a shed
// storm takes the pod out of rotation and makes the overload worse.
func TestHealthz_NotBehindTheConcurrencyCap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	signingKP, accPub := mustAccountKP(t)

	release := make(chan struct{})
	entered := make(chan struct{})
	handler := NewAuthHandler(&blockingValidator{entered: entered, release: release},
		signingKP, accPub, 2*time.Hour, false)

	r := gin.New()
	registerRoutes(r, handler, ginutil.ConcurrencyConfig{MaxConcurrency: 1}, nil)

	userPub := mustUserNKey(t)
	body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-entered

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, w.Code, "liveness must not queue behind auth")
	close(release)
}

// blockingValidator parks inside Validate so a test can hold the single
// admission slot for as long as it needs.
type blockingValidator struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (b *blockingValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	b.entered <- struct{}{}
	<-b.release
	return pkgoidc.Claims{Subject: "uuid-alice", PreferredUsername: "alice", Email: "alice@example.com"}, nil
}
