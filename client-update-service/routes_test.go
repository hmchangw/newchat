package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// passthroughAuth stands in for the real middleware: it records that it ran.
func passthroughAuth(ran *bool) gin.HandlerFunc {
	return func(c *gin.Context) { *ran = true; c.Next() }
}

// TestRegisterRoutes_UploadIsGuarded pins the security boundary: the upload
// route must run the auth middleware.
func TestRegisterRoutes_UploadIsGuarded(t *testing.T) {
	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(nil, testCache(1024)), passthroughAuth(&ran))

	// No body: the handler will reject it, but the middleware must have run first.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/version", nil))

	assert.True(t, ran, "POST /api/v1/version must be behind the auth middleware")
}

// TestRegisterRoutes_DownloadStaysOpen is a regression guard for spec §2: the
// download must never require a credential, because deployed desktop update
// clients hold none and cannot obtain one.
//
// Unlike the upload case, this route runs the handler to completion (there is
// no early-reject on a missing body), so it needs a real store — nil would
// panic on the store call, which is unrelated to the auth boundary this test
// pins.
func TestRegisterRoutes_DownloadStaysOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil, blobInfo{}, ErrObjectNotFound)

	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(store, testCache(1024)), passthroughAuth(&ran))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.yaml", nil))

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.False(t, ran, "GET /api/v1/version/:fileName must NOT be behind auth")
}

func TestRegisterRoutes_HealthStaysOpen(t *testing.T) {
	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(nil, testCache(1024)), passthroughAuth(&ran))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.False(t, ran, "the health probe must never require a credential")
}
