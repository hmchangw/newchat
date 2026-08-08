package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
)

// run() must fail fast when required config is absent (no MONGO_URI/SITE_ID).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("SITE_ID", "")
	require.Error(t, run())
}

// newTestSDK starts the o11y SDK in its default (disabled) mode, which needs no
// collector and no network — cheap enough to call from a unit test. Callers
// must invoke the returned shutdown func.
func newTestSDK(t *testing.T) *o11y.SDK {
	t.Helper()
	sdk, shutdown, err := obs.Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, shutdown(context.Background()))
	})
	return sdk
}

func TestNewServer_AddrAndTimeouts(t *testing.T) {
	sdk := newTestSDK(t)
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockRoomStore(ctrl), "site-a")

	srv := newServer(Config{Port: "18080", SiteID: "site-a"}, sdk, h)

	assert.Equal(t, ":18080", srv.Addr)
	assert.Equal(t, 10*time.Second, srv.ReadTimeout)
	assert.Equal(t, 30*time.Second, srv.WriteTimeout)
}

func TestNewServer_RoutesRegistered(t *testing.T) {
	sdk := newTestSDK(t)
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockRoomStore(ctrl), "site-a")
	srv := newServer(Config{Port: "18080", SiteID: "site-a"}, sdk, h)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestNewServer_VerifyReachesStore(t *testing.T) {
	sdk := newTestSDK(t)
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().RoomStates(gomock.Any(), gomock.Any()).
		Return(map[string]RoomState{}, nil)
	h := NewHandler(store, "site-a")
	srv := newServer(Config{Port: "18080", SiteID: "site-a"}, sdk, h)

	req := httptest.NewRequest(http.MethodPost, "/internal/teams/rooms/verify",
		strings.NewReader(`{"chatIds":["19:abc@thread.v2"]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestNewServer_RequestIDMiddlewareWired(t *testing.T) {
	sdk := newTestSDK(t)
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockRoomStore(ctrl), "site-a")
	srv := newServer(Config{Port: "18080", SiteID: "site-a"}, sdk, h)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, req)

	assert.NotEmpty(t, w.Header().Get(natsutil.RequestIDHeader))
}
