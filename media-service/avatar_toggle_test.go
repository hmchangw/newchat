package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// newDisabledAvatarRouter builds the avatar router with DEFAULT_AVATAR_ENABLED=false.
func newDisabledAvatarRouter(t *testing.T) (*gin.Engine, *MockavatarStore) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	h := newHandler(store, NewMockemojiStore(ctrl), &fakeBlobStore{}, &config{
		SiteID:               "s1",
		EmployeePhotoBaseURL: "https://photos.example.com",
		CacheMaxAgeSeconds:   3600,
		DefaultAvatarEnabled: false,
		MinioBucket:          "avatars",
		EIDCacheCapacity:     1000,
		EIDCacheTTL:          time.Minute,
	})
	r := gin.New()
	registerRoutes(r, h, &fakeSessionValidator{principal: adminPrincipal()}, testSiteID)
	return r, store
}

func get(t *testing.T, r *gin.Engine, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

// With the default disabled, every generated-default path returns 404 instead of an SVG.
func TestDefaultAvatarDisabled_User_404(t *testing.T) {
	r, store := newDisabledAvatarRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("", false, nil)
	w := get(t, r, "/api/v1/avatar/alice")
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.NotContains(t, w.Body.String(), "<svg")
	assert.Contains(t, w.Body.String(), "not_found")
	// Bounded negative-cache so a disabled default doesn't force a per-render backend lookup.
	assert.Equal(t, "public, max-age=3600", w.Header().Get("Cache-Control"))
}

func TestDefaultAvatarDisabled_Bot_404(t *testing.T) {
	r, store := newDisabledAvatarRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("", false, nil)
	w := get(t, r, "/api/v1/avatar/helper.bot")
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.NotContains(t, w.Body.String(), "<svg")
}

func TestDefaultAvatarDisabled_Room_404(t *testing.T) {
	r, store := newDisabledAvatarRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("", model.RoomType(""), "", false, nil)
	w := get(t, r, "/api/v1/avatar/room/room1")
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.NotContains(t, w.Body.String(), "<svg")
}

// Enabled (default) still serves the SVG — the toggle is the only difference.
func TestDefaultAvatarEnabled_User_SVG(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("", false, nil)
	w := get(t, r, "/api/v1/avatar/alice")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Body.String(), "<svg")
}
