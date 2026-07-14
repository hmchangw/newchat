package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func newTestRouter(t *testing.T) (*gin.Engine, *MockavatarStore, *fakeBlobStore) {
	r, store, _, blobs := newEmojiTestRouter(t)
	return r, store, blobs
}

func newEmojiTestRouter(t *testing.T) (*gin.Engine, *MockavatarStore, *MockemojiStore, *fakeBlobStore) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	emojis := NewMockemojiStore(ctrl)
	blobs := &fakeBlobStore{}
	h := newHandler(store, emojis, blobs, &config{
		SiteID:               "s1",
		EmployeePhotoBaseURL: "https://photos.example.com",
		CacheMaxAgeSeconds:   3600,
		MinioBucket:          "avatars",
		ClusterDomains:       clusterDomains{byID: map[string]string{"s2": "https://avatar-s2"}}, // keep the original s2 value — existing avatar tests assert on it
		MaxUploadBytes:       1048576,
		EmojiMaxUploadBytes:  262144,
		EmojiMaxDimension:    512,
		EIDCacheCapacity:     1000,
		EIDCacheTTL:          time.Minute,
	})
	r := gin.New()
	registerRoutes(r, h)
	return r, store, emojis, blobs
}

// fakeBlobStore is an in-memory blobStore for handler tests.
type fakeBlobStore struct {
	objects   map[string][]byte
	info      map[string]blobInfo
	putErr    error
	getErr    error
	deleteErr error
}

func (f *fakeBlobStore) Get(_ context.Context, key string) (io.ReadCloser, blobInfo, error) {
	if f.getErr != nil {
		return nil, blobInfo{}, f.getErr
	}
	b, ok := f.objects[key]
	if !ok {
		return nil, blobInfo{}, errBlobNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), f.info[key], nil
}

func (f *fakeBlobStore) Put(_ context.Context, key string, r io.Reader, _ int64, ct string) (string, error) {
	if f.putErr != nil {
		return "", f.putErr
	}
	if f.objects == nil {
		f.objects = map[string][]byte{}
		f.info = map[string]blobInfo{}
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	f.objects[key] = b
	f.info[key] = blobInfo{Size: int64(len(b)), ContentType: ct, ETag: "etag-" + key}
	return "etag-" + key, nil
}

func (f *fakeBlobStore) Delete(_ context.Context, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.objects, key)
	delete(f.info, key)
	return nil
}

func TestHandleHealth(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestEndpoint1_UserRedirectToEmployeePhoto(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("E123", true, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/alice", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://photos.example.com/E123_120.JPG", w.Header().Get("Location"))
}

func TestEndpoint1_UserNoEmployeeID_ServesDefault(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("", false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/alice", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	assert.Contains(t, w.Body.String(), "<svg")
}

func TestEndpoint1_UserNoEmployeeID_NotModified(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("", false, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatar/alice", nil)
	req.Header.Set("If-None-Match", defaultETag("alice", "alice"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestEndpoint1_BotLocalCustomImage_Streams(t *testing.T) {
	r, store, blobs := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s1", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").
		Return(&model.Avatar{MinioKey: "bot/helper.bot", ETag: `"e1"`}, true, nil)
	blobs.objects = map[string][]byte{"bot/helper.bot": []byte("PNG")}
	blobs.info = map[string]blobInfo{"bot/helper.bot": {Size: 3, ContentType: "image/png", ETag: `"e1"`}}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	assert.Equal(t, "PNG", w.Body.String())
}

func TestEndpoint1_BotCustomImage_NotModified(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s1", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").
		Return(&model.Avatar{MinioKey: "bot/helper.bot", ETag: `"e1"`}, true, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil)
	req.Header.Set("If-None-Match", `"e1"`)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestEndpoint1_BotNoRecord_ServesDefault(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("", false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint1_BotRemoteCluster_Redirects(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s2", true, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/api/v1/avatar/helper.bot?fwd=1", w.Header().Get("Location"))
}

func TestEndpoint1_BotSiteidHint_SkipsBotSite(t *testing.T) {
	r, store, _ := newTestRouter(t)
	_ = store // no BotSite EXPECT — the hint must skip the lookup
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot?siteid=s2", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/api/v1/avatar/helper.bot?fwd=1", w.Header().Get("Location"))
}

func TestEndpoint1_BotRemoteWithFwd_NoReRedirect(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s2", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot?fwd=1", nil))
	assert.Equal(t, http.StatusOK, w.Code) // served default locally despite remote site
}

func TestEndpoint2_ChannelCustomImage_Streams(t *testing.T) {
	r, store, blobs := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectRoom, "room-1").
		Return(&model.Avatar{MinioKey: "room/room-1", ETag: `"r1"`}, true, nil)
	blobs.objects = map[string][]byte{"room/room-1": []byte("PNG")}
	blobs.info = map[string]blobInfo{"room/room-1": {Size: 3, ContentType: "image/png", ETag: `"r1"`}}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "PNG", w.Body.String())
}

func TestEndpoint2_ChannelNoCustomImage_Default(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectRoom, "room-1").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint2_NotFound_Default(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-x").Return("", model.RoomType(""), "", false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-x", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint2_DMType_Default(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "dm-1").Return("s1", model.RoomTypeDM, "", true, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/dm-1", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint2_RemoteCluster_Redirects(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("s2", model.RoomTypeChannel, "General", true, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/api/v1/avatar/room/room-1?fwd=1", w.Header().Get("Location"))
}

func TestEndpoint2_UnknownRemoteSite_FallsThroughToLocal(t *testing.T) {
	// "s3" is a real, non-local site the store returns, but it has no entry in
	// ClusterDomains — redirectCrossCluster must not redirect to an unresolvable
	// base URL and instead falls through to the normal local serving path.
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("s3", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectRoom, "room-1").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint2_SiteidHint_RemoteRedirect_SkipsRoomSite(t *testing.T) {
	r, store, _ := newTestRouter(t)
	_ = store // no RoomSite EXPECT — the hint skips it
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1?siteid=s2", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/api/v1/avatar/room/room-1?fwd=1", w.Header().Get("Location"))
}

func TestEndpoint2_SiteidHint_Local_AvatarsLookupOnly(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectRoom, "room-1").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1?siteid=s1", nil))
	assert.Equal(t, http.StatusOK, w.Code) // default, no RoomSite call
}

func TestEndpoint1_BotSiteError_500(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("", false, errors.New("mongo down"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEndpoint1_BotAvatarError_500(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s1", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").Return(nil, false, errors.New("mongo down"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEndpoint1_UserEmployeeIDError_500(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().EmployeeID(gomock.Any(), "alice").Return("", false, errors.New("mongo down"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/alice", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestServeStored_BlobError_500(t *testing.T) {
	r, store, blobs := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s1", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").
		Return(&model.Avatar{MinioKey: "bot/helper.bot", ETag: `"e1"`}, true, nil)
	blobs.getErr = errors.New("minio down")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestServeStored_BlobNotFound_FallsBackToDefault(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().BotSite(gomock.Any(), "helper.bot").Return("s1", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectBot, "helper.bot").
		Return(&model.Avatar{MinioKey: "bot/helper.bot", ETag: `"e1"`}, true, nil)
	// blobs has no object for that key → Get returns errBlobNotFound → default SVG
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/helper.bot", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
}

func TestEndpoint2_RoomSiteError_500(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("", model.RoomType(""), "", false, errors.New("mongo down"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEndpoint2_RoomAvatarError_500(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room-1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().Avatar(gomock.Any(), model.AvatarSubjectRoom, "room-1").Return(nil, false, errors.New("mongo down"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/avatar/room/room-1", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
