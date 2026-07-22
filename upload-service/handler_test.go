package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/model"
)

const (
	testMaxImages            = 10
	testMaxAttachments       = 1
	testMaxImageSize   int64 = 25 << 20
	// testCacheMaxAge is a deliberately non-default value so download tests prove
	// the Cache-Control max-age reflects configuration rather than a hardcoded constant.
	testCacheMaxAge = 604800 // 1 week
)

// fakeDrive implements driveClient for handler tests.
type fakeDrive struct {
	uploadResp []drive.UploadGroupImageResponse
	uploadErr  error
	uploadGot  struct {
		userID, username, email, groupID, origin string
		n                                        int
		filenames                                []string
	}

	getResp *drive.GetGroupImageResponse
	getErr  error
	getGot  struct{ host, groupID, fileID string }

	baseURL string
}

func (f *fakeDrive) UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error) {
	f.uploadGot.userID, f.uploadGot.username, f.uploadGot.email = userID, username, email
	f.uploadGot.groupID, f.uploadGot.origin, f.uploadGot.n = groupID, origin, len(files)
	f.uploadGot.filenames = nil
	for _, mf := range files {
		f.uploadGot.filenames = append(f.uploadGot.filenames, mf.Filename)
	}
	return f.uploadResp, f.uploadErr
}
func (f *fakeDrive) GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error) {
	f.getGot.host, f.getGot.groupID, f.getGot.fileID = host, groupID, fileID
	return f.getResp, f.getErr
}
func (f *fakeDrive) GetBaseURLFromRoomOrigin(string) string { return f.baseURL }

// fakeS3 implements objectStore for handler tests.
type fakeS3 struct {
	body   string
	err    error
	gotKey string
}

func (f *fakeS3) Open(_ context.Context, key string) (io.ReadCloser, error) {
	f.gotKey = key
	if f.err != nil {
		return nil, f.err
	}
	return readCloser{strings.NewReader(f.body)}, nil
}

// multipartBody builds a multipart body with the named files under one field.
func multipartBody(t *testing.T, field string, files map[string][]byte) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	for name, data := range files {
		w, err := mw.CreateFormFile(field, name)
		require.NoError(t, err)
		_, _ = w.Write(data)
	}
	require.NoError(t, mw.Close())
	return body, mw.FormDataContentType()
}

func newUploadCtx(t *testing.T, roomID string, body *bytes.Buffer, contentType string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/file/rooms/"+roomID+"/upload/images", body)
	req.Header.Set("Content-Type", contentType)
	c.Request = req
	if roomID != "" {
		c.Params = gin.Params{{Key: "roomId", Value: roomID}}
	}
	if user != nil {
		c.Set(ctxUserKey, user)
	}
	return c, w
}

func okUser() *AuthenticatedUser {
	return &AuthenticatedUser{User: model.User{Account: "alice", EngName: "Alice", ChineseName: "陳"}, Email: "alice@x.com"}
}

func newHandler(store Store, dc driveClient) *Handler {
	return NewHandler(store, dc, &fakeS3{}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true)
}

func TestUpload_MissingRoomID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "", body, ct, okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestUpload_NoUserInContext_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, nil)
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestUpload_NoEmail_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	u := okUser()
	u.Email = ""
	c, w := newUploadCtx(t, "r1", body, ct, u)
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestUpload_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
	env := decodeErr(t, w)
	assert.Equal(t, "forbidden", env.Code)
	assert.Equal(t, "not_room_member", env.Reason)
}

func TestUpload_RoomNotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("", ErrRoomNotFound)
	h := newHandler(store, &fakeDrive{})
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not_found", decodeErr(t, w).Code)
}

func TestUpload_NotMultipart_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newUploadCtx(t, "r1", bytes.NewBufferString("not-multipart"), "text/plain", okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestUpload_TooManyFiles_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	h := NewHandler(store, &fakeDrive{}, &fakeS3{}, 1, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true) // image limit 1
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x"), "b.png": []byte("y")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestUpload_AllRejected_EarlyExit_NoDriveCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{}
	h := newHandler(store, fd)
	// .exe is an invalid type -> rejected in preprocessing.
	body, ct := multipartBody(t, "images", map[string][]byte{"big.exe": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 0, fd.uploadGot.n, "drive must not be called when all files are rejected")
	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "failure", got.Results[0].Status)
	assert.Equal(t, "file has an invalid file type", got.Results[0].Error)
}

func TestUpload_OversizeRejectedPerFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{}
	h := NewHandler(store, fd, &fakeS3{}, testMaxImages, testMaxAttachments, 4, 0, nil, nil, testCacheMaxAge, true) // 4-byte per-image ceiling
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("0123456789")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 0, fd.uploadGot.n, "oversized file must not reach drive")
	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "failure", got.Results[0].Status)
	assert.Equal(t, "file size exceeds limit", got.Results[0].Error)
}

func TestUpload_MixedSuccessAndFailure_Merges(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "img-xyz", GroupID: "r1", Filename: "a.png"}},
		},
	}
	h := newHandler(store, fd)
	// one valid (a.png), one invalid (big.exe).
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x"), "big.exe": []byte("y")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, fd.uploadGot.n, "only the valid file reaches drive")
	assert.Equal(t, "alice", fd.uploadGot.userID)
	assert.Equal(t, "Alice 陳", fd.uploadGot.username)
	assert.Equal(t, "alice@x.com", fd.uploadGot.email)
	assert.Equal(t, "site-x", fd.uploadGot.origin)

	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 2)
	var success, failure uploadResultItem
	for _, r := range got.Results {
		if r.Status == "success" {
			success = r
		} else {
			failure = r
		}
	}
	assert.Equal(t, "a.png", success.Name)
	assert.Equal(t, "api/v1/file/rooms/r1/file/img-xyz?drive_host=https://drive.example.com", success.RelativePath)
	assert.Equal(t, "big.exe", failure.Name)
	assert.Equal(t, "file has an invalid file type", failure.Error)
}

func TestUpload_DriveError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{uploadErr: errors.New("boom")}
	h := newHandler(store, fd)
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

// errEnvelope mirrors the errcode wire shape: {code, reason?, error}.
type errEnvelope struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
	Error  string `json:"error"`
}

func decodeErr(t *testing.T, w *httptest.ResponseRecorder) errEnvelope {
	t.Helper()
	var e errEnvelope
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &e))
	return e
}

func TestHandleHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.HandleHealth(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestHandleUploadImages_SendsUniqueNames_ReturnsOriginals(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "img-1", GroupID: "r1", Filename: "a_1719312000000_0.png"}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"a_1719312000000_0.png"}, fd.uploadGot.filenames, "drive receives the unique name")

	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "success", got.Results[0].Status)
	assert.Equal(t, "a.png", got.Results[0].Name, "response shows the original name")
	assert.Equal(t, "api/v1/file/rooms/r1/file/img-1?drive_host=https://drive.example.com", got.Results[0].RelativePath)
}

// Two files with the SAME name in one batch must get distinct indexed names so
// they don't collide in Drive; both response items keep the original name.
func TestHandleUploadImages_DuplicateNamesInBatch_GetDistinctNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "img-0", GroupID: "r1", Filename: "a_1719312000000_0.png"}},
			{Status: "success", File: drive.GroupImageObject{FileID: "img-1", GroupID: "r1", Filename: "a_1719312000000_1.png"}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	// Two parts under the same field with the same filename.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	for i := 0; i < 2; i++ {
		fw, err := mw.CreateFormFile("images", "a.png")
		require.NoError(t, err)
		_, _ = fw.Write([]byte("x"))
	}
	require.NoError(t, mw.Close())

	c, w := newUploadCtx(t, "r1", body, mw.FormDataContentType(), okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"a_1719312000000_0.png", "a_1719312000000_1.png"}, fd.uploadGot.filenames, "duplicate names get distinct indexed names")

	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 2)
	assert.Equal(t, "a.png", got.Results[0].Name)
	assert.Equal(t, "a.png", got.Results[1].Name)
}

func TestHandleUploadImages_DriveErrorEmptyFilename_KeepsOriginalName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	// Drive reports a per-file failure: status "failure", empty File (so
	// resp.File.Filename == "").
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "failure", Error: "drive exploded", File: drive.GroupImageObject{}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "failure", got.Results[0].Status)
	assert.Equal(t, "drive exploded", got.Results[0].Error)
	assert.Equal(t, "a.png", got.Results[0].Name, "name falls back to original even when drive returns empty filename")
	assert.Empty(t, got.Results[0].RelativePath)
}

func TestHandleUploadFile_SendsUniqueName_ReturnsOriginal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "http://drive",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "f1", GroupID: "r1", Filename: "photo_1719312000000_0.png", FileSize: 3}},
		},
	}
	h := NewHandler(store, fd, &fakeS3{}, 0, testMaxAttachments, 0, 100<<20, newMediaTypeFilter("", "image/svg+xml"), imagePreview, testCacheMaxAge, true)
	h.nowMilli = func() int64 { return 1719312000000 }

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	w, err := mw.CreateFormFile("file", "photo.png")
	require.NoError(t, err)
	_, _ = w.Write([]byte("xxx"))
	require.NoError(t, mw.Close())

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/file/rooms/r1/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	c.Request = req
	c.Params = gin.Params{{Key: "roomId", Value: "r1"}}
	c.Set(ctxUserKey, okUser())

	h.HandleUploadFile(c)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"photo_1719312000000_0.png"}, fd.uploadGot.filenames, "drive receives the unique name")

	var got struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Attachments, 1)
	assert.Equal(t, "photo.png", got.Attachments[0].Title, "response keeps the original name")
}

func Test_uniqueName(t *testing.T) {
	const milli int64 = 1719312000000
	tests := []struct {
		name string
		in   string
		i    int
		want string
	}{
		{"with extension", "photo.png", 0, "photo_1719312000000_0.png"},
		{"uppercase extension", "IMG.JPG", 1, "IMG_1719312000000_1.JPG"},
		{"no extension", "README", 2, "README_1719312000000_2"},
		{"multi dot", "a.tar.gz", 0, "a.tar_1719312000000_0.gz"},
		{"dotfile (filepath.Ext semantics)", ".gitignore", 0, "_1719312000000_0.gitignore"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, uniqueName(tt.in, milli, tt.i))
		})
	}
}

func TestRegisterRoutes_HealthAndAuthGuard(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// devMode=true so the auth middleware doesn't need a validator.
	registerRoutes(r, h, nil, true)

	// healthz is open.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w.Code)

	// the api group rejects a request with no ssoToken header (401).
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/file/rooms/r1/file/f1?drive_host=h", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRegisterRoutes_UploadFilePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, nil, true)

	// New path is registered: with no ssoToken the auth middleware returns 401
	// (NOT 404 — 404 would mean the route does not exist).
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/file/rooms/r1/upload/file", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// Old bare path no longer exists.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/file/rooms/r1/upload", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRegisterRoutes_S3DownloadAuthGuard(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, nil, true)

	// no ssoToken header -> 401 from authMiddleware before the handler runs.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/file-upload/f1/report.pdf", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

func newDownloadCtx(t *testing.T, roomID, fileID, driveHost string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	url := "/api/v1/file/rooms/" + roomID + "/file/" + fileID
	if driveHost != "" {
		url += "?drive_host=" + driveHost
	}
	c.Request = httptest.NewRequest(http.MethodGet, url, nil)
	var params gin.Params
	if roomID != "" {
		params = append(params, gin.Param{Key: "roomId", Value: roomID})
	}
	if fileID != "" {
		params = append(params, gin.Param{Key: "fileId", Value: fileID})
	}
	c.Params = params
	if user != nil {
		c.Set(ctxUserKey, user)
	}
	return c, w
}

func TestDownload_MissingRoomID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_MissingFileID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_MissingDriveHost_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "", okUser())
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_NoUser_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", nil)
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestDownload_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
	env := decodeErr(t, w)
	assert.Equal(t, "forbidden", env.Code)
	assert.Equal(t, "not_room_member", env.Reason)
}

func TestDownload_DriveError_503(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	fd := &fakeDrive{getErr: errors.New("image not found")}
	h := newHandler(store, fd)
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

func newS3DownloadCtx(t *testing.T, fileID, fileName string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/file-upload/"+fileID+"/"+fileName, nil)
	var params gin.Params
	if fileID != "" {
		params = append(params, gin.Param{Key: "fileId", Value: fileID})
	}
	if fileName != "" {
		params = append(params, gin.Param{Key: "fileName", Value: fileName})
	}
	c.Params = params
	if user != nil {
		c.Set(ctxUserKey, user)
	}
	return c, w
}

func TestContentDisposition(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "utf8 and space", in: "réport space.pdf", want: "attachment; filename*=UTF-8''r%C3%A9port%20space.pdf"},
		{name: "simple", in: "x.pdf", want: "attachment; filename*=UTF-8''x.pdf"},
		{name: "empty", in: "", want: "attachment"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, contentDisposition(tc.in))
		})
	}
}

func sampleUpload() *upload {
	up := &upload{ID: "f1", UserID: "u1", RID: "r1", Name: "réport space.pdf", Type: "application/pdf", Size: 7}
	up.AmazonS3.Path = "app-001/uploads/r1/u1/f1"
	return up
}

func TestS3Download_MissingFileID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newS3DownloadCtx(t, "", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestS3Download_NoUser_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", nil)
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestS3Download_UploadNotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(nil, ErrUploadNotFound)
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not_found", decodeErr(t, w).Code)
}

func TestS3Download_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(nil, errors.New("boom"))
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestS3Download_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
	env := decodeErr(t, w)
	assert.Equal(t, "forbidden", env.Code)
	assert.Equal(t, "not_room_member", env.Reason)
}

func TestS3Download_S3Error_503(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	h := NewHandler(store, &fakeDrive{}, &fakeS3{err: errors.New("no such key")}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

func TestS3Download_Success_StreamsWithHeaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	s3 := &fakeS3{body: "PDFDATA"}
	h := NewHandler(store, &fakeDrive{}, s3, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "PDFDATA", w.Body.String())
	assert.Equal(t, "app-001/uploads/r1/u1/f1", s3.gotKey)
	assert.Equal(t, "application/pdf", w.Header().Get("Content-Type"))
	assert.Equal(t, "7", w.Header().Get("Content-Length"))
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control")) // private + configured testCacheMaxAge
	// RFC 5987: encodeURIComponent-style, spaces as %20 (not +).
	assert.Equal(t, "attachment; filename*=UTF-8''r%C3%A9port%20space.pdf", w.Header().Get("Content-Disposition"))
}

func TestS3Download_EmptyType_DefaultsOctetStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	up := sampleUpload()
	up.Type = ""
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(up, nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	h := NewHandler(store, &fakeDrive{}, &fakeS3{body: "PDFDATA"}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
}

func TestDownload_Success_StreamsBinary(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	fd := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader:        readCloser{strings.NewReader("PNGDATA")},
		ContentType:   "image/png",
		ContentLength: 7,
		Filename:      "réport space.png",
	}}
	h := newHandler(store, fd)
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	assert.Equal(t, "PNGDATA", w.Body.String())
	assert.Equal(t, "https://d.example.com", fd.getGot.host)
	assert.Equal(t, "r1", fd.getGot.groupID)
	assert.Equal(t, "f1", fd.getGot.fileID)
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control"))
	assert.Equal(t, "attachment; filename*=UTF-8''r%C3%A9port%20space.png", w.Header().Get("Content-Disposition"))
}

func TestDownload_Success_NoFilename(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	fd := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader:        readCloser{strings.NewReader("PNGDATA")},
		ContentType:   "image/png",
		ContentLength: 7,
	}}
	h := newHandler(store, fd)
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "attachment", w.Header().Get("Content-Disposition"))
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control"))
}

// multipartTyped builds a one-file multipart body with an explicit part
// Content-Type (CreateFormFile would force application/octet-stream).
func multipartTyped(t *testing.T, field, filename string, data []byte, mime string, fields map[string]string) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	hdr := make(textproto.MIMEHeader)
	hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, filename))
	hdr.Set("Content-Type", mime)
	w, err := mw.CreatePart(hdr)
	require.NoError(t, err)
	_, err = w.Write(data)
	require.NoError(t, err)
	for k, v := range fields {
		require.NoError(t, mw.WriteField(k, v))
	}
	require.NoError(t, mw.Close())
	return body, mw.FormDataContentType()
}

func fileHandler(store Store, fd *fakeDrive) *Handler {
	return NewHandler(store, fd, &fakeS3{}, 0, testMaxAttachments, 0, 100<<20, newMediaTypeFilter("", "image/svg+xml"), imagePreview, testCacheMaxAge, true)
}

func okFileDrive() *fakeDrive {
	return &fakeDrive{baseURL: "http://drive", uploadResp: []drive.UploadGroupImageResponse{
		{Status: driveStatusSuccess, File: drive.GroupImageObject{FileID: "drive-file-1", GroupID: "room-1", Filename: "report.pdf", FileSize: 2048}},
	}}
}

func TestHandleUploadFile_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	body, ct := multipartTyped(t, "file", "report.pdf", []byte("pdfbytes"), "application/pdf", map[string]string{"description": "Q2"})
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Success     bool               `json:"success"`
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.Success)
	require.Len(t, resp.Attachments, 1)
	assert.Equal(t, "drive-file-1", resp.Attachments[0].ID)
	assert.Equal(t, "report.pdf", resp.Attachments[0].Title)
	assert.Equal(t, "file", resp.Attachments[0].Type)
	assert.Equal(t, "Q2", resp.Attachments[0].Description)
	assert.Contains(t, resp.Attachments[0].TitleLink, "drive-file-1")
	assert.NotContains(t, w.Body.String(), `"message"`)
}

func TestHandleUploadFile_ImageSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	body, ct := multipartTyped(t, "file", "photo.png", makePNG(t, 64, 48), "image/png", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Attachments, 1)
	att := resp.Attachments[0]
	assert.NotEmpty(t, att.ImageURL)
	assert.Equal(t, "image/png", att.ImageType)
	assert.NotEmpty(t, att.ImagePreview)
	require.NotNil(t, att.ImageDimensions)
	assert.Equal(t, 64, att.ImageDimensions.Width)
	assert.Equal(t, 48, att.ImageDimensions.Height)
}

func TestHandleUploadFile_NotMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(false, nil)
	body, ct := multipartTyped(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestHandleUploadFile_RoomNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("", ErrRoomNotFound)
	body, ct := multipartTyped(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleUploadFile_BlockedMIME(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	body, ct := multipartTyped(t, "file", "x.svg", []byte("<svg/>"), "image/svg+xml", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUploadFile_OverSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	h := NewHandler(store, &fakeDrive{baseURL: "http://drive"}, &fakeS3{}, 0, testMaxAttachments, 0, 4, newMediaTypeFilter("", ""), imagePreview, testCacheMaxAge, true)
	body, ct := multipartTyped(t, "file", "big.pdf", []byte("morethan4"), "application/pdf", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	h.HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUploadFile_DriveError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	fd := &fakeDrive{baseURL: "http://drive", uploadErr: fmt.Errorf("drive boom")}
	h := fileHandler(store, fd)
	body, ct := multipartTyped(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	h.HandleUploadFile(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleUploadFile_DriveStatusFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	fd := &fakeDrive{baseURL: "http://drive", uploadResp: []drive.UploadGroupImageResponse{
		{Status: "failure", Error: "quota exceeded"},
	}}
	h := fileHandler(store, fd)
	body, ct := multipartTyped(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	h.HandleUploadFile(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

func TestRoute_UploadRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, &Handler{}, nil, true)
	found := false
	for _, ri := range r.Routes() {
		if ri.Method == http.MethodPost && ri.Path == "/api/v1/file/rooms/:roomId/upload/file" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestRoute_SetCookieRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, &Handler{}, nil, true)
	found := false
	for _, ri := range r.Routes() {
		if ri.Method == http.MethodPost && ri.Path == "/api/v1/file/setCookie" {
			found = true
		}
	}
	assert.True(t, found)
}

// multipartFileParts builds a multipart body with n parts all under the "file"
// field, so the single-file endpoint's attachment-count check can be exercised.
func multipartFileParts(t *testing.T, n int) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	for i := 0; i < n; i++ {
		hdr := make(textproto.MIMEHeader)
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="report-%d.pdf"`, i))
		hdr.Set("Content-Type", "application/pdf")
		w, err := mw.CreatePart(hdr)
		require.NoError(t, err)
		_, err = w.Write([]byte("pdfbytes"))
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())
	return body, mw.FormDataContentType()
}

func TestHandleUploadFile_TooManyFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	fd := okFileDrive()
	h := fileHandler(store, fd) // maxAttachments == testMaxAttachments == 1

	body, ct := multipartFileParts(t, 2) // two files, over the limit of 1
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	h.HandleUploadFile(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
	assert.Equal(t, 0, fd.uploadGot.n, "over-limit upload must not reach drive")
}

func TestHandleUploadFile_MissingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	body, ct := multipartTyped(t, "other", "x.txt", []byte("x"), "text/plain", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandler_HandleSetCookie_SetsCookieAttributes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{setCookiePartitioned: true}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/file/setCookie", nil)
	c.Request.Header.Set("ssoToken", "jwt-abc")

	h.HandleSetCookie(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())

	setCookie := w.Header().Get("Set-Cookie")
	require.NotEmpty(t, setCookie)
	assert.Contains(t, setCookie, "ssoToken=jwt-abc")
	assert.Contains(t, setCookie, "Path=/")
	assert.Contains(t, setCookie, "HttpOnly")
	assert.Contains(t, setCookie, "Secure")
	assert.Contains(t, setCookie, "SameSite=None")
	assert.Contains(t, setCookie, "Partitioned")
}

func TestHandler_HandleSetCookie_FallsBackToCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/file/setCookie", nil)
	c.Request.Header.Set("Cookie", "ssoToken=cookie-jwt")

	h.HandleSetCookie(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Set-Cookie"), "ssoToken=cookie-jwt")
}

// HandleSetCookie does no validation of its own — authMiddleware gates the route and
// rejects a missing token before this runs. Called with neither header nor cookie it
// still returns 200 and sets an empty-valued cookie; lock in that passthrough behavior.
func TestHandler_HandleSetCookie_NoToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/file/setCookie", nil)

	h.HandleSetCookie(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Set-Cookie"), "ssoToken=;")
}
