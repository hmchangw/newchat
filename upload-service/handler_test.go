package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
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
	testMaxFiles           = 10
	testMaxImageSize int64 = 25 << 20
)

// fakeDrive implements driveClient for handler tests.
type fakeDrive struct {
	uploadResp []drive.UploadGroupImageResponse
	uploadErr  error
	uploadGot  struct {
		userID, username, email, groupID, origin string
		n                                        int
	}

	getResp *drive.GetGroupImageResponse
	getErr  error
	getGot  struct{ host, groupID, fileID string }

	baseURL string
}

func (f *fakeDrive) UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error) {
	f.uploadGot.userID, f.uploadGot.username, f.uploadGot.email = userID, username, email
	f.uploadGot.groupID, f.uploadGot.origin, f.uploadGot.n = groupID, origin, len(files)
	return f.uploadResp, f.uploadErr
}
func (f *fakeDrive) GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error) {
	f.getGot.host, f.getGot.groupID, f.getGot.fileID = host, groupID, fileID
	return f.getResp, f.getErr
}
func (f *fakeDrive) GetBaseURLFromRoomOrigin(string) string { return f.baseURL }

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
	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/"+roomID+"/upload/images", body)
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
	return NewHandler(store, dc, testMaxFiles, testMaxImageSize)
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
	h := NewHandler(store, &fakeDrive{}, 1, testMaxImageSize) // limit 1
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
	h := NewHandler(store, fd, testMaxFiles, 4) // 4-byte per-image ceiling
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
			{Status: "Success", File: drive.GroupImageObject{FileID: "img-xyz", GroupID: "r1", Filename: "a.png"}},
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
		if r.Status == "Success" {
			success = r
		} else {
			failure = r
		}
	}
	assert.Equal(t, "a.png", success.Name)
	assert.Equal(t, "api/v1/rooms/r1/image/img-xyz?drive_host=https://drive.example.com", success.RelativePath)
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
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/rooms/r1/image/f1?drive_host=h", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

func newDownloadCtx(t *testing.T, roomID, fileID, driveHost string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	url := "/api/v1/rooms/" + roomID + "/image/" + fileID
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
	h.HandleDownloadImage(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_MissingFileID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "", "https://d.example.com", okUser())
	h.HandleDownloadImage(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_MissingDriveHost_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "", okUser())
	h.HandleDownloadImage(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownload_NoUser_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", nil)
	h.HandleDownloadImage(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestDownload_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadImage(c)
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
	h.HandleDownloadImage(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

func TestDownload_Success_StreamsBinary(t *testing.T) {
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
	h.HandleDownloadImage(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	assert.Equal(t, "PNGDATA", w.Body.String())
	assert.Equal(t, "https://d.example.com", fd.getGot.host)
	assert.Equal(t, "r1", fd.getGot.groupID)
	assert.Equal(t, "f1", fd.getGot.fileID)
}
