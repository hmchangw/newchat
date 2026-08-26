package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func init() { gin.SetMode(gin.TestMode) }

// testCache returns an enabled cache with a small object cap so "too large" is easy to hit.
func testCache(maxObjectBytes int64) *blobCache {
	return newBlobCache(4, time.Hour, maxObjectBytes)
}

func rc(s string) io.ReadCloser { return io.NopCloser(strings.NewReader(s)) }

func bytesBufferString(s string) *bytes.Buffer { return bytes.NewBufferString(s) }

// fileSpec describes one multipart file part. contentType, when empty, means the
// part is written with no Content-Type header (exercises the upload fallback).
type fileSpec struct {
	name        string
	content     string
	contentType string
}

// multipartBody builds a multipart form (omit a field by leaving it out). Parts use
// CreatePart so Content-Type is set only when declared (CreateFormFile forces octet-stream).
func multipartBody(t *testing.T, files map[string]fileSpec) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	for field, fs := range files {
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, fs.name))
		if fs.contentType != "" {
			hdr.Set("Content-Type", fs.contentType)
		}
		fw, err := w.CreatePart(hdr)
		require.NoError(t, err)
		_, err = io.WriteString(fw, fs.content)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return body, w.FormDataContentType()
}

func uploadCtx(t *testing.T, body *bytes.Buffer, contentType string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	c.Request.Header.Set("Content-Type", contentType)
	return c, w
}

func downloadCtx(t *testing.T, fileName string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/version/"+fileName, nil)
	c.Params = gin.Params{{Key: "fileName", Value: fileName}}
	return c, w
}

func TestHandleHealth(t *testing.T) {
	h := NewHandler(nil, testCache(1024))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.HandleHealth(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"status":"ok"}`, w.Body.String())
}

// testTokens is the token table every route-level test authenticates against.
func testTokens() map[string]string {
	return map[string]string{"admin-service": "0123456789abcdef"}
}

// TestRoutesRegistered proves all three method+path pairs are wired, so removing
// any one route fails the test.
func TestRoutesRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h, testTokens())

	got := map[string]bool{}
	for _, ri := range r.Routes() {
		got[ri.Method+" "+ri.Path] = true
	}
	for _, want := range []string{
		"GET /healthz",
		"POST /api/v1/version",
		"GET /api/v1/version/:fileName",
	} {
		assert.True(t, got[want], "route %q must be registered", want)
	}

	// And the health route actually responds.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRoutes_UploadRequiresServiceAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// No EXPECT() at all: gomock fails the test if Put is called, which is
	// exactly the assertion — an unauthenticated upload must never reach MinIO.
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", ct)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRoutes_UploadSucceedsWithServiceAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(6), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(4), "application/octet-stream").Return(nil)
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// The client fleet pulling updates holds no credential, so downloads stay open.
// This pins that asymmetry: gating GET would break every deployed client.
func TestRoutes_DownloadStaysUnauthenticated(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.yaml")).
		Return(rc("config"), blobInfo{Size: 6, ContentType: "application/x-yaml"}, nil)
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version/app.yaml", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
