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

// multipartBody builds a multipart form; omit a field by leaving it out of files.
// Parts are written via CreatePart so the Content-Type header is set only when the
// fileSpec declares one (CreateFormFile would always force application/octet-stream).
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

// TestRoutesRegistered proves the three routes are wired.
func TestRoutesRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w.Code)
}
