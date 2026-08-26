package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/session"
)

// fakeUploader stands in for the real forwarder: it records the parts it was
// handed and returns a scripted result.
type fakeUploader struct {
	called bool
	names  uploadedNames
	err    error
	// drained is what the fake actually read from the stream, proving the
	// handler passes a live reader rather than a spent one.
	drained map[string]string
}

func (f *fakeUploader) Forward(_ context.Context, src *multipart.Reader) (uploadedNames, error) {
	f.called = true
	f.drained = map[string]string{}
	for {
		part, err := src.NextPart()
		if err != nil {
			break
		}
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(part)
		f.drained[part.FormName()] = buf.String()
		_ = part.Close()
	}
	return f.names, f.err
}

// uploadRequest builds a POST carrying the two artifact parts.
func uploadRequest(t *testing.T) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, p := range []struct{ field, name, content string }{
		{configFileField, "app.yaml", "version: 4"},
		{executeFileField, "app.exe", "MZbinary"},
	} {
		w, err := mw.CreateFormFile(p.field, p.name)
		require.NoError(t, err)
		_, err = w.Write([]byte(p.content))
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/client-update/version", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// uploadRouter mounts just the upload handler with an already-authenticated
// principal, so these tests exercise the handler rather than requireAdmin
// (which middleware_test.go already covers).
func uploadRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/admin/client-update/version", func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			UserID: "u-1", Account: "admin1", SiteID: "site-local",
			Roles: []string{"admin"},
		})
		c.Next()
	}, h.uploadClientVersion)
	return r
}

func TestUploadClientVersion_ForwardsAndAudits(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	var audited *AuditEntry
	store.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, e *AuditEntry) error { audited = e; return nil })

	up := &fakeUploader{names: uploadedNames{ConfigFile: "app.yaml", ExecuteFile: "app.exe"}}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var got map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "success", got["result"])

	assert.True(t, up.called)
	assert.Equal(t, "version: 4", up.drained[configFileField],
		"the handler must hand the forwarder a live stream, not a consumed one")
	assert.Equal(t, "MZbinary", up.drained[executeFileField])

	require.NotNil(t, audited, "a successful upload must be audited")
	assert.Equal(t, auditActionClientUpdateUpload, audited.Action)
	assert.Equal(t, "admin1", audited.ActorAccount)
	assert.Equal(t, "app.yaml", audited.Details["configFile"])
	assert.Equal(t, "app.exe", audited.Details["executeFile"])
}

func TestUploadClientVersion_ForwardErrorIsNotAudited(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)
	// No AppendAudit expectation: a failed upload must not be recorded as one.

	up := &fakeUploader{err: errcode.Unavailable("client-update-service is unreachable",
		errcode.WithReason(errcode.AdminUpstreamUnavailable))}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var env map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.AdminUpstreamUnavailable), env["reason"])
}

func TestUploadClientVersion_RejectsNonMultipart(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	up := &fakeUploader{}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/client-update/version",
		bytes.NewBufferString(`{"not":"multipart"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, up.called, "a non-multipart body must be refused before forwarding")
}

// TestUploadClientVersion_UnconfiguredIsUnavailable covers the nil-forwarder
// path, which every existing newHandler call site in this package produces.
func TestUploadClientVersion_UnconfiguredIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil)

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestExtendDeadlines_ToleratesUnsupportedWriter proves the middleware is a
// no-op rather than a failure on a ResponseWriter that cannot take deadlines
// (httptest's recorder), so unit tests of every other route stay unaffected.
func TestExtendDeadlines_ToleratesUnsupportedWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var reached bool
	r.GET("/probe", extendDeadlines(time.Minute), func(c *gin.Context) {
		reached = true
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/probe", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached)
}
