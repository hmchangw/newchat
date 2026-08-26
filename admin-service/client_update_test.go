package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/session"
)

func TestMapUpstreamStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantNil  bool
		wantCode errcode.Code
		wantMsg  string
	}{
		{
			name: "200 is success", status: 200, body: `{"result":"success"}`, wantNil: true,
		},
		{
			name: "400 relays the upstream message", status: 400,
			body:     `{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}`,
			wantCode: errcode.CodeBadRequest,
			wantMsg:  "configFile must be a .yaml or .yml file",
		},
		{
			name: "400 with an unparseable body falls back", status: 400, body: "not json",
			wantCode: errcode.CodeBadRequest,
			wantMsg:  "client update service rejected the upload",
		},
		{
			// Our own credential is bad. Relaying 401 would read to the admin as
			// an expired session and send them to a pointless re-login.
			name: "401 becomes unavailable, not unauthenticated", status: 401,
			body:     `{"code":"unauthenticated","error":"invalid or missing service account token"}`,
			wantCode: errcode.CodeUnavailable,
		},
		{
			name: "403 becomes unavailable", status: 403, body: "{}",
			wantCode: errcode.CodeUnavailable,
		},
		{
			name: "500 becomes unavailable", status: 500, body: "{}",
			wantCode: errcode.CodeUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapUpstreamStatus(tt.status, tt.body)
			if tt.wantNil {
				assert.NoError(t, err)
				return
			}
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tt.wantCode, ec.Code)
			if tt.wantMsg != "" {
				assert.Equal(t, tt.wantMsg, ec.Message)
			}
		})
	}
}

// The upstream's reason is a contract between client-update-service and its own
// clients. Re-emitting it would put codes into admin-service's surface that
// docs/client-api.md §9 does not document.
func TestMapUpstreamStatus_DoesNotCopyUpstreamReason(t *testing.T) {
	err := mapUpstreamStatus(400, `{"code":"bad_request","reason":"some_upstream_reason","error":"nope"}`)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Empty(t, ec.Reason)
}

func TestRestyVersionUploader_PostsBodyAndCredential(t *testing.T) {
	var gotAuth, gotContentType, gotBody, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer srv.Close()

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(30*time.Second)))

	err := u.Upload(context.Background(), "multipart/form-data; boundary=xyz", strings.NewReader("PAYLOAD"))

	require.NoError(t, err)
	assert.Equal(t, "Bearer 0123456789abcdef", gotAuth)
	assert.Equal(t, "multipart/form-data; boundary=xyz", gotContentType)
	assert.Equal(t, "PAYLOAD", gotBody)
	assert.Equal(t, clientUpdateVersionPath, gotPath)
}

// resty buffers an entire io.Reader body into memory when SetContentLength is
// on (resty v2.17.2 middleware.go:519-527), which would defeat streaming for a
// multi-hundred-MB artifact. This pins that it stays off.
func TestRestyVersionUploader_StreamsWithoutContentLength(t *testing.T) {
	const size = 4 << 20 // 4 MiB — larger than any internal buffer
	var gotLen int
	var gotTransferEncoding []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTransferEncoding = r.TransferEncoding
		b, _ := io.ReadAll(r.Body)
		gotLen = len(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(30*time.Second)))

	err := u.Upload(context.Background(), "application/octet-stream",
		io.LimitReader(zeroReader{}, size))

	require.NoError(t, err)
	assert.Equal(t, size, gotLen, "the whole body must arrive intact")
	assert.Contains(t, gotTransferEncoding, "chunked",
		"a streamed body of unknown length must be chunked, not buffered and measured")
}

// zeroReader is an endless source of zero bytes; pair it with io.LimitReader.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestRestyVersionUploader_TransportFailureIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // closed: every request fails at the transport

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(2*time.Second)))

	err := u.Upload(context.Background(), "application/octet-stream", strings.NewReader("x"))

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
}

// fakeUploader records what the handler streamed and returns a scripted verdict.
type fakeUploader struct {
	mu          sync.Mutex
	called      bool
	contentType string
	body        []byte
	err         error
}

func (f *fakeUploader) Upload(_ context.Context, contentType string, body io.Reader) error {
	b, readErr := io.ReadAll(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.contentType = contentType
	f.body = b
	if f.err != nil {
		return f.err
	}
	if readErr != nil {
		return readErr
	}
	return nil
}

func (f *fakeUploader) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// recordingAuditStore captures audit writes; every other AdminStore method is
// left nil so an unexpected call panics loudly.
type recordingAuditStore struct {
	AdminStore
	mu      sync.Mutex
	entries []AuditEntry
}

func (r *recordingAuditStore) AppendAudit(_ context.Context, e *AuditEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, *e)
	return nil
}

func (r *recordingAuditStore) audited() []AuditEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]AuditEntry(nil), r.entries...)
}

// uploadTestCfg is a Config with only what the upload handler reads.
func uploadTestCfg() Config {
	return Config{
		SiteID:              "site-A",
		ClientUpdateURL:     "http://client-update-service:8080",
		ClientUpdateToken:   "0123456789abcdef",
		ClientUpdateTimeout: 10 * time.Minute,
	}
}

// sessionForTest is the admin principal the stub middleware injects.
func sessionForTest() session.Session {
	return session.Session{
		ID:      "sess-1",
		UserID:  "admin-user-id",
		Account: "p_admin",
		SiteID:  "site-A",
		Roles:   []string{"admin"},
	}
}

// uploadTestServer builds the real router (base middleware + a stubbed admin
// principal) around a live server, so deadlines and streaming behave as in prod.
func uploadTestServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	applyBaseMiddleware(r, nil)
	r.POST("/v1/admin/client-updates", func(c *gin.Context) {
		c.Set(ctxPrincipal, sessionForTest())
		c.Next()
	}, h.uploadClientVersion)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

// uploadBody builds a multipart payload; a part with an empty contentType is
// written with no Content-Type header at all.
func uploadBody(t *testing.T, parts map[string]struct{ filename, content, contentType string }) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	for field, p := range parts {
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, p.filename))
		if p.contentType != "" {
			hdr.Set("Content-Type", p.contentType)
		}
		fw, err := w.CreatePart(hdr)
		require.NoError(t, err)
		_, err = io.WriteString(fw, p.content)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return body, w.FormDataContentType()
}

func postUpload(t *testing.T, srv *httptest.Server, body *bytes.Buffer, contentType string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestUploadClientVersion_Success(t *testing.T) {
	up := &fakeUploader{}
	store := &recordingAuditStore{}
	h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, up.wasCalled())
	assert.Contains(t, up.contentType, "multipart/form-data; boundary=")

	entries := store.audited()
	require.Len(t, entries, 1)
	assert.Equal(t, clientUpdateAuditAction, entries[0].Action)
	assert.Equal(t, "app.yaml", entries[0].Details["configFile"])
	assert.Equal(t, "app.exe", entries[0].Details["executeFile"])
}

// client-update-service picks a stored object's content type from the part's own
// header, falling back only when there is none. Re-encoding with CreateFormFile
// would stamp application/octet-stream on every part and silently change what
// the .yaml is stored as, so the relay must copy the header through verbatim.
func TestUploadClientVersion_PreservesPartContentTypes(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup
	require.Equal(t, http.StatusOK, resp.StatusCode)

	relayed := parseRelayed(t, up.body, up.contentType)
	assert.Equal(t, "text/yaml", relayed["configFile"].contentType,
		"a declared part Content-Type must survive the relay")
	assert.Empty(t, relayed["executeFile"].contentType,
		"a part with no Content-Type must stay bare so the upstream fallback applies")
	assert.Equal(t, "app.yaml", relayed["configFile"].filename)
	assert.Equal(t, "version: 1", relayed["configFile"].content)
	assert.Equal(t, "MZbinary", relayed["executeFile"].content)
}

// relayedPart is one part as it arrived at the uploader.
type relayedPart struct{ filename, content, contentType string }

func parseRelayed(t *testing.T, body []byte, contentType string) map[string]relayedPart {
	t.Helper()
	_, params, err := mime.ParseMediaType(contentType)
	require.NoError(t, err)
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	out := map[string]relayedPart{}
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		content, err := io.ReadAll(p)
		require.NoError(t, err)
		out[p.FormName()] = relayedPart{
			filename:    p.FileName(),
			content:     string(content),
			contentType: p.Header.Get("Content-Type"),
		}
		require.NoError(t, p.Close())
	}
	return out
}

// A body far larger than any internal buffer must arrive intact — proof the
// relay streams rather than truncating at a buffer boundary.
func TestUploadClientVersion_StreamsLargeBodyIntact(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	big := strings.Repeat("A", size)
	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", big, ""},
	})
	resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup
	require.Equal(t, http.StatusOK, resp.StatusCode)

	relayed := parseRelayed(t, up.body, up.contentType)
	assert.Len(t, relayed["executeFile"].content, size)
	assert.Equal(t, big, relayed["executeFile"].content)
}

func TestUploadClientVersion_UpstreamErrorsAreMapped(t *testing.T) {
	tests := []struct {
		name       string
		uploadErr  error
		wantStatus int
	}{
		{"upstream rejects the files", errcode.BadRequest("configFile must be a .yaml or .yml file"), http.StatusBadRequest},
		{"our credential is wrong", errcode.Unavailable("client update upload is misconfigured"), http.StatusServiceUnavailable},
		{"upstream is down", errcode.Unavailable("client update service is unavailable"), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &fakeUploader{err: tt.uploadErr}
			store := &recordingAuditStore{}
			h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
			srv := uploadTestServer(t, h)

			body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
				"configFile":  {"app.yaml", "version: 1", ""},
				"executeFile": {"app.exe", "MZ", ""},
			})
			resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Empty(t, store.audited(),
				"a failed upload must not be recorded as a published artifact")
		})
	}
}

func TestUploadClientVersion_NonMultipartIsBadRequest(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", strings.NewReader(`{"not":"multipart"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, up.wasCalled(), "nothing may be sent upstream before the body is known to be multipart")
}

func TestUploadClientVersion_UnconfiguredUploaderIsUnavailable(t *testing.T) {
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil)
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile": {"app.yaml", "version: 1", ""},
	})
	resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// The handler pushes its own read/write deadlines past the server's 15s/40s.
// If a middleware ever wraps c.Writer in a type without Unwrap,
// http.NewResponseController stops reaching the connection and this fails —
// which is the point: a silent no-op would kill real uploads at 15s.
func TestUploadClientVersion_ExtendsItsOwnDeadlines(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	resp := postUpload(t, srv, body, ct) //nolint:bodyclose // postUpload closes the body via t.Cleanup

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a 500 here means SetReadDeadline/SetWriteDeadline returned ErrNotSupported")
}
