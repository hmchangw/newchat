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
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/testutil"
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

// The default branch (an unexpected upstream status) is the only place a
// non-2xx body reaches our logs at all today. Without an echo of it, an
// operator debugging a 500 from client-update-service sees only the status
// code and nothing else.
func TestMapUpstreamStatus_DefaultBranchLogsUpstreamBody(t *testing.T) {
	err := mapUpstreamStatus(500, `{"error":"minio is down"}`)
	require.Error(t, err)
	cause := errors.Unwrap(err)
	require.Error(t, cause)
	assert.Contains(t, cause.Error(), "minio is down")
}

// The upstream body is capped so a large error page cannot bloat a log line.
// A 5xx from an ingress or proxy carries arbitrary content, and CLAUDE.md forbids
// wrapping a raw body into a cause. Only the upstream's own envelope message may
// reach the log, so a non-envelope body must contribute nothing but the status.
func TestMapUpstreamStatus_DefaultBranchKeepsRawBodyOutOfTheCause(t *testing.T) {
	rawBody := strings.Repeat("x", 1000)
	err := mapUpstreamStatus(500, rawBody)
	require.Error(t, err)
	cause := errors.Unwrap(err)
	require.Error(t, cause)

	assert.NotContains(t, cause.Error(), "xxx", "a non-envelope body must not reach the cause")
	assert.Contains(t, cause.Error(), "500")
}

func TestMapUpstreamStatus_DefaultBranchLogsTheEnvelopeMessageTruncated(t *testing.T) {
	longMessage := strings.Repeat("m", 1000)
	body := `{"code":"internal","error":"` + longMessage + `"}`

	err := mapUpstreamStatus(500, body)
	require.Error(t, err)
	cause := errors.Unwrap(err)
	require.Error(t, cause)

	assert.Contains(t, cause.Error(), "500")
	assert.Contains(t, cause.Error(), "mmm", "the upstream's own message is useful to an operator")
	assert.Contains(t, cause.Error(), "truncated")
	assert.Less(t, len(cause.Error()), len(longMessage))
}

// A 401/403 body may quote a credential rejection — it must never reach the
// cause, unlike the default branch above.
func TestMapUpstreamStatus_401And403StayBodyFree(t *testing.T) {
	for _, status := range []int{401, 403} {
		err := mapUpstreamStatus(status, `{"error":"credential xyz123 rejected"}`)
		require.Error(t, err)
		cause := errors.Unwrap(err)
		require.Error(t, cause)
		assert.NotContains(t, cause.Error(), "credential xyz123 rejected")
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

func TestHTTPVersionUploader_PostsBodyAndCredential(t *testing.T) {
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

	u := newHTTPVersionUploader(srv.URL, "0123456789abcdef", 30*time.Second)

	err := u.Upload(context.Background(), &uploadBody{
		reader: strings.NewReader("PAYLOAD"), contentType: "multipart/form-data; boundary=xyz", length: int64(len("PAYLOAD")),
	})

	require.NoError(t, err)
	assert.Equal(t, "Bearer 0123456789abcdef", gotAuth)
	assert.Equal(t, "multipart/form-data; boundary=xyz", gotContentType)
	assert.Equal(t, "PAYLOAD", gotBody)
	assert.Equal(t, clientUpdateVersionPath, gotPath)
}

// The body must go out with an exact Content-Length rather than chunked: the
// length is summed while the reader chain is assembled precisely so net/http
// does not have to guess, and client-update-service gets a declared size.
func TestHTTPVersionUploader_SendsExactContentLength(t *testing.T) {
	const size = 4 << 20 // larger than any internal buffer
	var declared, received int64
	var transferEncoding []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		declared = r.ContentLength
		transferEncoding = r.TransferEncoding
		received, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := newHTTPVersionUploader(srv.URL, "0123456789abcdef", 30*time.Second)

	err := u.Upload(context.Background(), testBody(io.LimitReader(zeroReader{}, size), size))

	require.NoError(t, err)
	assert.Equal(t, int64(size), received, "the whole body must arrive intact")
	assert.Equal(t, int64(size), declared, "Content-Length must be declared, not inferred")
	assert.Empty(t, transferEncoding, "a body with a known length must not be chunked")
}

// testBody wraps a plain reader as an uploadBody for the uploader-level tests,
// which exercise transport behaviour rather than body assembly.
func testBody(r io.Reader, length int64) *uploadBody {
	return &uploadBody{reader: r, contentType: "application/octet-stream", length: length}
}

// zeroReader is an endless source of zero bytes; pair it with io.LimitReader.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestHTTPVersionUploader_UpstreamTimeoutSaysSo(t *testing.T) {
	// An upstream that accepts the body and then never answers: the budget runs
	// out on the outbound half rather than the inbound one.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)

	u := newHTTPVersionUploader(srv.URL, "0123456789abcdef", 200*time.Millisecond)

	err := u.Upload(context.Background(), testBody(strings.NewReader("x"), 1))

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Contains(t, ec.Message, "time",
		"a budget that expired waiting on the upstream must not read as an unreachable upstream")
	assert.NotContains(t, ec.Message, "unavailable")
}

func TestHTTPVersionUploader_TransportFailureIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // closed: every request fails at the transport

	u := newHTTPVersionUploader(srv.URL, "0123456789abcdef", 2*time.Second)

	err := u.Upload(context.Background(), testBody(strings.NewReader("x"), 1))

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
	length      int64
	err         error
}

func (f *fakeUploader) Upload(_ context.Context, body *uploadBody) error {
	contentType := body.contentType
	b, readErr := io.ReadAll(body.reader)
	f.length = body.length
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.contentType = contentType
	f.body = b
	if f.err != nil {
		return f.err
	}
	if readErr != nil {
		// Mirrors restyVersionUploader: a failure draining the body surfaces as
		// this service's own Unavailable, never as the pipe's raw error. The
		// handler's errors.Is guard depends on that — an uploader that echoed the
		// pipe error back verbatim would make a client-side fault indistinguishable
		// from an upstream one.
		return errcode.Unavailable("client update service is unavailable", errcode.WithCause(readErr))
	}
	return nil
}

func (f *fakeUploader) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// relayedContentType and relayedBody read what Upload recorded under the mutex
// that guards it. The HTTP round-trip happens to order the goroutines today, but
// reading the fields bare is still a race the detector is entitled to catch.
func (f *fakeUploader) relayedContentType() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.contentType
}

func (f *fakeUploader) relayedBody() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.body...)
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
		SiteID:                     "site-A",
		ClientUpdateURL:            "http://client-update-service:8080",
		ClientUpdateToken:          "0123456789abcdef",
		ClientUpdateTimeout:        10 * time.Minute,
		ClientUpdateMaxUploadBytes: 2 << 30,
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

// uploadTestServer builds the real router (base middleware, including the real
// o11ygin chain wired the same way main.go does, + a stubbed admin principal)
// around a live server, so deadlines and streaming behave as in prod. In
// particular, this exercises whatever c.Writer wrapping o11ygin/otelgin do —
// see TestUploadClientVersion_ExtendsItsOwnDeadlines.
func uploadTestServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.MaxMultipartMemory = maxMultipartMemory // mirror main.go, or the spill threshold differs
	obsMW := o11ygin.Middleware("admin-service", tracenoop.NewTracerProvider(), noop.NewMeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())
	applyBaseMiddleware(r, obsMW)
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
func multipartPayload(t *testing.T, parts map[string]struct{ filename, content, contentType string }) (*bytes.Buffer, string) {
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

// postUpload returns the status and the response body, closing the body here so
// callers neither can nor need to. Returning *http.Response instead would leave
// every caller looking like a leak to bodyclose.
func postUpload(t *testing.T, srv *httptest.Server, body *bytes.Buffer, contentType string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, respBody
}

func TestUploadClientVersion_Success(t *testing.T) {
	up := &fakeUploader{}
	store := &recordingAuditStore{}
	h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	status, _ := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, up.wasCalled())
	assert.Contains(t, up.relayedContentType(), "multipart/form-data; boundary=")

	entries := store.audited()
	require.Len(t, entries, 1)
	assert.Equal(t, clientUpdateAuditAction, entries[0].Action)
	// No per-file details: the filenames are client-update-service's record.
	assert.Empty(t, entries[0].Details)
}

// client-update-service picks a stored object's content type from the part's own
// header, falling back only when there is none. Re-encoding with CreateFormFile
// would stamp application/octet-stream on every part and silently change what
// the .yaml is stored as, so the relay must copy the header through verbatim.
func TestUploadClientVersion_PreservesPartContentTypes(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	status, _ := postUpload(t, srv, body, ct)
	require.Equal(t, http.StatusOK, status)

	relayed := parseRelayed(t, up.relayedBody(), up.relayedContentType())
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

// parseRelayed re-parses the multipart body the uploader actually sent, keyed by
// form field, so a test can assert on what crossed the wire rather than on how
// the handler chose to build it.
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

// A body far larger than any internal buffer must arrive intact. This proves
// integrity, not streaming — a fully buffering handler would pass it too. The
// streaming claim is pinned by TestUploadClientVersion_RelaysBeforeBodyIsComplete.
func TestUploadClientVersion_LargeBodyArrivesIntact(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	big := strings.Repeat("A", size)
	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", big, ""},
	})
	status, _ := postUpload(t, srv, body, ct)
	require.Equal(t, http.StatusOK, status)

	relayed := parseRelayed(t, up.relayedBody(), up.relayedContentType())
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

			body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
				"configFile":  {"app.yaml", "version: 1", ""},
				"executeFile": {"app.exe", "MZ", ""},
			})
			status, _ := postUpload(t, srv, body, ct)

			assert.Equal(t, tt.wantStatus, status)
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

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile": {"app.yaml", "version: 1", ""},
	})
	status, _ := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusServiceUnavailable, status)
}

// The handler pushes its own read/write deadlines past the server's 15s/40s.
// If a middleware ever wraps c.Writer in a type without Unwrap,
// http.NewResponseController stops reaching the connection and this fails —
// which is the point: a silent no-op would kill real uploads at 15s.
func TestUploadClientVersion_ExtendsItsOwnDeadlines(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	status, _ := postUpload(t, srv, body, ct)

	require.Equal(t, http.StatusOK, status,
		"a 500 here means SetReadDeadline/SetWriteDeadline returned ErrNotSupported")
}

// The route must sit inside the /v1/admin group so requireAdmin gates it. A
// non-admin caller must be turned away before any byte reaches the upstream.
func TestRoutes_ClientUpdatesRequiresAdmin(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))

	sessions := &fakeSessionStore{
		FindByHashFn: func(context.Context, string) (*session.Session, error) {
			return &session.Session{
				ID: "s1", UserID: "u1", Account: "someone", SiteID: "site-A",
				Roles: []string{"user"}, // not an admin
			}, nil
		},
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	applyBaseMiddleware(r, nil)
	registerRoutes(r, h, sessions, "site-A")
	srv := httptest.NewServer(r)
	defer srv.Close()

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer some-session-token")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.False(t, up.wasCalled(), "a non-admin request must never reach the upstream")
}

// slowUploader answers only after the delay elapses, without failing early —
// the shape of the outbound timeout firing on a stalled upstream.
type slowUploader struct {
	after time.Duration
	err   error
}

func (s *slowUploader) Upload(_ context.Context, body *uploadBody) error {
	_, _ = io.Copy(io.Discard, body.reader)
	<-time.After(s.after)
	return s.err
}

// The outbound upload timeout and this request's write deadline are both derived
// from ClientUpdateTimeout, but the write deadline starts FIRST, at handler
// entry. Were they equal, the connection would already be unwritable by the time
// a stalled upstream produced an error to report, and the admin would get a
// transport failure instead of the documented 503 envelope. uploadResponseMargin
// is what keeps the connection writable long enough to answer.
func TestUploadClientVersion_AnswersAfterTheOutboundBudgetExpires(t *testing.T) {
	cfg := uploadTestCfg()
	cfg.ClientUpdateTimeout = 200 * time.Millisecond
	up := &slowUploader{after: 700 * time.Millisecond, err: errcode.Unavailable("client update service is unavailable")}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), cfg, nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err, "the write deadline expired before the handler could write its envelope")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// deadlineCapturingUploader records the context it was handed.
type deadlineCapturingUploader struct {
	mu       sync.Mutex
	deadline time.Time
	hasDL    bool
}

func (d *deadlineCapturingUploader) Upload(ctx context.Context, body *uploadBody) error {
	_, _ = io.Copy(io.Discard, body.reader)
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deadline, d.hasDL = dl, ok
	return nil
}

// Because resty drains the relay pipe before dialling, the inbound and outbound
// phases run back to back rather than overlapping. Without a deadline bounding
// the whole handler, each phase gets its own ClientUpdateTimeout — up to 2d —
// while the write deadline only runs to d + uploadResponseMargin, so the
// connection can die before the handler can answer. The upstream call must
// therefore inherit what is left of ONE budget, not start a second one.
func TestUploadClientVersion_UpstreamCallInheritsTheRequestBudget(t *testing.T) {
	cfg := uploadTestCfg()
	cfg.ClientUpdateTimeout = 90 * time.Second
	up := &deadlineCapturingUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), cfg, nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	start := time.Now()
	status, _ := postUpload(t, srv, body, ct)
	require.Equal(t, http.StatusOK, status)

	up.mu.Lock()
	defer up.mu.Unlock()
	require.True(t, up.hasDL, "the upstream call must carry a deadline, or a slow inbound phase plus a full outbound timeout can outlive the write deadline")
	// One budget, not two: the deadline sits ~ClientUpdateTimeout from when the
	// handler began (a few ms after start), never 2x it.
	budget := up.deadline.Sub(start)
	assert.Greater(t, budget, cfg.ClientUpdateTimeout/2,
		"the deadline must be the request budget, not some smaller leftover")
	assert.Less(t, budget, cfg.ClientUpdateTimeout+5*time.Second,
		"the upstream call must inherit the request's budget, not start a fresh one on top of it")
}

// The upload endpoint has no legitimate reason to redirect, and following one
// risks the service-account token: net/http strips Authorization only when the
// HOST changes (shouldCopyHeaderOnRedirect compares hosts, not schemes), so a
// same-host https->http downgrade — or a hop to a subdomain — would carry the
// credential onward, in the clear. A 3xx here is an error, not a hop.
func TestHTTPVersionUploader_DoesNotFollowRedirects(t *testing.T) {
	var reachedTarget bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reachedTarget = true
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/v1/version", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	u := newHTTPVersionUploader(srv.URL, "0123456789abcdef", 5*time.Second)

	err := u.Upload(context.Background(), testBody(strings.NewReader("x"), 1))

	require.Error(t, err, "a redirect must not be followed silently")
	assert.False(t, reachedTarget, "the credential must not reach the redirect target")
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
}

// The whole point of the rewrite: peak memory must stay flat regardless of
// artifact size. The old io.Pipe relay fed resty, which ran io.ReadAll on the
// body before dialling, so admin-service held the entire artifact in RAM.
func TestUploadClientVersion_StreamsWithoutBufferingTheArtifact(t *testing.T) {
	const artifactSize = 48 << 20
	// Generous next to the artifact, but far under it: the handler holds only
	// MaxMultipartMemory plus envelope snapshots, and the rest lives on disk.
	const maxPeak = 24 << 20

	var received int64
	up := &countingUploader{onBody: func(r io.Reader) { received, _ = io.Copy(io.Discard, r) }}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	// The request body is streamed too — a bytes.Buffer here would put the
	// artifact on the test's own heap and swamp the measurement.
	reqBody, ct := streamingPayload(t, artifactSize)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", reqBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)

	var status int
	peak := testutil.PeakHeapDuring(func() {
		resp, doErr := srv.Client().Do(req)
		require.NoError(t, doErr)
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		status = resp.StatusCode
	})

	require.Equal(t, http.StatusOK, status)
	require.Greater(t, received, int64(artifactSize), "the artifact must arrive whole")
	require.LessOrEqualf(t, peak, uint64(maxPeak),
		"peak heap %d MiB while relaying a %d MiB artifact: the body is being buffered",
		peak>>20, artifactSize>>20)
	t.Logf("relayed %d MiB with a %d MiB peak heap", artifactSize>>20, peak>>20)
}

// streamingPayload builds a multipart request body whose large part is generated
// on the fly, so constructing it costs no heap.
func streamingPayload(t *testing.T, bigSize int64) (io.Reader, string) {
	t.Helper()
	const boundary = "streamingpayloadboundary"
	var head bytes.Buffer
	mw := multipart.NewWriter(&head)
	require.NoError(t, mw.SetBoundary(boundary))

	fw, err := mw.CreateFormFile("configFile", "app.yaml")
	require.NoError(t, err)
	_, err = fw.Write([]byte("version: 1"))
	require.NoError(t, err)
	// Header only: the content is appended as a generated reader below.
	_, err = mw.CreateFormFile("executeFile", "app.exe")
	require.NoError(t, err)

	tail := "\r\n--" + boundary + "--\r\n"
	return io.MultiReader(
		bytes.NewReader(append([]byte(nil), head.Bytes()...)),
		io.LimitReader(zeroReader{}, bigSize),
		strings.NewReader(tail),
	), mw.FormDataContentType()
}

// countingUploader hands the assembled body to a callback.
type countingUploader struct{ onBody func(io.Reader) }

func (c *countingUploader) Upload(_ context.Context, body *uploadBody) error {
	c.onBody(body.reader)
	return nil
}

// admin-service validates nothing: duplicate parts for a contract field are
// relayed as sent, and client-update-service decides what to do with them.
func TestUploadClientVersion_RelaysDuplicatePartsUnvalidated(t *testing.T) {
	up := &fakeUploader{}
	store := &recordingAuditStore{}
	h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	raw := &bytes.Buffer{}
	mw := multipart.NewWriter(raw)
	for _, f := range []struct{ field, name string }{
		{"configFile", "first.yaml"}, {"configFile", "second.yaml"},
		{"executeFile", "first.exe"}, {"executeFile", "second.exe"},
	} {
		fw, err := mw.CreateFormFile(f.field, f.name)
		require.NoError(t, err)
		_, err = fw.Write([]byte("x"))
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	status, _ := postUpload(t, srv, raw, mw.FormDataContentType())
	require.Equal(t, http.StatusOK, status)

	relayed := string(up.relayedBody())
	for _, name := range []string{"first.yaml", "second.yaml", "first.exe", "second.exe"} {
		assert.Contains(t, relayed, name, "every part must reach the upstream")
	}
	require.Len(t, store.audited(), 1)
	assert.Empty(t, store.audited()[0].Details)
}

// A caller must not be able to hold the relay open with an unbounded number of
// parts, nor grow the audit entry by choosing arbitrary field names.
func TestUploadClientVersion_RejectsTooManyParts(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	raw := &bytes.Buffer{}
	mw := multipart.NewWriter(raw)
	for i := 0; i <= maxUploadParts; i++ {
		fw, err := mw.CreateFormFile(fmt.Sprintf("extra%02d", i), fmt.Sprintf("f%d.bin", i))
		require.NoError(t, err)
		_, err = fw.Write([]byte("x"))
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	status, respBody := postUpload(t, srv, raw, mw.FormDataContentType())

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, string(respBody), "could not read the uploaded files")
	assert.False(t, up.wasCalled(), "nothing may reach the upstream once the part cap trips")
}

// stalledUploadBody returns a body carrying one complete part and then nothing:
// the client never finishes sending, so the request stays open until a deadline
// fires. The writer goroutine has a real termination path — cleanup signals it
// and waits — so a run leaves nothing parked behind.
func stalledUploadBody(t *testing.T) (io.Reader, string) {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = pw.Close()
		<-done
	})
	go func() {
		defer close(done)
		fw, err := mw.CreateFormFile("configFile", "app.yaml")
		if err != nil {
			return
		}
		_, _ = fw.Write([]byte("version: 1"))
		<-stop // never Closed: the body has no terminating boundary
	}()
	return pr, mw.FormDataContentType()
}

// A client that stalls mid-body trips the request's read deadline. The status is
// right either way — the client did not finish sending — but answering "could
// not read the uploaded files" points an operator at malformed multipart data
// when the cause was the clock.
func TestUploadClientVersion_StalledBodyReportsATimeout(t *testing.T) {
	cfg := uploadTestCfg()
	cfg.ClientUpdateTimeout = 300 * time.Millisecond
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), cfg, nil, nil, withVersionUploader(&fakeUploader{}))
	srv := uploadTestServer(t, h)

	reqBody, ct := stalledUploadBody(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", reqBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err, "the handler must answer the timeout, not drop the connection")
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.NotContains(t, string(body), "could not read the uploaded files",
		"a deadline expiry must not be reported as malformed multipart data")
	assert.Contains(t, strings.ToLower(string(body)), "time")
}

// Without a byte cap the spooled temp files are unbounded: c.MultipartForm
// writes the whole body to disk before maxUploadParts is ever consulted, so a
// single caller could fill the pod's ephemeral storage.
func TestUploadClientVersion_RejectsAnOversizeBody(t *testing.T) {
	cfg := uploadTestCfg()
	cfg.ClientUpdateMaxUploadBytes = 4 << 10
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), cfg, nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", strings.Repeat("A", 64<<10), ""},
	})
	status, respBody := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, strings.ToLower(string(respBody)), "too large")
	assert.False(t, up.wasCalled(), "nothing may reach the upstream once the cap trips")
}

// The cap must not reject an upload that fits.
func TestUploadClientVersion_AcceptsABodyUnderTheCap(t *testing.T) {
	cfg := uploadTestCfg()
	cfg.ClientUpdateMaxUploadBytes = 1 << 20
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), cfg, nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := multipartPayload(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", strings.Repeat("A", 8<<10), ""},
	})
	status, _ := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, up.wasCalled())
}
