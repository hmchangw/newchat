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
func TestMapUpstreamStatus_DefaultBranchTruncatesLongBody(t *testing.T) {
	longBody := strings.Repeat("x", 1000)
	err := mapUpstreamStatus(500, longBody)
	require.Error(t, err)
	cause := errors.Unwrap(err)
	require.Error(t, cause)
	assert.LessOrEqual(t, len(cause.Error()), len(longBody))
	assert.Contains(t, cause.Error(), "truncated")
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

// uploadTestServer builds the real router (base middleware, including the real
// o11ygin chain wired the same way main.go does, + a stubbed admin principal)
// around a live server, so deadlines and streaming behave as in prod. In
// particular, this exercises whatever c.Writer wrapping o11ygin/otelgin do —
// see TestUploadClientVersion_ExtendsItsOwnDeadlines.
func uploadTestServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
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

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
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
	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
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

			body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
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

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
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

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	status, _ := postUpload(t, srv, body, ct)

	require.Equal(t, http.StatusOK, status,
		"a 500 here means SetReadDeadline/SetWriteDeadline returned ErrNotSupported")
}

// errTestTornDown ends a deliberately unfinished request body at test cleanup.
var errTestTornDown = errors.New("test torn down")

// streamProbeUploader closes saw on its first non-empty read, then drains the
// rest. A handler that buffered the request body would not even call Upload
// until the body was complete, so the signal arriving while the test is still
// holding the body open is what distinguishes streaming from buffering.
type streamProbeUploader struct {
	saw  chan struct{}
	once sync.Once
}

func (s *streamProbeUploader) Upload(_ context.Context, _ string, body io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			s.once.Do(func() { close(s.saw) })
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// writeHeldBody writes the first part, waits for release, then writes the second
// and terminates the body — so the request is deliberately incomplete until the
// test says otherwise.
func writeHeldBody(mw *multipart.Writer, bodyW *io.PipeWriter, release <-chan struct{}) error {
	first := textproto.MIMEHeader{}
	first.Set("Content-Disposition", `form-data; name="configFile"; filename="app.yaml"`)
	fw, err := mw.CreatePart(first)
	if err != nil {
		return err
	}
	// Comfortably past the transport's and the multipart reader's buffers, so a
	// pass cannot be an artefact of something small sitting in a buffer.
	if _, err := io.WriteString(fw, strings.Repeat("A", 64<<10)); err != nil {
		return err
	}

	<-release

	second := textproto.MIMEHeader{}
	second.Set("Content-Disposition", `form-data; name="executeFile"; filename="app.exe"`)
	fw2, err := mw.CreatePart(second)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(fw2, "MZ"); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	return bodyW.Close()
}

// Streaming is this branch's central performance claim, so it gets a test that
// only a streaming handler can pass: the request body is driven from a test-side
// pipe and deliberately left incomplete, and the uploader must already have seen
// bytes before the remainder is written. A handler that did io.ReadAll on the
// request body and handed Upload a buffer would block here until the timeout.
func TestUploadClientVersion_RelaysBeforeBodyIsComplete(t *testing.T) {
	up := &streamProbeUploader{saw: make(chan struct{})}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	bodyR, bodyW := io.Pipe()
	mw := multipart.NewWriter(bodyW)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBody := func() { releaseOnce.Do(func() { close(release) }) }
	writeErr := make(chan error, 1)
	go func() { writeErr <- writeHeldBody(mw, bodyW, release) }()

	// Without this, a buffering regression would time out below and then hang the
	// suite: httptest.Server.Close waits for the in-flight request, whose body
	// never ends. Releasing the writer and tearing down both pipe ends lets the
	// handler unwind so the failure is reported promptly. All no-ops on success.
	t.Cleanup(func() {
		releaseBody()
		_ = bodyW.CloseWithError(errTestTornDown)
		_ = bodyR.CloseWithError(errTestTornDown)
	})

	type result struct {
		status int
		err    error
	}
	got := make(chan result, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", bodyR)
		if err != nil {
			got <- result{err: err}
			return
		}
		req.Header.Set("Content-Type", mw.FormDataContentType())
		resp, err := srv.Client().Do(req)
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		got <- result{status: resp.StatusCode}
	}()

	select {
	case <-up.saw:
	case r := <-got:
		t.Fatalf("request finished before any part reached the uploader: status=%d err=%v", r.status, r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("no bytes reached the uploader while the request body was still open — the handler buffered instead of streaming")
	}

	releaseBody() // only now does the rest of the body exist
	require.NoError(t, <-writeErr)

	select {
	case r := <-got:
		require.NoError(t, r.err)
		assert.Equal(t, http.StatusOK, r.status)
	case <-time.After(10 * time.Second):
		t.Fatal("request did not complete after the body was finished")
	}
}

// A truncated client body is the admin's fault, not an upstream outage. The
// relay's pipe close propagates its read error into Upload, which maps it to
// Unavailable — so without the handler's errors.Is guard this would answer 503
// and send ops chasing a phantom outage while the upstream is healthy.
func TestUploadClientVersion_TruncatedBodyIsBadRequest(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	// Hand-built rather than truncated from uploadBody's output, so it is
	// unambiguous where the body stops: mid-part, with no closing boundary.
	const boundary = "truncatedbodyboundary"
	raw := "--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"configFile\"; filename=\"app.yaml\"\r\n\r\n" +
		"version: 1"

	status, body := postUpload(t, srv, bytes.NewBufferString(raw), "multipart/form-data; boundary="+boundary)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, string(body), "could not read the uploaded files")
}

// abortingUploader reads one chunk and then rejects, the way an upstream that
// validates the first part does — leaving the relay still writing, so the
// handler's pr.CloseWithError injects the upstream's own error into the pipe and
// res.err comes back wrapping it.
type abortingUploader struct{ err error }

func (a *abortingUploader) Upload(_ context.Context, _ string, body io.Reader) error {
	var buf [16]byte
	_, _ = body.Read(buf[:])
	return a.err
}

// The mirror of the truncation case: when the upstream is the one that rejects
// mid-body, its message must survive rather than being replaced by the generic
// client-side one. Swapping the two checks unconditionally would break this.
func TestUploadClientVersion_UpstreamRejectionSurvivesTheRelayAbort(t *testing.T) {
	up := &abortingUploader{err: errcode.BadRequest("configFile must be a .yaml or .yml file")}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	// Large enough that the relay is certainly still writing when Upload returns.
	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"executeFile": {"app.exe", strings.Repeat("A", 4<<20), ""},
	})
	status, respBody := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, string(respBody), "configFile must be a .yaml or .yml file")
	assert.NotContains(t, string(respBody), "could not read the uploaded files",
		"the upstream's own message must not be replaced by the client-side one")
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

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
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
