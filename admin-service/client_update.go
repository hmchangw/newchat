package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// clientUpdateVersionPath is client-update-service's upload endpoint
// (docs/client-api.md §12).
const clientUpdateVersionPath = "/api/v1/version"

// versionUploader ships one artifact pair to client-update-service. Defined here,
// in the consumer, so tests can substitute a fake without an HTTP server.
type versionUploader interface {
	Upload(ctx context.Context, body *uploadBody) error
}

// httpVersionUploader is the production versionUploader.
//
// It drives net/http directly rather than resty — the same deliberate exception
// pkg/drive/uploader.go takes, for the same reason: resty v2 materializes any
// io.Reader body it cannot natively replay (createHTTPRequest -> getBodyCopy ->
// io.ReadAll), which is exactly the buffering this path exists to avoid. The
// client still comes from restyutil so the shared transport, OTel
// instrumentation and timeout are preserved; only resty's body handling and its
// request/response log hooks are given up, and upload failures are logged once
// at the caller's errhttp boundary anyway.
type httpVersionUploader struct {
	client   *http.Client
	endpoint string
	token    string
}

// maxUpstreamBodyReadLen caps how much of an upstream response is read. An
// errcode envelope is far smaller; the cap stops a large error page from being
// pulled into memory just to be truncated for a log line.
const maxUpstreamBodyReadLen = 8 << 10

// newHTTPVersionUploader builds the uploader for baseURL. Redirects are refused:
// the upload endpoint has no reason to redirect, and net/http strips
// Authorization only when the HOST changes (shouldCopyHeaderOnRedirect compares
// hosts, not schemes), so a same-host https->http hop would carry the
// service-account token onward in the clear. ErrUseLastResponse surfaces the 3xx
// as a status for mapUpstreamStatus rather than following it.
func newHTTPVersionUploader(baseURL, token string, timeout time.Duration) *httpVersionUploader {
	client := restyutil.New(baseURL, restyutil.WithTimeout(timeout)).GetClient()
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &httpVersionUploader{
		client:   client,
		endpoint: strings.TrimRight(baseURL, "/") + clientUpdateVersionPath,
		token:    token,
	}
}

// Upload POSTs body to client-update-service under this service's own
// credential. The body is streamed, not buffered, so ContentLength must be set
// explicitly from what buildUploadBody counted — net/http cannot infer a reader
// chain's length and would otherwise fall back to chunked encoding, which the
// upstream's multipart parser has no reason to accept. ctx carries the whole
// request's remaining budget, so an expiry here is reported as a timeout rather
// than as an unreachable upstream.
func (u *httpVersionUploader) Upload(ctx context.Context, body *uploadBody) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.endpoint, body.reader)
	if err != nil {
		return errcode.Unavailable("client update upload is misconfigured",
			errcode.WithCause(fmt.Errorf("build client update request: %w", err)))
	}
	req.ContentLength = body.length
	req.Header.Set("Content-Type", body.contentType)
	req.Header.Set("Authorization", "Bearer "+u.token)

	resp, err := u.client.Do(req)
	if err != nil {
		return uploadPostError(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Bounded: a read error here is irrelevant next to the status, and
	// mapUpstreamStatus treats an unparseable body as no body at all.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBodyReadLen))
	return mapUpstreamStatus(resp.StatusCode, string(snippet))
}

// uploadPostError separates "the upstream never answered in time" from "the
// upstream could not be reached". Both are 503 — neither is the admin's doing —
// but only one is worth retrying at a larger CLIENT_UPDATE_UPLOAD_TIMEOUT, and
// the generic message sent an operator looking for a down service instead.
func uploadPostError(err error) error {
	// The shared budget only ever surfaces as a context deadline here — resty's
	// client timeout wraps one too. A transport-level i/o timeout (dial, TLS)
	// deliberately does NOT match: that upstream is unreachable, not slow.
	if errors.Is(err, context.DeadlineExceeded) {
		return errcode.Unavailable("client update service did not respond in time",
			errcode.WithCause(fmt.Errorf("post client update: %w", err)))
	}
	return errcode.Unavailable("client update service is unavailable",
		errcode.WithCause(fmt.Errorf("post client update: %w", err)))
}

// mapUpstreamStatus turns client-update-service's verdict into this service's.
// A 401/403 means OUR credential is wrong — a deployment fault, not the admin's:
// relaying it would read as an expired admin session and prompt a pointless
// re-login, so it becomes a 503 with the real reason logged server-side.
func mapUpstreamStatus(status int, body string) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusBadRequest:
		return errcode.BadRequest(upstreamMessage(body, "client update service rejected the upload"))
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return errcode.Unavailable("client update upload is misconfigured",
			errcode.WithCause(fmt.Errorf("client-update-service rejected this service's credential with status %d", status)))
	default:
		// The upstream's own envelope message, never the raw body: CLAUDE.md forbids
		// wrapping a raw body into a cause, and a 5xx from an ingress or proxy can
		// carry arbitrary content. A body that is not an envelope yields status only.
		if msg := upstreamMessage(body, ""); msg != "" {
			return errcode.Unavailable("client update service is unavailable",
				errcode.WithCause(fmt.Errorf("client-update-service returned status %d: %s", status, truncateForLog(msg))))
		}
		return errcode.Unavailable("client update service is unavailable",
			errcode.WithCause(fmt.Errorf("client-update-service returned status %d", status)))
	}
}

// maxUpstreamBodyLogLen caps how much of an upstream error body reaches the
// cause on the default (unexpected-status) branch, so a large error page
// cannot bloat a log line.
const maxUpstreamBodyLogLen = 256

// truncateForLog caps a log value at maxUpstreamBodyLogLen bytes.
func truncateForLog(s string) string {
	if len(s) <= maxUpstreamBodyLogLen {
		return s
	}
	return s[:maxUpstreamBodyLogLen] + "...(truncated)"
}

// upstreamMessage lifts the human-readable text out of an errcode envelope. The
// upstream `reason` is deliberately NOT copied: reasons are a contract between a
// service and its own clients, and re-emitting another service's would put
// undocumented codes into admin-service's surface.
func upstreamMessage(body, fallback string) string {
	if remote, ok := errcode.Parse([]byte(body)); ok {
		return remote.Message
	}
	return fallback
}

// clientUpdateAuditAction is the audit action for a published artifact pair.
const clientUpdateAuditAction = "client_update.upload"

// maxUploadParts caps the file parts one request may carry. The contract needs
// two; the ceiling is generous but finite, so a caller cannot hold a relay open
// indefinitely by streaming an unbounded number of parts.
const maxUploadParts = 16

// quoteEscaper mirrors mime/multipart's own escaping for Content-Disposition
// values, so a filename containing a quote or backslash cannot break out of the
// header it is written into. CR and LF become %0D/%0A for the same reason,
// matching pkg/drive/multipart.go's escaper rather than diverging from it.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

// uploadBody is a multipart payload whose file bytes are never copied into
// memory: small envelope snapshots (boundaries and part headers) are
// interleaved with the uploaded files' own readers, so peak memory is
// independent of artifact size. The exact length is summed up front so the
// request still carries a Content-Length instead of falling back to chunked.
type uploadBody struct {
	reader      io.Reader
	contentType string
	length      int64
	files       []multipart.File
}

// Close releases the opened form files. The temp files themselves are removed by
// net/http, which calls MultipartForm.RemoveAll when the request finishes
// (server.go:1709).
func (b *uploadBody) Close() error {
	for _, f := range b.files {
		_ = f.Close()
	}
	return nil
}

// partHeader builds a file part's MIME header. Concatenated rather than
// %q-formatted: the values are already escaped by quoteEscaper, and Go quoting
// would escape them a second time. Content-Type is copied only when the incoming
// part carried one, so a bare part stays bare and client-update-service's own
// fallback still applies.
func partHeader(field, filename, contentType string) textproto.MIMEHeader {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="`+quoteEscaper.Replace(field)+
		`"; filename="`+quoteEscaper.Replace(filename)+`"`)
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	return h
}

// buildUploadBody re-encodes the parsed form into a streamed multipart body,
// preserving every file part unvalidated — client-update-service remains the
// sole judge of the artifacts.
//
// Fields are walked in sorted order because Go map iteration is random and the
// body must be deterministic; multipart imposes no ordering, and the upstream
// selects parts by name. Within a field the wire order is preserved.
func buildUploadBody(form *multipart.Form) (*uploadBody, error) {
	var env bytes.Buffer
	mw := multipart.NewWriter(&env)
	b := &uploadBody{contentType: mw.FormDataContentType()}

	var chain []io.Reader
	// flush moves everything the writer emitted since the last call into the
	// chain. The bytes are copied out because env reuses its array after Reset.
	flush := func() {
		if env.Len() == 0 {
			return
		}
		buf := append([]byte(nil), env.Bytes()...)
		env.Reset()
		chain = append(chain, bytes.NewReader(buf))
		b.length += int64(len(buf))
	}

	fields := make([]string, 0, len(form.File))
	for field := range form.File {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	parts := 0
	for _, field := range fields {
		for _, fh := range form.File[field] {
			parts++
			if parts > maxUploadParts {
				_ = b.Close()
				return nil, fmt.Errorf("upload has more than %d parts", maxUploadParts)
			}
			f, err := fh.Open()
			if err != nil {
				_ = b.Close()
				return nil, fmt.Errorf("open upload part %q: %w", field, err)
			}
			b.files = append(b.files, f)

			if _, err := mw.CreatePart(partHeader(field, fh.Filename, fh.Header.Get("Content-Type"))); err != nil {
				_ = b.Close()
				return nil, fmt.Errorf("create relay part %q: %w", field, err)
			}
			flush()
			chain = append(chain, f)
			b.length += fh.Size
		}
	}

	if err := mw.Close(); err != nil {
		_ = b.Close()
		return nil, fmt.Errorf("finish relay body: %w", err)
	}
	flush()

	b.reader = io.MultiReader(chain...)
	return b, nil
}

// uploadClientVersion relays an artifact pair to client-update-service under this
// service's own credential, then records the publication in the audit log.
//
// It validates nothing about the artifacts: client-update-service owns the
// extension and content rules, and duplicating them here would let the two
// services disagree about what a valid upload is.
func (h *Handler) uploadClientVersion(c *gin.Context) {
	// ONE budget for the whole request, inherited by the upstream call: the
	// inbound parse and the outbound send run back to back, so without this each
	// would get its own ClientUpdateTimeout and the handler could run to 2d
	// against a write deadline of d + uploadResponseMargin.
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.ClientUpdateTimeout)
	defer cancel()

	if h.uploader == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("client update upload is not configured"))
		return
	}

	// A large artifact outlives the server's 15s read / 40s write timeouts. Those
	// stay put — httpWriteTimeout doubles as the ceiling checkHandlerTimeout
	// validates ROOM_RPC_TIMEOUT and FANOUT_TIMEOUT against — so only this
	// request's deadlines move.
	if err := extendUploadDeadlines(c, h.cfg.ClientUpdateTimeout); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	// Capped BEFORE parsing: c.MultipartForm spools the whole body to disk, and
	// maxUploadParts is only consulted afterwards, so without this one caller
	// could fill the pod's ephemeral storage regardless of the part count.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.cfg.ClientUpdateMaxUploadBytes)

	// Spills past MaxMultipartMemory to a temp file, so a large artifact costs
	// disk rather than heap. net/http removes those files when the request ends.
	form, err := c.MultipartForm()
	if err != nil {
		errhttp.Write(ctx, c, uploadReadError(ctx, err))
		return
	}

	body, err := buildUploadBody(form)
	if err != nil {
		errhttp.Write(ctx, c, uploadReadError(ctx, err))
		return
	}
	defer func() { _ = body.Close() }()

	if err := h.uploader.Upload(ctx, body); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	// Only after the upstream accepted the pair, and with no details: the
	// filenames are the upstream's record to keep, and auditing them here made
	// this service responsible for matching how it picks parts.
	h.audit(ctx, c, clientUpdateAuditAction, "", "", nil)
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}

// uploadReadError classifies a failure to obtain a usable request body. All
// three are 4xx — the client did not deliver one — but the message must say
// which, or an operator hunts malformed multipart data when the real cause was
// a size cap or the clock. errcode has no 413, so an oversize body is a
// bad_request whose message names the limit.
func uploadReadError(ctx context.Context, err error) error {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return errcode.BadRequest(fmt.Sprintf("the upload is too large: the limit is %d bytes", tooBig.Limit))
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errcode.BadRequest("the upload did not finish within the allowed time")
	}
	return errcode.BadRequest("could not read the uploaded files")
}

// uploadResponseMargin is how much longer this request's WRITE deadline runs
// than its read deadline, which is what keeps the budgets strictly ordered:
//
//	CLIENT_UPDATE_UPLOAD_TIMEOUT — ONE budget for the whole handler, inbound
//	  read plus outbound call (uploadClientVersion pins it on the context, and
//	  it is also this request's read deadline)
//	  < that timeout + uploadResponseMargin (this request's write deadline)
//	  < the browser's upload timeout (admin-frontend adds its own margin)
//
// The context deadline is what makes this hold. resty drains the relay pipe
// before dialling, so the inbound and outbound phases are sequential; left
// unbounded the outbound call would start a SECOND full timeout on top of the
// inbound one and the handler could run to 2x the budget, long past the write
// deadline. The margin then covers only what is left: writing the response.
//
// Without the margin the outbound timeout and the write deadline would be
// equal, and the write deadline starts FIRST — at handler entry. A stalled
// upstream would produce its error only after the connection had gone
// unwritable, and the admin would see a transport failure instead of the 503
// envelope this handler took the trouble to build.
//
// client-update-service's own HTTP_WRITE_TIMEOUT should be at least this
// service's CLIENT_UPDATE_UPLOAD_TIMEOUT, so it does not abandon a request this
// service is still waiting on. Its clock starts later (after the inbound
// phase), so equal defaults are safe.
const uploadResponseMargin = 30 * time.Second

// uploadDeadlines returns one upload's read and write deadlines: d bounds how
// long the client may take to send, and the write deadline outlives it by
// uploadResponseMargin so the verdict on that send is always reportable.
func uploadDeadlines(now time.Time, d time.Duration) (read, write time.Time) {
	return now.Add(d), now.Add(d + uploadResponseMargin)
}

// extendUploadDeadlines pushes this request's read and write deadlines out past
// the server's 15s/40s, per uploadDeadlines.
// Verified reachable: gin's responseWriter implements Unwrap, and neither
// o11ygin nor otelgin replaces c.Writer. A failure here is fatal rather than
// ignored — a silent no-op would kill every large upload at the server's 15s
// read timeout, with nothing in the logs to explain it.
func extendUploadDeadlines(c *gin.Context, d time.Duration) error {
	rc := http.NewResponseController(c.Writer)
	read, write := uploadDeadlines(time.Now(), d)
	if err := rc.SetReadDeadline(read); err != nil {
		return fmt.Errorf("extend upload read deadline: %w", err)
	}
	if err := rc.SetWriteDeadline(write); err != nil {
		return fmt.Errorf("extend upload write deadline: %w", err)
	}
	return nil
}
