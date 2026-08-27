package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// clientUpdateVersionPath is client-update-service's upload endpoint
// (docs/client-api.md §12).
const clientUpdateVersionPath = "/api/v1/version"

// versionUploader ships one artifact pair to client-update-service. Defined here,
// in the consumer, so tests can substitute a fake without an HTTP server.
type versionUploader interface {
	// Upload streams body — an already-encoded multipart payload whose boundary
	// contentType declares — to the upload endpoint.
	Upload(ctx context.Context, contentType string, body io.Reader) error
}

// restyVersionUploader is the production versionUploader over resty.
type restyVersionUploader struct {
	client *resty.Client
}

// newRestyVersionUploader wraps a client built by restyutil.New with the
// service-account bearer token and the upload timeout already applied.
//
// Three properties of that client are load-bearing and must not change:
//   - SetContentLength stays OFF. resty measures an io.Reader body when it is
//     on (v2.17.2 middleware.go:519-527), sending a fixed Content-Length.
//   - No retries. The body is a pipe; once drained, a retry would send nothing.
//   - Redirects are refused, set here rather than left to the caller. The
//     upload endpoint has no reason to redirect, and net/http strips
//     Authorization only when the HOST changes (shouldCopyHeaderOnRedirect
//     compares hosts, not schemes), so a same-host https->http hop would carry
//     the service-account token onward in the clear. A 3xx becomes an error.
func newRestyVersionUploader(client *resty.Client) *restyVersionUploader {
	client.SetRedirectPolicy(resty.NoRedirectPolicy())
	return &restyVersionUploader{client: client}
}

func (u *restyVersionUploader) Upload(ctx context.Context, contentType string, body io.Reader) error {
	resp, err := u.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", contentType).
		SetBody(body).
		Post(clientUpdateVersionPath)
	if err != nil {
		return errcode.Unavailable("client update service is unavailable",
			errcode.WithCause(fmt.Errorf("post client update: %w", err)))
	}
	return mapUpstreamStatus(resp.StatusCode(), resp.String())
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

// maxAuditFileNameLen caps an audited filename. Separate from
// maxUpstreamBodyLogLen: that one bounds a log line, this one bounds a value
// persisted to Mongo, and the two are free to move independently.
const maxAuditFileNameLen = 256

// truncateFileName caps a filename on a rune boundary. A byte-offset cut (what
// truncateForLog does, correctly, for a log value) could split a multi-byte
// rune and store invalid UTF-8 in the audit entry.
func truncateFileName(s string) string {
	if len(s) <= maxAuditFileNameLen {
		return s
	}
	cut := maxAuditFileNameLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "...(truncated)"
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

// auditedUploadFields bounds what the audit entry records. These are the two
// fields client-update-service's contract defines; anything else is relayed but
// not retained, so the entry's cardinality cannot grow with the part count.
var auditedUploadFields = map[string]struct{}{
	"configFile":  {},
	"executeFile": {},
}

// maxUploadParts caps the file parts one request may carry. The contract needs
// two; the ceiling is generous but finite, so a caller cannot hold a relay open
// indefinitely by streaming an unbounded number of parts.
const maxUploadParts = 16

// quoteEscaper mirrors mime/multipart's own escaping for Content-Disposition
// values, so a filename containing a quote or backslash cannot break out of the
// header it is written into. CR and LF become %0D/%0A for the same reason,
// matching pkg/drive/multipart.go's escaper rather than diverging from it.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

// relayResult carries the relay goroutine's outcome back to the handler. The
// filenames travel on the channel rather than a shared map so the handler can
// read them without racing the goroutine that filled them.
type relayResult struct {
	names map[string]string
	err   error
}

// uploadClientVersion relays an artifact pair to client-update-service under this
// service's own credential, then records the publication in the audit log.
//
// It validates nothing about the artifacts: client-update-service owns the
// extension and content rules, and duplicating them here would let the two
// services disagree about what a valid upload is.
func (h *Handler) uploadClientVersion(c *gin.Context) {
	// ONE budget for the whole request, inherited by the upstream call. resty
	// drains the relay pipe before dialling, so the inbound and outbound phases
	// run back to back rather than overlapping: without this, each would get its
	// own ClientUpdateTimeout and the handler could run to 2d against a write
	// deadline of d + uploadResponseMargin.
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

	mr, err := c.Request.MultipartReader()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request body must be multipart/form-data"))
		return
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	done := make(chan relayResult, 1)
	go func() {
		names, relayErr := relayParts(mr, mw)
		// Publish BEFORE closing, onto the buffered channel so this never blocks.
		// CloseWithError is what wakes Upload, so anything that observes the close
		// — including an Upload that returns because of it — must already be able
		// to see this result; otherwise awaitRelay's non-blocking receive races the
		// send and a genuine bad-body error reports as an upstream outage.
		done <- relayResult{names: names, err: relayErr}
		// Closing the pipe is what unblocks the reader, so it must happen on every
		// path out of this goroutine.
		_ = pw.CloseWithError(relayErr)
	}()

	// Deferred as well as called inline: the inline close is what unblocks the
	// goroutine before <-done, and the defer is what unblocks it if Upload panics
	// and gin.Recovery unwinds this frame — otherwise the relay would park on
	// pw.Write forever, pinning the request body reader.
	var uploadErr error
	defer func() { _ = pr.CloseWithError(uploadErr) }()

	uploadErr = h.uploader.Upload(ctx, mw.FormDataContentType(), pr)
	// Unblocks the goroutine if the upload gave up mid-body; a no-op otherwise.
	_ = pr.CloseWithError(uploadErr)

	res, relayKilled := h.awaitRelay(c, done, uploadErr)

	// res.err first, but only when the relay failed on its own terms — the
	// client's body — rather than because we tore it down after the upstream
	// rejected the request. Whether the relay had already finished is the
	// discriminator: awaitRelay reports relayKilled only when it was still
	// parked when we closed the request body under it. Without this split, a bad
	// client body reports as an upstream outage and sends ops chasing a phantom
	// one — or, once the teardown exists, an upstream 401 reports as a bad body.
	if res.err != nil && !relayKilled {
		// Still 4xx — the client did not finish sending — but a clock running out
		// is not malformed data, and saying so sends an operator hunting the wrong
		// bug.
		if uploadDeadlineExpired(ctx, res.err) {
			errhttp.Write(ctx, c, errcode.BadRequest("the upload did not finish within the allowed time"))
			return
		}
		errhttp.Write(ctx, c, errcode.BadRequest("could not read the uploaded files"))
		return
	}
	if uploadErr != nil {
		errhttp.Write(ctx, c, uploadErr)
		return
	}

	h.audit(ctx, c, clientUpdateAuditAction, "", "", res.names)
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}

// uploadDeadlineExpired reports whether the relay failed because the request ran
// out of time rather than because the body was malformed. Two clocks can fire:
// the request's read deadline, which surfaces through the relay's read as
// os.ErrDeadlineExceeded, and this handler's own budget on ctx.
func uploadDeadlineExpired(ctx context.Context, relayErr error) bool {
	return errors.Is(relayErr, os.ErrDeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// awaitRelay collects the relay's outcome, tearing it down first if the upload
// already failed while the relay is still running.
//
// Closing the pipe only unblocks a relay parked on pw.Write. When the upstream
// answers before the browser finishes sending — an immediate 401 from a bad
// service token is the clearest case — the relay is parked on the REQUEST body
// instead, and a bare <-done would pin this handler, its goroutine and both
// connections until the client sends more or the extended read deadline expires.
//
// killed reports whether the relay was still running when we cut the body out
// from under it, which is what tells the caller the relay's error is ours rather
// than the client's. A relay that had already finished keeps its own verdict.
//
// The teardown is an immediate read deadline, NOT c.Request.Body.Close():
// net/http's server body guards Read and Close with the same mutex, so closing
// during an in-flight read blocks behind that read instead of interrupting it.
// Expiring the deadline fails the read itself.
func (h *Handler) awaitRelay(c *gin.Context, done <-chan relayResult, uploadErr error) (res relayResult, killed bool) {
	if uploadErr == nil {
		return <-done, false
	}
	select {
	case res = <-done:
		return res, false // finished on its own; its error is genuine
	default:
	}
	if err := http.NewResponseController(c.Writer).SetReadDeadline(time.Now()); err != nil {
		// Nothing better to do than wait: the relay still exits when the client
		// stops sending or the extended deadline expires.
		slog.WarnContext(c.Request.Context(), "could not expire the upload read deadline", "error", err)
	}
	return <-done, true
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

// relayParts copies every file part through to mw, preserving each part's field
// name, filename and declared Content-Type, and returns field->filename for the
// audit entry. Non-file fields are skipped: client-update-service reads only
// file parts.
//
// CreatePart, not CreateFormFile: the latter stamps application/octet-stream on
// every part, which would change what client-update-service stores the .yaml as.
func relayParts(mr *multipart.Reader, mw *multipart.Writer) (map[string]string, error) {
	names := make(map[string]string)
	parts := 0
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return names, fmt.Errorf("read upload part: %w", err)
		}
		parts++
		if parts > maxUploadParts {
			_ = part.Close()
			return names, fmt.Errorf("upload has more than %d parts", maxUploadParts)
		}
		if err := relayOnePart(part, mw, names); err != nil {
			_ = part.Close()
			return names, err
		}
		if err := part.Close(); err != nil {
			return names, fmt.Errorf("close upload part: %w", err)
		}
	}
	if err := mw.Close(); err != nil {
		return names, fmt.Errorf("finish relay body: %w", err)
	}
	return names, nil
}

// relayOnePart copies a single part and records its filename.
func relayOnePart(part *multipart.Part, mw *multipart.Writer, names map[string]string) error {
	if part.FileName() == "" {
		return nil // not a file part
	}
	// Audit only the two contract fields. Every part is still relayed unvalidated
	// — client-update-service remains the sole judge of the artifacts — but a
	// caller sending millions of distinct field names must not be able to grow
	// this map (and the AuditEntry built from it) without bound.
	//
	// FIRST wins, never overwritten: client-update-service reads its parts with
	// c.FormFile, which selects the first file for a field name. Recording a
	// later duplicate would make the audit entry name a file the upstream did
	// not store.
	if _, ok := auditedUploadFields[part.FormName()]; ok {
		if _, seen := names[part.FormName()]; !seen {
			names[part.FormName()] = truncateFileName(part.FileName())
		}
	}

	hdr := textproto.MIMEHeader{}
	// %q rather than "%s" would re-escape what quoteEscaper already escaped.
	hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, //nolint:gocritic // sprintfQuotedString
		quoteEscaper.Replace(part.FormName()), quoteEscaper.Replace(part.FileName())))
	// Copied only when present, so a bare part stays bare and the upstream's own
	// content-type fallback still applies.
	if ct := part.Header.Get("Content-Type"); ct != "" {
		hdr.Set("Content-Type", ct)
	}
	fw, err := mw.CreatePart(hdr)
	if err != nil {
		return fmt.Errorf("create relay part %q: %w", part.FormName(), err)
	}
	if _, err := io.Copy(fw, part); err != nil {
		return fmt.Errorf("relay part %q: %w", part.FormName(), err)
	}
	return nil
}
