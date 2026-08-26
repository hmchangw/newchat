package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

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
// Two properties of that client are load-bearing and must not change:
//   - SetContentLength stays OFF. resty buffers an entire io.Reader body into
//     memory when it is on (v2.17.2 middleware.go:519-527), defeating streaming.
//   - No retries. The body is a pipe; once drained, a retry would send nothing.
func newRestyVersionUploader(client *resty.Client) *restyVersionUploader {
	return &restyVersionUploader{client: client}
}

func (u *restyVersionUploader) Upload(ctx context.Context, contentType string, body io.Reader) error {
	resp, err := u.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", contentType).
		SetBody(body).
		Post(clientUpdateVersionPath)
	if err != nil {
		return errcode.Unavailable("client update service is unavailable", errcode.WithCause(err))
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
		return errcode.Unavailable("client update service is unavailable",
			errcode.WithCause(fmt.Errorf("client-update-service returned status %d: %s", status, truncateUpstreamBody(body))))
	}
}

// maxUpstreamBodyLogLen caps how much of an upstream error body reaches the
// cause on the default (unexpected-status) branch, so a large error page
// cannot bloat a log line.
const maxUpstreamBodyLogLen = 256

// truncateUpstreamBody trims body to maxUpstreamBodyLogLen bytes for logging.
func truncateUpstreamBody(body string) string {
	if len(body) <= maxUpstreamBodyLogLen {
		return body
	}
	return body[:maxUpstreamBodyLogLen] + "...(truncated)"
}

// upstreamMessage lifts the human-readable text out of an errcode envelope. The
// upstream `reason` is deliberately NOT copied: reasons are a contract between a
// service and its own clients, and re-emitting another service's would put
// undocumented codes into admin-service's surface.
func upstreamMessage(body, fallback string) string {
	var env struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &env); err == nil && env.Error != "" {
		return env.Error
	}
	return fallback
}

// clientUpdateAuditAction is the audit action for a published artifact pair.
const clientUpdateAuditAction = "client_update.upload"

// quoteEscaper mirrors mime/multipart's own escaping for Content-Disposition
// values, so a filename containing a quote or backslash cannot break out of the
// header it is written into.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

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
	ctx := c.Request.Context()

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
		// Closing the pipe is what unblocks the reader, so it must happen on every
		// path out of this goroutine.
		_ = pw.CloseWithError(relayErr)
		done <- relayResult{names: names, err: relayErr}
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
	res := <-done

	// res.err first, but only when the relay failed on its own terms — the
	// client's body — rather than because we closed the pipe after the upstream
	// rejected the request. pr.CloseWithError injects uploadErr into the pipe, so
	// on a genuine upstream failure res.err wraps it and errors.Is is true; on a
	// truncated or malformed client body res.err is an independent request-side
	// read error. Without this split, a bad client body reports as an upstream
	// outage and sends ops chasing a phantom one.
	if res.err != nil && !errors.Is(res.err, uploadErr) {
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

// extendUploadDeadlines pushes this request's read and write deadlines out to d.
// Verified reachable: gin's responseWriter implements Unwrap, and neither
// o11ygin nor otelgin replaces c.Writer. A failure here is fatal rather than
// ignored — a silent no-op would kill every large upload at the server's 15s
// read timeout, with nothing in the logs to explain it.
func extendUploadDeadlines(c *gin.Context, d time.Duration) error {
	rc := http.NewResponseController(c.Writer)
	deadline := time.Now().Add(d)
	if err := rc.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("extend upload read deadline: %w", err)
	}
	if err := rc.SetWriteDeadline(deadline); err != nil {
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
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return names, fmt.Errorf("read upload part: %w", err)
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
	names[part.FormName()] = part.FileName()

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
