package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
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
			errcode.WithCause(fmt.Errorf("client-update-service returned status %d", status)))
	}
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
