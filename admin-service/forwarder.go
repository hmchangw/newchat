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

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// The two multipart fields client-update-service requires.
const (
	configFileField  = "configFile"
	executeFileField = "executeFile"
)

// upstreamBodyLimit bounds how much of a downstream error body we read, so a
// misbehaving upstream cannot push an unbounded string into our response.
const upstreamBodyLimit = 4 << 10

// quoteEscaper mirrors mime/multipart's own escaping. CR and LF become %0D/%0A
// so a crafted upload filename cannot inject extra headers into the part.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

// uploadedNames records which artifacts a forward carried, for the audit entry.
type uploadedNames struct {
	ConfigFile  string
	ExecuteFile string
}

// clientUpdateForwarder streams an artifact pair to client-update-service under
// a freshly minted service-account token.
type clientUpdateForwarder struct {
	http      *http.Client
	baseURL   string
	mintToken func() (string, error)
}

// newClientUpdateForwarder builds the forwarder. mintToken yields one bearer
// token per forward.
func newClientUpdateForwarder(baseURL string, timeout time.Duration, mintToken func() (string, error)) *clientUpdateForwarder {
	return &clientUpdateForwarder{
		// A raw *http.Client rather than the resty client itself: resty v2
		// materializes any io.Reader body it cannot natively replay
		// (createHTTPRequest -> getBodyCopy -> io.ReadAll), which is precisely
		// the OOM this streaming path exists to avoid. Built through restyutil
		// so the shared transport, OTel instrumentation and timeout still apply.
		// Same documented exception as pkg/drive/uploader.go.
		http:      restyutil.New(baseURL, restyutil.WithTimeout(timeout)).GetClient(),
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		mintToken: mintToken,
	}
}

// Forward re-encodes src part-by-part into a request to client-update-service.
// Nothing is buffered whole and nothing touches disk, so peak memory is one copy
// buffer regardless of artifact size.
//
// Because the body is piped it carries no Content-Length and is sent chunked;
// the downstream reads it with c.FormFile, which handles that normally.
func (f *clientUpdateForwarder) Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error) {
	token, err := f.mintToken()
	if err != nil {
		return uploadedNames{}, fmt.Errorf("mint service token: %w", err)
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// Built before the copier goroutine starts, and using only pr (which
	// already exists): an error here (e.g. a malformed configured baseURL)
	// returns before anything is spawned, so there is no reader-less
	// goroutine left blocked forever on its first pw.Write.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.baseURL+"/api/v1/version", pr)
	if err != nil {
		return uploadedNames{}, fmt.Errorf("build client-update request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	// Buffered so the copier never blocks handing back what it saw.
	done := make(chan struct {
		names uploadedNames
		err   error
	}, 1)

	go func() {
		names, err := copyParts(src, mw)
		if err == nil {
			err = mw.Close()
		}
		done <- struct {
			names uploadedNames
			err   error
		}{names, err}
		// A non-nil error surfaces on the reader side, so the in-flight request
		// fails rather than sending a silently truncated body.
		_ = pw.CloseWithError(err)
	}()

	resp, doErr := f.http.Do(req)
	// net/http closes req.Body on the way out, which unblocks the copier if it
	// is mid-write, so this receive cannot deadlock.
	copied := <-done

	// A genuine copier error is the truer cause: the request failed because we
	// stopped feeding it. Report that rather than the transport symptom.
	//
	// But io.ErrClosedPipe specifically is not genuine: net/http closes
	// req.Body (our pr) synchronously on several failure paths — notably a
	// dial failure, before RoundTrip even returns — and that race reliably
	// wins over the copier goroutine. In that case the copier's error is
	// itself a downstream symptom, not an independent cause, so fall through
	// and let doErr / resp settle it.
	if copied.err != nil && !errors.Is(copied.err, io.ErrClosedPipe) {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return copied.names, copied.err
	}
	if doErr != nil {
		return copied.names, errcode.Unavailable("client-update-service is unreachable",
			errcode.WithReason(errcode.AdminUpstreamUnavailable), errcode.WithCause(doErr))
	}
	// doErr == nil means resp is the authoritative answer, even if the copier
	// also saw io.ErrClosedPipe: net/http's roundTrip returns as soon as
	// response headers arrive, racing ahead of the still-in-flight request
	// write — the canonical case being an upstream that rejects the request
	// (401/403) before reading the body at all. That ErrClosedPipe carries no
	// information the response doesn't, so it must never override resp.
	defer resp.Body.Close()
	return copied.names, classifyUpstream(resp)
}

// copyParts streams each part of src into dst, preserving field name, filename
// and declared content type, and recording the two artifact names as it goes.
//
// An unknown field is refused before its bytes are streamed, so a client cannot
// push a large body into a field nothing reads. A MISSING field cannot be
// detected here — that is only knowable at EOF, by which point the upload is
// spent — so client-update-service's own 400 covers it.
func copyParts(src *multipart.Reader, dst *multipart.Writer) (uploadedNames, error) {
	var names uploadedNames
	for {
		part, err := src.NextPart()
		if errors.Is(err, io.EOF) {
			return names, nil
		}
		if err != nil {
			return names, errcode.BadRequest("malformed multipart upload",
				errcode.WithCause(fmt.Errorf("read upload part: %w", err)))
		}

		field, filename := part.FormName(), part.FileName()
		switch field {
		case configFileField:
			names.ConfigFile = filename
		case executeFileField:
			names.ExecuteFile = filename
		default:
			_ = part.Close()
			return names, errcode.BadRequest(
				fmt.Sprintf("unexpected form field %q: only %s and %s are accepted",
					field, configFileField, executeFileField))
		}

		hdr := textproto.MIMEHeader{}
		//nolint:gocritic // sprintfQuotedString: the literal " chars are RFC 2183 param
		// delimiters, not Go quoting — %q would re-escape the already
		// quoteEscaper-escaped value and change its meaning.
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			quoteEscaper.Replace(field), quoteEscaper.Replace(filename)))
		// Preserve the declared type: the downstream only falls back to
		// application/x-yaml when a part declares none, so dropping this would
		// store the descriptor as octet-stream.
		if ct := part.Header.Get("Content-Type"); ct != "" {
			hdr.Set("Content-Type", ct)
		}
		w, err := dst.CreatePart(hdr)
		if err != nil {
			_ = part.Close()
			return names, fmt.Errorf("create forwarded part %q: %w", field, err)
		}
		if _, err := io.Copy(w, part); err != nil {
			_ = part.Close()
			return names, fmt.Errorf("stream part %q: %w", field, err)
		}
		if err := part.Close(); err != nil {
			return names, fmt.Errorf("close part %q: %w", field, err)
		}
	}
}

// classifyUpstream turns client-update-service's answer into nil or the error
// admin-service should return.
//
// A 401/403 means OUR credential was refused — a key, issuer, audience or
// allowlist misconfiguration — not the admin's session. Relaying it as a 401
// would tell an authenticated admin their own login failed and send them to
// debug the wrong thing entirely, so it becomes a 503 with a distinct reason.
func classifyUpstream(resp *http.Response) error {
	switch {
	case resp.StatusCode < http.StatusMultipleChoices:
		return nil
	case resp.StatusCode == http.StatusBadRequest:
		return errcode.BadRequest(upstreamMessage(resp))
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return errcode.Unavailable("client-update-service rejected this service's credential",
			errcode.WithReason(errcode.AdminUpstreamUnauthorized))
	default:
		return errcode.Unavailable("client-update-service could not store the upload",
			errcode.WithReason(errcode.AdminUpstreamUnavailable))
	}
}

// upstreamMessage reads the downstream errcode envelope's user-safe message.
// The envelope marshals its message under "error" (see pkg/errcode.Error).
func upstreamMessage(resp *http.Response) string {
	const fallback = "client-update-service rejected the upload"
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamBodyLimit))
	if err != nil || len(body) == 0 {
		return fallback
	}
	var env struct {
		Message string `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Message == "" {
		return fallback
	}
	return env.Message
}
