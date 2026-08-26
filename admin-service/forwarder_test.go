package main

import (
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
)

// fwdTestForm builds a multipart body and returns a reader over it plus its
// boundary, mimicking what Gin hands the handler.
func fwdTestForm(t *testing.T, parts []struct{ field, name, body, contentType string }) (*multipart.Reader, string) {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		for _, p := range parts {
			var w io.Writer
			var err error
			if p.contentType != "" {
				h := textproto.MIMEHeader{}
				h.Set("Content-Disposition", `form-data; name="`+p.field+`"; filename="`+p.name+`"`)
				h.Set("Content-Type", p.contentType)
				w, err = mw.CreatePart(h)
			} else {
				w, err = mw.CreateFormFile(p.field, p.name)
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, err := io.WriteString(w, p.body); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
		_ = pw.CloseWithError(mw.Close())
	}()
	return multipart.NewReader(pr, mw.Boundary()), mw.Boundary()
}

// twoGoodParts is the ordinary, valid submission.
func twoGoodParts() []struct{ field, name, body, contentType string } {
	return []struct{ field, name, body, contentType string }{
		{configFileField, "app.yaml", "version: 3", "application/x-yaml"},
		{executeFileField, "app.exe", "MZbinarybytes", ""},
	}
}

func staticToken(tok string) func() (string, error) {
	return func() (string, error) { return tok, nil }
}

func TestForward_StreamsBothPartsAndAuthenticates(t *testing.T) {
	var (
		gotAuth       string
		gotConfig     string
		gotExecute    string
		gotConfigType string
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		mr, err := r.MultipartReader()
		require.NoError(t, err)
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			body, err := io.ReadAll(part)
			require.NoError(t, err)
			switch part.FormName() {
			case configFileField:
				gotConfig = string(body)
				gotConfigType = part.Header.Get("Content-Type")
			case executeFileField:
				gotExecute = string(body)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("minted-token"))
	src, _ := fwdTestForm(t, twoGoodParts())

	names, err := f.Forward(context.Background(), src)
	require.NoError(t, err)

	assert.Equal(t, "Bearer minted-token", gotAuth, "the forward must carry the minted service token")
	assert.Equal(t, "version: 3", gotConfig)
	assert.Equal(t, "MZbinarybytes", gotExecute)
	assert.Equal(t, "application/x-yaml", gotConfigType,
		"the part's declared content type must survive the re-encode, or the downstream stores the yaml as octet-stream")
	assert.Equal(t, "app.yaml", names.ConfigFile)
	assert.Equal(t, "app.exe", names.ExecuteFile)
}

// TestForward_MapsUpstreamStatuses pins spec §6.1's table. A downstream 401/403
// means OUR credential was refused — a configuration fault — so it must never
// be relayed as the admin's own 401.
func TestForward_MapsUpstreamStatuses(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		wantHTTP     int
		wantReason   errcode.Reason
		wantContains string
	}{
		{"200 succeeds", http.StatusOK, `{"result":"success"}`, 0, "", ""},
		{"400 relays the message", http.StatusBadRequest,
			`{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}`,
			http.StatusBadRequest, "", "configFile must be a .yaml"},
		{"401 becomes 503 upstream_unauthorized", http.StatusUnauthorized,
			`{"code":"unauthenticated","error":"invalid service token"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnauthorized, ""},
		{"403 becomes 503 upstream_unauthorized", http.StatusForbidden,
			`{"code":"forbidden","error":"not authorized"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnauthorized, ""},
		{"500 becomes 503 upstream_unavailable", http.StatusInternalServerError,
			`{"code":"internal","error":"minio down"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnavailable, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Drain so the pipe writer always completes.
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()

			f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("tok"))
			src, _ := fwdTestForm(t, twoGoodParts())

			_, err := f.Forward(context.Background(), src)
			if tc.wantHTTP == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tc.wantHTTP, ec.HTTPStatus())
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, ec.Reason)
			}
			if tc.wantContains != "" {
				assert.Contains(t, ec.Error(), tc.wantContains)
			}
		})
	}
}

// TestForward_UpstreamErrorNeverLeaksTheToken guards the logging rule.
func TestForward_UpstreamErrorNeverLeaksTheToken(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","error":"nope"}`))
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("super-secret-token"))
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "super-secret-token")
}

func TestForward_TransportFailureIsUnavailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := upstream.URL
	upstream.Close() // nothing is listening any more

	f := newClientUpdateForwarder(url, 2*time.Second, staticToken("tok"))
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, http.StatusServiceUnavailable, ec.HTTPStatus())
	assert.Equal(t, errcode.AdminUpstreamUnavailable, ec.Reason)
}

func TestForward_MintFailureIsReported(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be called when minting fails")
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, func() (string, error) {
		return "", assert.AnError
	})
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	assert.Error(t, err)
}

// TestForward_RejectsUnknownField stops a client streaming a large body into a
// field nothing will read. A missing field cannot be caught here — it is only
// knowable at EOF — so the downstream's own 400 covers that case.
func TestForward_RejectsUnknownField(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("tok"))
	src, _ := fwdTestForm(t, []struct{ field, name, body, contentType string }{
		{"surpriseField", "evil.bin", strings.Repeat("x", 128), ""},
	})

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, http.StatusBadRequest, ec.HTTPStatus())
}
