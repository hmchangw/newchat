package drive

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_GetBaseURLFromRoomOrigin(t *testing.T) {
	c := NewClient(&Config{
		URL:        "https://default.example.com",
		Token:      "tok",
		BaseURLMap: map[string]string{"site-a": "https://a.example.com"},
	})
	if got := c.GetBaseURL(); got != "https://default.example.com" {
		t.Fatalf("GetBaseURL = %q", got)
	}
	if got := c.GetBaseURLFromRoomOrigin("site-a"); got != "https://a.example.com" {
		t.Fatalf("known origin = %q", got)
	}
	if got := c.GetBaseURLFromRoomOrigin("unknown"); got != "https://default.example.com" {
		t.Fatalf("unknown origin should fall back to base, got %q", got)
	}
}

func TestClient_UploadGroupImages(t *testing.T) {
	var gotPath, gotBypass, gotToken, gotUserID, gotFileName, gotMode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBypass = r.URL.Query().Get("bypass")
		gotToken = r.Header.Get("api-token")
		// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
		_ = r.ParseMultipartForm(10 << 20)
		gotUserID = r.FormValue("userId")
		gotFileName = r.FormValue("files[0].fileName")
		gotMode = r.FormValue("files[0].mode")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"status":"Success","object":{"objectId":"f1","groupId":"r1","fileName":"a.png"}}]`)
	}))
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
		[]MultipartFile{{File: fakeMultipart("aaa"), Filename: "a.png"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp) != 1 || resp[0].Status != "Success" || resp[0].File.FileID != "f1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if gotPath != "/api/v1/groups/r1/files/bulk" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotBypass != "true" {
		t.Fatalf("bypass query param = %q, want %q", gotBypass, "true")
	}
	if gotToken != "tok" || gotUserID != "alice" || gotFileName != "a.png" || gotMode != "KeepBoth" {
		t.Fatalf("token=%q userId=%q fileName=%q mode=%q", gotToken, gotUserID, gotFileName, gotMode)
	}
}

// partContentTypeServer captures the Content-Type of every uploaded file part,
// keyed by the part's form field name.
func partContentTypeServer(t *testing.T, got map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			t.Errorf("parse multipart form: %v", err)
		}
		for field, headers := range r.MultipartForm.File {
			got[field] = headers[0].Header.Get("Content-Type")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
}

// Magic-byte prefixes are all http.DetectContentType needs to classify a part.
const (
	pngMagic  = "\x89PNG\r\n\x1a\n....."
	jpegMagic = "\xff\xd8\xff....."
	pdfMagic  = "%PDF-1.7\n....."
	// HEIC is an ISO-BMFF 'ftyp' box. Go's sniffer only recognizes the mp4 brand,
	// so heic falls through to octet-stream — the one format sniffing can't name.
	heicMagic = "\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1heic"
)

// The part Content-Type is sniffed from the leading bytes, so the true media type
// reaches Drive without any caller having to declare it.
func TestClient_UploadGroupImages_SniffsPartContentType(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		body     string
		want     string
	}{
		{name: "png", filename: "a.png", body: pngMagic, want: "image/png"},
		{name: "jpeg", filename: "a.jpg", body: jpegMagic, want: "image/jpeg"},
		{name: "pdf", filename: "a.pdf", body: pdfMagic, want: "application/pdf"},
		{name: "text", filename: "a.txt", body: "hello there", want: "text/plain; charset=utf-8"},
		{name: "heic sniffs as octet-stream", filename: "a.heic", body: heicMagic, want: defaultContentType},
		{name: "unrecognized binary", filename: "a.bin", body: "\x07\x08\x09\x00\x01\x02\x03", want: defaultContentType},
		// DetectContentType treats no bytes as text, not octet-stream.
		{name: "empty body sniffs as text", filename: "a.bin", body: "", want: "text/plain; charset=utf-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]string{}
			srv := partContentTypeServer(t, got)
			defer srv.Close()

			c := NewClient(&Config{URL: srv.URL, Token: "tok"})
			_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
				[]MultipartFile{{File: fakeMultipart(tt.body), Filename: tt.filename}})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got["files[0].file"] != tt.want {
				t.Fatalf("part content type = %q, want %q", got["files[0].file"], tt.want)
			}
		})
	}
}

// Sniffing must not consume the leading bytes: Drive has to receive the whole file.
func TestClient_UploadGroupImages_SniffPreservesFullBody(t *testing.T) {
	body := pngMagic + strings.Repeat("payload", 200)
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			t.Errorf("parse multipart form: %v", err)
		}
		f, err := r.MultipartForm.File["files[0].file"][0].Open()
		if err != nil {
			t.Errorf("open part: %v", err)
			return
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			t.Errorf("read part: %v", err)
		}
		got = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
		[]MultipartFile{{File: fakeMultipart(body), Filename: "a.png"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != body {
		t.Fatalf("part body = %d bytes, want %d (sniffing must not swallow the prefix)", len(got), len(body))
	}
}

// Multiple files must each be sniffed independently and keep their index mapping.
func TestClient_UploadGroupImages_SniffsEachPartIndependently(t *testing.T) {
	got := map[string]string{}
	srv := partContentTypeServer(t, got)
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", []MultipartFile{
		{File: fakeMultipart(pngMagic), Filename: "a.png"},
		{File: fakeMultipart(pdfMagic), Filename: "b.pdf"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["files[0].file"] != "image/png" {
		t.Fatalf("part 0 content type = %q, want image/png", got["files[0].file"])
	}
	if got["files[1].file"] != "application/pdf" {
		t.Fatalf("part 1 content type = %q, want application/pdf", got["files[1].file"])
	}
}

func TestClient_UploadGroupImages_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"quota exceeded"}`)
	}))
	defer srv.Close()
	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
		[]MultipartFile{{File: fakeMultipart("x"), Filename: "a.png"}})
	require.ErrorContains(t, err, "status 500")
	require.ErrorContains(t, err, "quota exceeded", "the response snippet must reach the error for diagnosis")
}

// TestClient_UploadGroupImages_TransportAndDecodeErrors covers the two error
// paths around the HTTP round trip: the request never gets a response (network
// error) and a 2xx response whose body isn't valid JSON (decode error).
func TestClient_UploadGroupImages_TransportAndDecodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) string // returns the client base URL
		wantErr string
	}{
		{
			name: "network error: server unreachable",
			setup: func(t *testing.T) string {
				srv := httptest.NewServer(http.NotFoundHandler())
				srv.Close() // closed before use: Do() fails with a connection error
				return srv.URL
			},
			wantErr: "upload group images",
		},
		{
			name: "response body is not valid JSON",
			setup: func(t *testing.T) string {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, "not valid json")
				}))
				t.Cleanup(srv.Close)
				return srv.URL
			},
			wantErr: "decode drive bulk upload response",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(&Config{URL: tt.setup(t), Token: "tok"})
			_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
				[]MultipartFile{{File: fakeMultipart("x"), Filename: "a.png"}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// fakeFile adapts a string to multipart.File for tests.
type fakeFile struct{ *strings.Reader }

func (fakeFile) Close() error               { return nil }
func fakeMultipart(s string) multipart.File { return fakeFile{strings.NewReader(s)} }

func TestClient_GetGroupImage_Success(t *testing.T) {
	mux := http.NewServeMux()
	// signer returns a presigned URL pointing back at /img on the same server.
	var base string
	mux.HandleFunc("/api/v1/groups/r1/files/f1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"`+base+`/img"}`)
	})
	mux.HandleFunc("/img", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("PNGDATA"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	base = srv.URL

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	img, err := c.GetGroupImage(srv.URL, "r1", "f1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer img.Reader.Close()
	if img.ContentType != "image/png" {
		t.Fatalf("content type = %q", img.ContentType)
	}
	body, _ := io.ReadAll(img.Reader)
	if string(body) != "PNGDATA" {
		t.Fatalf("body = %q", string(body))
	}
	if img.Filename != "" {
		t.Fatalf("filename = %q, want empty", img.Filename)
	}
}

func TestClient_GetGroupImage_ForwardsFilename(t *testing.T) {
	mux := http.NewServeMux()
	var base string
	mux.HandleFunc("/api/v1/groups/r1/files/f1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"`+base+`/img"}`)
	})
	mux.HandleFunc("/img", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Disposition", `attachment; filename="report.png"`)
		_, _ = w.Write([]byte("PNGDATA"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	base = srv.URL

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	img, err := c.GetGroupImage(srv.URL, "r1", "f1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer img.Reader.Close()
	if img.Filename != "report.png" {
		t.Fatalf("filename = %q, want %q", img.Filename, "report.png")
	}
}

func TestClient_GetGroupImage_NoDisposition(t *testing.T) {
	mux := http.NewServeMux()
	var base string
	mux.HandleFunc("/api/v1/groups/r1/files/f1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"`+base+`/img"}`)
	})
	mux.HandleFunc("/img", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("PNGDATA"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	base = srv.URL

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	img, err := c.GetGroupImage(srv.URL, "r1", "f1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer img.Reader.Close()
	if img.Filename != "" {
		t.Fatalf("filename = %q, want empty", img.Filename)
	}
}

func TestClient_GetGroupImage_EmptyURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":""}`)
	}))
	defer srv.Close()
	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	if _, err := c.GetGroupImage(srv.URL, "r1", "f1"); err == nil {
		t.Fatal("expected error on empty signer URL")
	}
}

func TestClient_GetGroupImage_SignerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"not found"}`)
	}))
	defer srv.Close()
	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	if _, err := c.GetGroupImage(srv.URL, "r1", "f1"); err == nil {
		t.Fatal("expected error on signer 404")
	}
}

func TestClient_GetGroupImage_DownloadNotFound(t *testing.T) {
	mux := http.NewServeMux()
	var base string
	mux.HandleFunc("/api/v1/groups/r1/files/f1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"`+base+`/img"}`)
	})
	mux.HandleFunc("/img", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	base = srv.URL

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.GetGroupImage(srv.URL, "r1", "f1")
	if err == nil || !strings.Contains(err.Error(), "image not found") {
		t.Fatalf("expected image-not-found error, got %v", err)
	}
}
