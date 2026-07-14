package drive

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	if gotToken != "tok" || gotUserID != "alice" || gotFileName != "a.png" || gotMode != "Normal" {
		t.Fatalf("token=%q userId=%q fileName=%q mode=%q", gotToken, gotUserID, gotFileName, gotMode)
	}
}

func TestClient_UploadGroupImages_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
		[]MultipartFile{{File: fakeMultipart("x"), Filename: "a.png"}})
	if err == nil {
		t.Fatal("expected error on 500")
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
