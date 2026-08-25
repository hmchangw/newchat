package drive

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

// The bulk upload must stream: peak memory has to stay flat regardless of file
// size, or a handful of concurrent large uploads OOMs the service.
func TestClient_UploadGroupImages_StreamsWithoutBufferingBody(t *testing.T) {
	// Three files at once mirrors the bulk image endpoint, where the old code
	// concatenated every file into one buffer (MAX_IMAGES x MAX_IMAGE_SIZE_BYTES).
	const fileSize = 40 << 20
	const fileCount = 3
	// Generous: the body is assembled from small envelope snapshots plus the file
	// readers, so anything near the total means the body was buffered.
	const maxPeak = 32 << 20

	var got int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"status":"Success","object":{"objectId":"f1"}}]`)
	}))
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})

	files := make([]MultipartFile, fileCount)
	for i := range files {
		files[i] = MultipartFile{File: &testutil.ZeroReader{N: fileSize}, Filename: fmt.Sprintf("big%d.bin", i)}
	}

	var err error
	peak := testutil.PeakHeapDuring(func() {
		_, err = c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", files)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	total := int64(fileSize) * fileCount
	if got < total {
		t.Fatalf("server received %d bytes, want at least %d — every file must arrive whole", got, total)
	}
	if peak > maxPeak {
		t.Fatalf("peak heap %d MiB while uploading %d MiB across %d files (limit %d MiB): the body is being buffered in memory",
			peak>>20, total>>20, fileCount, maxPeak>>20)
	}
	fmt.Printf("streamed %d MiB across %d files with a %d MiB peak heap\n", total>>20, fileCount, peak>>20)
}

// Streaming must not silently downgrade the request to chunked transfer-encoding:
// the Drive backend has always been sent a Content-Length and may reject a body
// without one, so the exact length is computed up front.
func TestClient_UploadGroupImages_SendsExactContentLength(t *testing.T) {
	var declared, received int64
	var te []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		declared = r.ContentLength
		te = r.TransferEncoding
		received, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", []MultipartFile{
		{File: fakeMultipart(pngMagic + "aaaaaaaaaa"), Filename: "a.png"},
		{File: fakeMultipart(pdfMagic + "bbb"), Filename: "b.pdf"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(te) != 0 {
		t.Fatalf("Transfer-Encoding = %v, want none (a Content-Length body)", te)
	}
	if declared != received {
		t.Fatalf("Content-Length = %d but body was %d bytes — the declared length must be exact", declared, received)
	}
}

// failingFile is a multipart.File whose Seek or Read fails on demand.
type failingFile struct {
	*strings.Reader
	seekErr error
	readErr error
}

func (f *failingFile) Seek(offset int64, whence int) (int64, error) {
	if f.seekErr != nil {
		return 0, f.seekErr
	}
	return f.Reader.Seek(offset, whence)
}

func (f *failingFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.Reader.Read(p)
}

func (f *failingFile) Close() error { return nil }

// A file that cannot be measured or read must fail the upload before any bytes
// go out, rather than sending a body that disagrees with its Content-Length.
func TestClient_UploadGroupImages_UnreadableFile(t *testing.T) {
	tests := []struct {
		name    string
		file    *failingFile
		wantErr string
	}{
		{
			name:    "seek fails",
			file:    &failingFile{Reader: strings.NewReader("data"), seekErr: errors.New("seek boom")},
			wantErr: "measure file 0",
		},
		{
			name:    "read fails",
			file:    &failingFile{Reader: strings.NewReader("data"), readErr: errors.New("read boom")},
			wantErr: "sniff file 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits++
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `[]`)
			}))
			defer srv.Close()

			c := NewClient(&Config{URL: srv.URL, Token: "tok"})
			_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
				[]MultipartFile{{File: tt.file, Filename: "a.png"}})
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to mention %q", err, tt.wantErr)
			}
			if hits != 0 {
				t.Fatalf("Drive was called %d times; the upload must fail before any request", hits)
			}
		})
	}
}

// A short file still reaching Drive whole matters most at the sniff boundary,
// where the first 512 bytes are read out and pushed back onto the chain.
func TestClient_UploadGroupImages_FilesAroundSniffBoundary(t *testing.T) {
	for _, size := range []int{0, 1, sniffLen - 1, sniffLen, sniffLen + 1} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			want := strings.Repeat("x", size)
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
				if err := r.ParseMultipartForm(10 << 20); err != nil {
					t.Errorf("parse multipart form: %v", err)
					return
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
				[]MultipartFile{{File: fakeMultipart(want), Filename: "a.bin"}})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != want {
				t.Fatalf("part body = %d bytes, want %d", len(got), len(want))
			}
		})
	}
}
