package drive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)
	total := int64(fileSize) * fileCount
	require.GreaterOrEqual(t, got, total, "every file must arrive whole")
	require.LessOrEqualf(t, peak, uint64(maxPeak),
		"peak heap %d MiB while uploading %d MiB across %d files: the body is being buffered in memory",
		peak>>20, total>>20, fileCount)
	t.Logf("streamed %d MiB across %d files with a %d MiB peak heap", total>>20, fileCount, peak>>20)
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
	require.NoError(t, err)
	require.Empty(t, te, "want a Content-Length body, not chunked transfer-encoding")
	require.Equal(t, received, declared, "the declared Content-Length must be exact")
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
			require.ErrorContains(t, err, tt.wantErr)
			require.Zero(t, hits, "the upload must fail before any request reaches Drive")
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
				// assert (never require) here: the handler runs off the test
				// goroutine, where FailNow is not allowed.
				// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
				if err := r.ParseMultipartForm(10 << 20); !assert.NoError(t, err, "parse multipart form") {
					return
				}
				f, err := r.MultipartForm.File["files[0].file"][0].Open()
				if !assert.NoError(t, err, "open part") {
					return
				}
				defer f.Close()
				b, err := io.ReadAll(f)
				assert.NoError(t, err, "read part")
				got = string(b)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `[]`)
			}))
			defer srv.Close()

			c := NewClient(&Config{URL: srv.URL, Token: "tok"})
			_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
				[]MultipartFile{{File: fakeMultipart(want), Filename: "a.bin"}})
			require.NoError(t, err)
			require.Equal(t, want, got, "the part body must arrive whole")
		})
	}
}

// A hostile filename must not be able to inject headers into the multipart
// body. quoteEscaper percent-encodes CR/LF in the Content-Disposition filename
// (stdlib leaves them raw); if that regressed, the injected "Content-Type:
// text/html" would parse as the file part's real header. The files[N].fileName
// form FIELD legitimately carries the raw filename — CR/LF in a field value
// live in the body region, which cannot terminate a header (old resty parity).
func TestClient_UploadGroupImages_HostileFilename(t *testing.T) {
	const payload = "PAYLOAD"
	hostile := "evil\"\r\nContent-Type: text/html\r\n\r\n.png"

	var raw []byte
	var boundary string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// assert (never require) here: the handler runs off the test goroutine,
		// where FailNow is not allowed.
		var err error
		raw, err = io.ReadAll(r.Body)
		assert.NoError(t, err, "read raw body")
		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if assert.NoError(t, err, "parse content-type") {
			boundary = params["boundary"]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()

	c := NewClient(&Config{URL: srv.URL, Token: "tok"})
	_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
		[]MultipartFile{{File: fakeMultipart(payload), Filename: hostile}})
	require.NoError(t, err)

	mr := multipart.NewReader(bytes.NewReader(raw), boundary)
	var fileParts int
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "a hostile filename must not break the body")
		if part.FileName() == "" {
			continue // envelope form fields
		}
		fileParts++
		// The injection signature: raw CR/LF in the filename would end the
		// Content-Disposition line and make text/html this part's real header.
		require.Equal(t, "text/plain; charset=utf-8", part.Header.Get("Content-Type"),
			"an injected Content-Type must never override the sniffed one")
		require.Contains(t, part.Header.Get("Content-Disposition"), "%0D%0A",
			"CR/LF must arrive percent-encoded in the header")
		require.NotContains(t, part.FileName(), "\r")
		require.NotContains(t, part.FileName(), "\n")
		b, err := io.ReadAll(part)
		require.NoError(t, err, "read part")
		require.Equal(t, payload, string(b), "the part body must arrive whole")
	}
	require.Equal(t, 1, fileParts, "an injection would have split the file part")
}
