package drive

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bulkRequest is one captured /files/bulk call: the form values Drive received,
// indexed the same way the request numbered its file parts.
type bulkRequest struct {
	userID    string
	fileNames []string
	modes     []string
	bodies    []string
}

// bulkRecorder is a fake Drive bulk endpoint. It records every request and
// replays scripted response bodies in order; a scripted body of "" makes that
// attempt answer 500 instead, standing in for a Drive-side failure.
type bulkRecorder struct {
	mu        sync.Mutex
	requests  []bulkRequest
	responses []string
}

// newBulkServer starts a recorder-backed Drive and returns a client aimed at it.
func newBulkServer(t *testing.T, responses ...string) (*bulkRecorder, *Client) {
	t.Helper()
	rec := &bulkRecorder{responses: responses}
	srv := httptest.NewServer(rec.serve(t))
	t.Cleanup(srv.Close)
	return rec, NewClient(&Config{URL: srv.URL, Token: "tok"})
}

func (r *bulkRecorder) serve(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, req *http.Request) {
		// #nosec G120 -- test httptest server with a fixed 10MiB bound; not exposed to untrusted traffic
		if err := req.ParseMultipartForm(10 << 20); err != nil {
			t.Errorf("parse multipart form: %v", err)
			return
		}
		got := bulkRequest{userID: req.FormValue("userId")}
		for i := 0; ; i++ {
			name := req.FormValue(fmt.Sprintf("files[%d].fileName", i))
			if name == "" {
				break
			}
			got.fileNames = append(got.fileNames, name)
			got.modes = append(got.modes, req.FormValue(fmt.Sprintf("files[%d].mode", i)))
			got.bodies = append(got.bodies, readFilePart(t, req, i))
		}

		r.mu.Lock()
		r.requests = append(r.requests, got)
		body := ""
		if n := len(r.requests) - 1; n < len(r.responses) {
			body = r.responses[n]
		}
		r.mu.Unlock()

		if body == "" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":"drive down"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

func (r *bulkRecorder) snapshot() []bulkRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bulkRequest(nil), r.requests...)
}

// readFilePart returns the full bytes Drive received for file part i.
func readFilePart(t *testing.T, req *http.Request, i int) string {
	t.Helper()
	headers := req.MultipartForm.File[fmt.Sprintf("files[%d].file", i)]
	if len(headers) == 0 {
		return ""
	}
	f, err := headers[0].Open()
	if err != nil {
		t.Errorf("open part %d: %v", i, err)
		return ""
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("read part %d: %v", i, err)
	}
	return string(b)
}

// pngFiles stages one distinguishable PNG per name; each body is pngMagic+name.
func pngFiles(names ...string) []MultipartFile {
	files := make([]MultipartFile, len(names))
	for i, n := range names {
		files[i] = MultipartFile{File: fakeMultipart(pngMagic + n), Filename: n}
	}
	return files
}

// A conflict is per-file, so only the conflicting file is re-sent under
// KeepBoth — re-sending the whole batch would store a second copy of every file
// that already succeeded.
func TestClient_UploadGroupImages_RetriesOnlyConflictedFiles(t *testing.T) {
	attempt1 := `[{"status":"success","object":{"objectId":"f0","groupId":"r1","fileName":"a.png"}},
	              {"status":"failure","error":"file conflict: b.png already exists"},
	              {"status":"success","object":{"objectId":"f2","groupId":"r1","fileName":"c.png"}}]`
	attempt2 := `[{"status":"success","object":{"objectId":"f1kb","groupId":"r1","fileName":"b (1).png"}}]`
	rec, c := newBulkServer(t, attempt1, attempt2)

	resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", pngFiles("a.png", "b.png", "c.png"))
	require.NoError(t, err)

	reqs := rec.snapshot()
	require.Len(t, reqs, 2, "one conflicting file must trigger exactly one retry")
	assert.Equal(t, []string{"Normal", "Normal", "Normal"}, reqs[0].modes)
	assert.Equal(t, []string{"b.png"}, reqs[1].fileNames, "only the conflicting file is re-sent")
	assert.Equal(t, []string{"KeepBoth"}, reqs[1].modes)
	assert.Equal(t, []string{pngMagic + "b.png"}, reqs[1].bodies,
		"the retry must re-read the file from the start, whole")
	assert.Equal(t, "alice", reqs[1].userID, "the retry carries the same identity fields")

	require.Len(t, resp, 3)
	assert.Equal(t, "f0", resp[0].File.FileID)
	assert.Equal(t, "f1kb", resp[1].File.FileID, "the retry result replaces the conflict in place")
	assert.Equal(t, "success", resp[1].Status)
	assert.Empty(t, resp[1].Error)
	assert.Equal(t, "f2", resp[2].File.FileID)
}

// Every file conflicting is the ordinary re-upload case: all of them go back
// under KeepBoth, renumbered from zero, and merge back in order.
func TestClient_UploadGroupImages_RetriesEveryConflictedFile(t *testing.T) {
	attempt1 := `[{"status":"failure","error":"file conflict"},{"status":"failure","error":"file conflict"}]`
	attempt2 := `[{"status":"success","object":{"objectId":"k0"}},{"status":"success","object":{"objectId":"k1"}}]`
	rec, c := newBulkServer(t, attempt1, attempt2)

	resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", pngFiles("a.png", "b.png"))
	require.NoError(t, err)

	reqs := rec.snapshot()
	require.Len(t, reqs, 2)
	assert.Equal(t, []string{"a.png", "b.png"}, reqs[1].fileNames)
	assert.Equal(t, []string{"KeepBoth", "KeepBoth"}, reqs[1].modes)
	assert.Equal(t, []string{pngMagic + "a.png", pngMagic + "b.png"}, reqs[1].bodies)

	require.Len(t, resp, 2)
	assert.Equal(t, "k0", resp[0].File.FileID)
	assert.Equal(t, "k1", resp[1].File.FileID)
}

// Only a conflict earns a retry: any other per-file failure is Drive's final
// answer, and re-uploading it would just burn the bytes again.
func TestClient_UploadGroupImages_RetryTrigger(t *testing.T) {
	tests := []struct {
		name         string
		result       string
		wantRequests int
	}{
		{
			name:         "conflict is retried",
			result:       `[{"status":"failure","error":"file conflict: a.png already exists"}]`,
			wantRequests: 2,
		},
		{
			name:         "casing of status and error does not matter",
			result:       `[{"status":"Failure","error":"File Conflict detected"}]`,
			wantRequests: 2,
		},
		{
			name:         "another failure reason is not retried",
			result:       `[{"status":"failure","error":"quota exceeded"}]`,
			wantRequests: 1,
		},
		{
			name:         "success is not retried",
			result:       `[{"status":"success","object":{"objectId":"f0"}}]`,
			wantRequests: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, c := newBulkServer(t, tt.result, `[{"status":"success","object":{"objectId":"kb"}}]`)
			_, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
				pngFiles("a.png"))
			require.NoError(t, err)
			assert.Len(t, rec.snapshot(), tt.wantRequests)
		})
	}
}

// A retry that never lands must not sink the whole batch: the files that did
// upload keep their results, and the conflict stays a per-file failure whose
// error says the retry was attempted.
func TestClient_UploadGroupImages_KeepsFirstAttemptWhenRetryFails(t *testing.T) {
	attempt1 := `[{"status":"success","object":{"objectId":"f0"}},
	              {"status":"failure","error":"file conflict: b.png already exists"}]`
	rec, c := newBulkServer(t, attempt1) // no second scripted body: the retry gets a 500

	resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", pngFiles("a.png", "b.png"))
	require.NoError(t, err, "a failed retry must not discard the files that uploaded")
	assert.Len(t, rec.snapshot(), 2)

	require.Len(t, resp, 2)
	assert.Equal(t, "f0", resp[0].File.FileID)
	assert.Equal(t, "failure", resp[1].Status)
	assert.Equal(t, "file conflict: b.png already exists", resp[1].Error,
		"the entry keeps Drive's own message; the retry failure is logged, never handed to the client")
}

// Drive's array is trusted only as far as it lines up with what was sent: an
// entry with no file behind it cannot be retried, and a short retry response
// leaves the entries it did not answer for untouched.
func TestClient_UploadGroupImages_MisalignedResponses(t *testing.T) {
	t.Run("more results than files sent", func(t *testing.T) {
		attempt1 := `[{"status":"failure","error":"file conflict"},{"status":"failure","error":"file conflict"}]`
		attempt2 := `[{"status":"success","object":{"objectId":"kb"}}]`
		rec, c := newBulkServer(t, attempt1, attempt2)

		resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x",
			pngFiles("a.png"))
		require.NoError(t, err)

		reqs := rec.snapshot()
		require.Len(t, reqs, 2)
		assert.Equal(t, []string{"a.png"}, reqs[1].fileNames, "only entries backed by a sent file are retried")
		require.Len(t, resp, 2)
		assert.Equal(t, "kb", resp[0].File.FileID)
		assert.Equal(t, "failure", resp[1].Status, "the unmatched entry is left as Drive reported it")
	})

	t.Run("retry answers for fewer files than it was sent", func(t *testing.T) {
		attempt1 := `[{"status":"failure","error":"file conflict"},{"status":"failure","error":"file conflict"}]`
		rec, c := newBulkServer(t, attempt1, `[{"status":"success","object":{"objectId":"kb"}}]`)

		resp, err := c.UploadGroupImages("alice", "Alice", "a@x.com", "r1", "site-x", pngFiles("a.png", "b.png"))
		require.NoError(t, err)
		assert.Len(t, rec.snapshot(), 2)

		require.Len(t, resp, 2)
		assert.Equal(t, "kb", resp[0].File.FileID)
		assert.Equal(t, "failure", resp[1].Status, "an unanswered retry entry keeps its first-attempt result")
	})
}
