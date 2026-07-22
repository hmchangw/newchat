# Drive Download Response Headers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set `Content-Disposition`, `Content-Security-Policy`, and `Cache-Control` response headers on the Drive file-download endpoint, at parity with the legacy MinIO/S3 download endpoint.

**Architecture:** Lift the filename off the Drive storage response's own `Content-Disposition` header in `pkg/drive` (exposed as a new `GetGroupImageResponse.Filename` field), then have `upload-service`'s `HandleDownloadFile` emit the three headers via a `contentDisposition` helper that is shared with — and refactored out of — the existing MinIO/S3 handler.

**Tech Stack:** Go 1.25, Gin, Resty, `mime` (stdlib), `go.uber.org/mock`, `stretchr/testify`.

## Global Constraints

- Use `make` targets, never raw `go` commands. `make test SERVICE=<path>` runs `go test -race ./<path>/...`, so scope with `SERVICE=upload-service` and `SERVICE=pkg/drive`; `make test` runs the whole repo.
- TDD is mandatory: Red → Green → Refactor → Commit. Never write implementation before a failing test.
- Minimum 80% coverage; target 90%+ for handlers and `pkg/` code.
- Error wrapping: `fmt.Errorf("short description: %w", err)` — describe what the current function was doing.
- No new third-party dependencies — `mime`, `net/url`, `strings`, `fmt` are all stdlib.
- `upload-service` HTTP routes are not `chat.user.` NATS subjects → **no `docs/client-api.md` update required**.
- The MinIO/S3 path's observable output must stay byte-identical after the helper refactor.
- Commit messages end with the Co-Authored-By / Claude-Session trailer lines used elsewhere on this branch. Do NOT include the model identifier anywhere in commits.

---

### Task 1: Expose the storage filename from `pkg/drive`

Add a `Filename` field to `GetGroupImageResponse` and populate it in `GetGroupImage` by parsing the storage response's `Content-Disposition` header.

**Files:**
- Modify: `pkg/drive/images_file.go` (struct `GetGroupImageResponse`, ~lines 31-36)
- Modify: `pkg/drive/uploader.go` (func `GetGroupImage`, ~lines 113-146; add `mime` import)
- Test: `pkg/drive/uploader_test.go` (extend `TestClient_GetGroupImage_Success`, add `TestClient_GetGroupImage_ForwardsFilename` and `TestClient_GetGroupImage_NoDisposition`)

**Interfaces:**
- Produces: `drive.GetGroupImageResponse` gains `Filename string`. It is `""` when the storage response has no `Content-Disposition` or the header cannot be parsed; otherwise the parsed `filename`/`filename*` value.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/drive/uploader_test.go`. These mirror the existing `TestClient_GetGroupImage_Success` mux setup but assert on `img.Filename`, and the `/img` stub sets a `Content-Disposition`.

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/drive` — if that target does not resolve a `pkg/` service, run the repo test suite: `make test` and confirm these two new tests fail.
Expected: FAIL — `img.Filename` undefined (compile error) because the field does not exist yet.

- [ ] **Step 3: Add the `Filename` field**

In `pkg/drive/images_file.go`, extend the struct:

```go
// GetGroupImageResponse carries a streamed download body plus metadata.
type GetGroupImageResponse struct {
	Reader        io.ReadCloser
	ContentType   string
	ContentLength int64
	Filename      string
}
```

- [ ] **Step 4: Populate `Filename` in `GetGroupImage`**

In `pkg/drive/uploader.go`, add `"mime"` to the import block, then parse the header just before building the response. Replace the tail of `GetGroupImage` (the `contentType` resolution + `return`) with:

```go
	contentType := resp.Header().Get("Content-Type")
	if contentType == "" {
		contentType = defaultContentType
	}
	var contentLength int64
	if resp.RawResponse != nil {
		contentLength = resp.RawResponse.ContentLength
	}
	return &GetGroupImageResponse{
		Reader:        resp.RawBody(),
		ContentType:   contentType,
		ContentLength: contentLength,
		Filename:      filenameFromDisposition(resp.Header().Get("Content-Disposition")),
	}, nil
}

// filenameFromDisposition parses the filename out of a Content-Disposition
// header value, returning "" when the header is absent or unparseable. It
// prefers the RFC 5987 filename* parameter and falls back to filename.
func filenameFromDisposition(v string) string {
	if v == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(v)
	if err != nil {
		return ""
	}
	if name := params["filename*"]; name != "" {
		return name
	}
	return params["filename"]
}
```

(`mime.ParseMediaType` decodes RFC 5987 `filename*` into `params["filename*"]` as a decoded UTF-8 value.)

- [ ] **Step 5: Update the existing success test to assert the empty default**

In `TestClient_GetGroupImage_Success`, the `/img` stub sets no `Content-Disposition`, so add one assertion after the body check to lock in the default:

```go
	if img.Filename != "" {
		t.Fatalf("filename = %q, want empty", img.Filename)
	}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/drive` (or `make test`).
Expected: PASS — all three `TestClient_GetGroupImage_*` filename assertions green.

- [ ] **Step 7: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 8: Commit**

```bash
git add pkg/drive/images_file.go pkg/drive/uploader.go pkg/drive/uploader_test.go
git commit -m "feat(drive): expose storage filename on GetGroupImageResponse

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011aQMKUU89RiYi1LZhg7GQW"
```

---

### Task 2: Add the shared `contentDisposition` helper and refactor the MinIO/S3 handler onto it

Extract the RFC 5987 `Content-Disposition` formatting that is currently inlined in `HandleDownloadMinioS3File` into a reusable helper, keeping the MinIO/S3 output byte-identical.

**Files:**
- Modify: `upload-service/handler.go` (add `contentDisposition`; refactor `HandleDownloadMinioS3File`, ~lines 393-401)
- Test: `upload-service/handler_test.go` (add `TestContentDisposition`)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `func contentDisposition(name string) string` — returns `attachment; filename*=UTF-8''<enc>` when `name != ""` (where `<enc>` is `url.QueryEscape(name)` with `+` rewritten to `%20`), and `attachment` when `name == ""`.

- [ ] **Step 1: Write the failing test**

Add to `upload-service/handler_test.go`:

```go
func TestContentDisposition(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "utf8 and space", in: "réport space.pdf", want: "attachment; filename*=UTF-8''r%C3%A9port%20space.pdf"},
		{name: "simple", in: "x.pdf", want: "attachment; filename*=UTF-8''x.pdf"},
		{name: "empty", in: "", want: "attachment"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, contentDisposition(tc.in))
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `contentDisposition` undefined (compile error).

- [ ] **Step 3: Add the helper**

In `upload-service/handler.go`, add near the other small helpers (e.g. below `uniqueName`):

```go
// contentDisposition builds an attachment Content-Disposition header value.
// When name is non-empty it appends an RFC 5987 filename*: the name is
// percent-encoded like encodeURIComponent (space -> %20, not +). An empty
// name yields a bare "attachment" so the browser still force-downloads.
func contentDisposition(name string) string {
	if name == "" {
		return "attachment"
	}
	encodedName := strings.ReplaceAll(url.QueryEscape(name), "+", "%20")
	return fmt.Sprintf("attachment; filename*=UTF-8''%s", encodedName)
}
```

- [ ] **Step 4: Refactor `HandleDownloadMinioS3File` to use the helper**

In `HandleDownloadMinioS3File`, replace the inlined encoding + map entry. Change:

```go
	// RFC 5987 filename*: percent-encode UTF-8 like encodeURIComponent (space -> %20, not +).
	encodedName := strings.ReplaceAll(url.QueryEscape(up.Name), "+", "%20")
	extraHeaders := map[string]string{
		"Content-Disposition":     fmt.Sprintf("attachment; filename*=UTF-8''%s", encodedName),
		"Content-Security-Policy": "default-src 'none'",
		// private: this response is authorization-gated (auth + room membership),
		// so only the user agent may cache it — never a shared/intermediary cache.
		"Cache-Control": fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
```

to:

```go
	extraHeaders := map[string]string{
		"Content-Disposition":     contentDisposition(up.Name),
		"Content-Security-Policy": "default-src 'none'",
		// private: this response is authorization-gated (auth + room membership),
		// so only the user agent may cache it — never a shared/intermediary cache.
		"Cache-Control": fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — `TestContentDisposition` green AND the pre-existing `TestS3Download_Success_StreamsWithHeaders` (which asserts `attachment; filename*=UTF-8''r%C3%A9port%20space.pdf`) still green, proving byte-identical output.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: no new findings. If `net/url` becomes unused elsewhere it will still be used by the helper, so the import stays.

- [ ] **Step 7: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "refactor(upload-service): extract shared contentDisposition helper

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011aQMKUU89RiYi1LZhg7GQW"
```

---

### Task 3: Emit the three headers on the Drive download path

Wire the headers into `HandleDownloadFile`, using the forwarded filename from Task 1 and the helper from Task 2.

**Files:**
- Modify: `upload-service/handler.go` (`HandleDownloadFile`, ~lines 343-345)
- Test: `upload-service/handler_test.go` (extend `fakeDrive.getResp` usage; update `TestDownload_Success_StreamsBinary`; add `TestDownload_Success_NoFilename`)

**Interfaces:**
- Consumes: `drive.GetGroupImageResponse.Filename` (Task 1); `contentDisposition` (Task 2).
- Produces: `HandleDownloadFile` responses now carry `Content-Disposition`, `Content-Security-Policy: default-src 'none'`, and `Cache-Control: private, max-age=<cacheMaxAge>`.

- [ ] **Step 1: Write the failing tests**

Update `TestDownload_Success_StreamsBinary` to set a filename and assert all three headers, and add a no-filename fallback test. Replace the existing `TestDownload_Success_StreamsBinary` body's `fakeDrive` literal and add assertions:

```go
func TestDownload_Success_StreamsBinary(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	fd := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader:        readCloser{strings.NewReader("PNGDATA")},
		ContentType:   "image/png",
		ContentLength: 7,
		Filename:      "réport space.png",
	}}
	h := newHandler(store, fd)
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	assert.Equal(t, "PNGDATA", w.Body.String())
	assert.Equal(t, "https://d.example.com", fd.getGot.host)
	assert.Equal(t, "r1", fd.getGot.groupID)
	assert.Equal(t, "f1", fd.getGot.fileID)
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control"))
	assert.Equal(t, "attachment; filename*=UTF-8''r%C3%A9port%20space.png", w.Header().Get("Content-Disposition"))
}

func TestDownload_Success_NoFilename(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	fd := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader:        readCloser{strings.NewReader("PNGDATA")},
		ContentType:   "image/png",
		ContentLength: 7,
	}}
	h := newHandler(store, fd)
	c, w := newDownloadCtx(t, "r1", "f1", "https://d.example.com", okUser())
	h.HandleDownloadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "attachment", w.Header().Get("Content-Disposition"))
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control"))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `TestDownload_Success_StreamsBinary` and `TestDownload_Success_NoFilename` fail on the missing header assertions (headers are empty because `HandleDownloadFile` still passes `map[string]string{}`). The `Filename` field itself compiles (added in Task 1).

- [ ] **Step 3: Set the headers in `HandleDownloadFile`**

In `upload-service/handler.go`, replace the final line of `HandleDownloadFile`:

```go
	// GetGroupImage already defaults ContentType to application/octet-stream, so
	// stream the body straight through with no intermediate buffering.
	c.DataFromReader(http.StatusOK, img.ContentLength, img.ContentType, img.Reader, map[string]string{})
```

with:

```go
	// GetGroupImage already defaults ContentType to application/octet-stream, so
	// stream the body straight through with no intermediate buffering. The
	// download headers mirror the legacy MinIO/S3 path: force-download,
	// lock down execution, and allow private (per-user) caching only — the
	// response is authorization-gated (auth + room membership).
	extraHeaders := map[string]string{
		"Content-Disposition":     contentDisposition(img.Filename),
		"Content-Security-Policy": "default-src 'none'",
		"Cache-Control":           fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
	c.DataFromReader(http.StatusOK, img.ContentLength, img.ContentType, img.Reader, extraHeaders)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — both download success tests green.

- [ ] **Step 5: Run the full service + drive suites and lint**

Run: `make test SERVICE=upload-service && make test SERVICE=pkg/drive && make lint`
Expected: PASS, no new lint findings.

- [ ] **Step 6: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "feat(upload-service): set download headers on drive file endpoint

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011aQMKUU89RiYi1LZhg7GQW"
```

---

### Task 4: SAST + push

Final gate before review.

**Files:** none (verification only).

- [ ] **Step 1: Run SAST**

Run: `make sast`
Expected: no medium+ findings. (No new `InsecureSkipVerify`, unsafe conversions, or tainted sinks were introduced; `mime.ParseMediaType` on a response header is not a SAST concern.)

- [ ] **Step 2: Push the branch**

```bash
git push -u origin claude/drive-api-response-headers-si5pzz
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) only on network errors.

- [ ] **Step 3: Report**

Summarize the change and confirm all suites + SAST are green. Do NOT open a PR unless explicitly requested.

---

## Self-Review

**Spec coverage:**
- Spec §1 (`pkg/drive` filename) → Task 1. ✓
- Spec §2 (helper + Drive headers) → Task 2 (helper + MinIO refactor) and Task 3 (Drive headers). ✓
- Spec §3 (inline `<img>` safety) → design note, no code; covered by the comment added in Task 3 and no behavioural change to embedded rendering. ✓
- Spec Testing (pkg/drive present/absent; handler forwarded + fallback; helper encoding) → Task 1 Steps 1&5, Task 2 Step 1, Task 3 Step 1. ✓
- Spec scope (no client-api.md, no new deps) → Global Constraints. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**Type consistency:** `GetGroupImageResponse.Filename` (Task 1) is consumed as `img.Filename` (Task 3); `contentDisposition(name string) string` (Task 2) called with `up.Name` (Task 2) and `img.Filename` (Task 3) — signatures match. `filenameFromDisposition` is internal to `pkg/drive`. ✓
