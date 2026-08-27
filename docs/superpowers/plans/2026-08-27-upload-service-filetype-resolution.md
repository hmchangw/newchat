# Upload-Service `fileType` Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `POST /api/v1/file/rooms/:roomId/upload/file` from returning `fileType: "application/octet-stream"` for files whose client declared no useful type, by resolving the real MIME from the file's bytes and name server-side.

**Architecture:** One new pure resolver in `upload-service/mediatype.go` — `resolveMediaType(declared, filename, reader)` — layered declared → byte sniff → extension table → `application/octet-stream`. `HandleUploadFile` opens the upload *before* the allow/deny check so the resolver can read it, then filters on the **resolved** type instead of the declared one. No config, no store, no interface, no dependency changes.

**Tech Stack:** Go 1.25, stdlib only (`net/http`, `mime`, `io`, `path/filepath`, `strings`, `errors`, `fmt`), `stretchr/testify` for assertions, `go.uber.org/mock` for the existing store mock.

**Spec:** None as a separate file — this is a bounded fix. The design was brainstormed in-session on 2026-08-27 and is recorded verbatim in **§Design Decisions** below; that section is the spec this plan argues from.

**Branch:** `claude/upload-service-filetype-yf92un`

---

## Design Decisions

Decisions taken during brainstorming. Do not re-litigate them mid-execution; if one turns out to be wrong, stop and raise it.

1. **Resolution order is declared → sniff → extension → fallback.** A client that declares a *specific* type (`text/csv`, `application/pdf`) knows more than a sniffer, so its value wins. Only an empty or `application/octet-stream` declaration triggers resolution.
2. **A sniff result of `application/zip`, `text/plain`, `text/xml`, `text/html`, or `application/octet-stream` is treated as INCONCLUSIVE**, and the extension table answers instead. This is not optional polish — `http.DetectContentType` reports every OOXML file (`.docx`/`.xlsx`/`.pptx`) as `application/zip` and every SVG as `text/xml` or `text/plain`. A strict sniff-first order would return `application/zip` for Word documents and would never reach the `.svg` entry that the default blacklist depends on.
3. **The allow/deny filter runs on the RESOLVED type**, not the declared one. Today a client can upload an SVG while declaring `application/octet-stream` and walk straight past the default `FILE_UPLOAD_MEDIA_TYPE_BLACKLIST=image/svg+xml`. Closing that is the point. **Accepted consequence:** an upload that used to succeed under a generic label can now be rejected with `file type is not allowed`.
4. **The extension table is hand-maintained in-repo.** The runtime image is bare `alpine:3.21` with no `mailcap`, so `/etc/mime.types` does not exist and `mime.TypeByExtension` only knows Go's ~16 builtin entries — no `.docx`, `.xlsx`, `.zip`, `.txt`, `.mp3`, `.mp4`. `mime.TypeByExtension` is still consulted as a second lookup, never as the only one.
5. **Out of scope:** `POST /api/v1/file/rooms/:roomId/upload/images`. It returns `{name, status, relativePath}` items with no `fileType` field at all, so this bug cannot reach it. Do not touch `HandleUploadImages` or `preprocessFiles`.

## Global Constraints

- Branch: `claude/upload-service-filetype-yf92un`. Never commit to `master`/`main`.
- **No new third-party dependencies.** Stdlib only. Do not add a MIME-detection library.
- All commands go through `make` targets — never raw `go` commands.
- TDD is mandatory: write the test, run it, watch it fail for the right reason, then implement. Never write implementation before its test exists.
- Tests live in `package main` alongside the code (`upload-service/*_test.go`).
- **No `_test.go` file in `upload-service` may import `image/png` or `image/jpeg`.** Those blank imports live in `dimensions.go` alone; re-registering the decoders from a test would let a deleted production import pass every test while every real upload silently lost its dimensions. See the comment at `upload-service/dimensions_test.go:17-22`. Use the existing embedded `png64x48` / `jpeg32x32` fixtures instead of encoding images in a test.
- Error wrapping style: `fmt.Errorf("short description: %w", err)` describing what the *current* function was doing. Never a bare `err`.
- Client-facing errors go through `pkg/errcode` + `errhttp.Write`. Never log and return the same error — `errhttp.Write` classifies and logs once.
- Comments explain WHY, max ~2 lines, matching the density already in `mediatype.go` and `dimensions.go`.
- Minimum 80% coverage; target 90%+ for handler and resolver code.
- Every commit must pass the pre-commit hook (lint + tests). Fix failures, do not bypass the hook.

## File Structure

| File | Responsibility |
|---|---|
| `upload-service/mediatype.go` (modify) | Already owns MIME normalization and the allow/deny filter. Gains the resolution layer: `extensionMediaTypes`, `mediaTypeByExtension`, `sniffLen`, `sniffMediaType`, `inconclusiveSniffTypes`, `resolveMediaType`. All MIME knowledge stays in one ~200-line file. |
| `upload-service/mediatype_test.go` (modify) | Table-driven tests for each new function. Already holds `TestMediaTypeFilter_Allowed`; keep that test untouched. |
| `upload-service/handler.go` (modify, `HandleUploadFile` only, currently lines 272-299) | Reorders open-before-filter and calls the resolver. No other handler changes. |
| `upload-service/handler_test.go` (modify) | New end-to-end cases through `HandleUploadFile`. Reuses the existing `multipartTyped`, `newUploadCtx`, `fileHandler`, `okFileDrive`, `okUser` helpers — do not redefine them. |
| `docs/client-api.md` (modify) | §2.4 upload/file request table + success JSON example; §3.0 `Attachment.fileType` row. |
| `docs/client-api/request-reply.md` (modify) | Derived view — one clause so it does not drift from the canonical doc. |

**Existing symbols the tasks depend on** (already in the package, do not recreate):

- `normalizeMediaType(v string) string` — `mediatype.go:79`. Lowercases, trims, drops `;`-parameters. Returns `""` for a blank input.
- `defaultUploadContentType = "application/octet-stream"` — `handler.go:36`.
- `(*mediaTypeFilter).allowed(mime string) bool` — `mediatype.go:43`.
- `imageDimensions(r io.ReadSeeker, mime string) (*model.ImageDimensions, error)` — `dimensions.go:24`. Rewinds `r` itself; no-ops on a non-`image/` MIME.
- `png64x48 []byte`, `jpeg32x32 []byte` — embedded fixtures, `dimensions_test.go:23-27`.
- `seekFailReader{io.Reader, seeks int, seekErr error}` — `dimensions_test.go:78`. A readable stream whose `Seek` always fails. **Reuse it; do not define a second one** (duplicate type name = compile error).
- `multipartTyped(t, field, filename string, data []byte, mime string, fields map[string]string) (*bytes.Buffer, string)` — `handler_test.go:981`. Builds a one-file multipart body with an explicit part `Content-Type`.
- `newUploadCtx(t, roomID string, body *bytes.Buffer, contentType string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder)` — `handler_test.go:97`.
- `fileHandler(store Store, fd *fakeDrive) *Handler` — `handler_test.go:995`. Wires `newMediaTypeFilter("", "image/svg+xml")`, i.e. the production default blacklist.
- `okFileDrive() *fakeDrive`, `okUser() *AuthenticatedUser` — `handler_test.go:999`, `handler_test.go`.

**Import-shadowing note for Task 1:** `mediatype.go` currently has functions with a parameter named `mime` (`allowed(mime string)`, `matchSet(..., mime string)`, `matchMediaType(pattern, mime string)`). Adding `import "mime"` to that file is legal — the shadow is function-scoped and those functions never use the package. Leave the existing parameter names alone. Only if `make lint` flags it (`importShadow`), alias the import as `gomime "mime"` and adjust the single call site.

---

### Task 1: Extension → MIME table

**Files:**
- Modify: `upload-service/mediatype.go`
- Test: `upload-service/mediatype_test.go`

**Interfaces:**
- Consumes: `normalizeMediaType(v string) string` (existing, same file).
- Produces: `mediaTypeByExtension(filename string) string` — returns a normalized MIME type, or `""` when the extension is unknown or absent. Used by Task 3.

- [ ] **Step 1: Write the failing test**

Append to `upload-service/mediatype_test.go`:

```go
func TestMediaTypeByExtension(t *testing.T) {
	tests := []struct {
		name, filename, want string
	}{
		{"docx from our table", "report.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{"xlsx from our table", "budget.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{"txt is absent from Go's builtin table", "notes.txt", "text/plain"},
		{"zip", "bundle.zip", "application/zip"},
		{"svg", "logo.svg", "image/svg+xml"},
		{"mp4", "clip.mp4", "video/mp4"},
		{"mp3", "song.mp3", "audio/mpeg"},
		{"uppercase extension", "REPORT.DOCX", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{"falls through to Go's builtin table", "photo.png", "image/png"},
		{"builtin charset parameter is stripped", "page.html", "text/html"},
		{"last extension wins", "archive.tar.gz", "application/gzip"},
		{"dots in the stem", "my.report.final.pdf", "application/pdf"},
		{"unknown extension", "data.zzz", ""},
		{"no extension", "README", ""},
		{"empty name", "", ""},
		{"dotfile has no extension to speak of", ".gitignore", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, mediaTypeByExtension(tc.filename))
		})
	}
}
```

`mediatype_test.go` currently imports only `testing`. Add the testify import:

```go
import (
	"testing"

	"github.com/stretchr/testify/assert"
)
```

Note on the `.gitignore` case: `filepath.Ext(".gitignore")` returns `".gitignore"`, which is in neither table, so the result is `""`. The case is there to pin that behavior, not to request special handling.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: mediaTypeByExtension`.

- [ ] **Step 3: Write the implementation**

Add to `upload-service/mediatype.go`. The import block becomes:

```go
import (
	"mime"
	"path/filepath"
	"strings"
)
```

Then append:

```go
// extensionMediaTypes maps a lowercase extension to its MIME type for the types
// Go's own table lacks. The runtime image is bare alpine with no /etc/mime.types,
// so mime.TypeByExtension knows only ~16 builtin entries — without this table
// every Office document, archive and media file resolves to nothing.
var extensionMediaTypes = map[string]string{
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".doc":  "application/msword",
	".xls":  "application/vnd.ms-excel",
	".ppt":  "application/vnd.ms-powerpoint",
	".odt":  "application/vnd.oasis.opendocument.text",
	".ods":  "application/vnd.oasis.opendocument.spreadsheet",
	".odp":  "application/vnd.oasis.opendocument.presentation",
	".zip":  "application/zip",
	".7z":   "application/x-7z-compressed",
	".rar":  "application/vnd.rar",
	".gz":   "application/gzip",
	".tar":  "application/x-tar",
	".txt":  "text/plain",
	".log":  "text/plain",
	".csv":  "text/csv",
	".md":   "text/markdown",
	".rtf":  "application/rtf",
	".svg":  "image/svg+xml",
	".heic": "image/heic",
	".heif": "image/heif",
	".bmp":  "image/bmp",
	".tif":  "image/tiff",
	".tiff": "image/tiff",
	".mp3":  "audio/mpeg",
	".m4a":  "audio/mp4",
	".wav":  "audio/wav",
	".ogg":  "audio/ogg",
	".aac":  "audio/aac",
	".mp4":  "video/mp4",
	".mov":  "video/quicktime",
	".webm": "video/webm",
	".mkv":  "video/x-matroska",
	".avi":  "video/x-msvideo",
}

// mediaTypeByExtension resolves a file name's extension to a MIME type: our own
// table first, then Go's builtin one. Returns "" when neither knows it.
func mediaTypeByExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		return ""
	}
	if mt, ok := extensionMediaTypes[ext]; ok {
		return mt
	}
	return normalizeMediaType(mime.TypeByExtension(ext))
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS, including the pre-existing `TestMediaTypeFilter_Allowed`.

- [ ] **Step 5: Commit**

```bash
git add upload-service/mediatype.go upload-service/mediatype_test.go
git commit -m "feat(upload-service): map file extensions to MIME types

The runtime image carries no /etc/mime.types, so mime.TypeByExtension
knows only Go's builtin entries — no Office, archive or media types."
```

---

### Task 2: Byte sniffing with rewind

**Files:**
- Modify: `upload-service/mediatype.go`
- Test: `upload-service/mediatype_test.go`

**Interfaces:**
- Consumes: `normalizeMediaType` (existing).
- Produces: `sniffMediaType(r io.ReadSeeker) (string, error)` — normalized `http.DetectContentType` result for the first 512 bytes, with `r` rewound to the start before returning. Errors only when `r` cannot be read or cannot be rewound. Used by Task 3.

- [ ] **Step 1: Write the failing test**

Append to `upload-service/mediatype_test.go`:

```go
// pdfBytes and zipBytes are the magic-number prefixes http.DetectContentType
// keys on; the rest of a real file is irrelevant to the sniff.
var (
	pdfBytes = []byte("%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\n")
	zipBytes = []byte("PK\x03\x04\x14\x00\x06\x00\x08\x00\x00\x00!\x00")
	svgBytes = []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>`)
)

func TestSniffMediaType(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"png fixture", png64x48, "image/png"},
		{"jpeg fixture", jpeg32x32, "image/jpeg"},
		{"pdf", pdfBytes, "application/pdf"},
		{"zip prefix, as every OOXML file sniffs", zipBytes, "application/zip"},
		{"svg sniffs as xml, not as an image", svgBytes, "text/xml"},
		{"plain text", []byte("hello, world"), "text/plain"},
		{"charset parameter is stripped", []byte("hello"), "text/plain"},
		{"empty file", []byte{}, "text/plain"},
		{"opaque binary", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}, "application/octet-stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			got, err := sniffMediaType(r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			rest, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tc.data, rest, "reader must be left at the start for the upload")
		})
	}
}

// A file larger than the sniff window must still be fully readable afterwards.
func TestSniffMediaType_RewindsPastSniffWindow(t *testing.T) {
	data := append(append([]byte{}, pdfBytes...), bytes.Repeat([]byte("a"), 4096)...)
	r := bytes.NewReader(data)

	got, err := sniffMediaType(r)
	require.NoError(t, err)
	assert.Equal(t, "application/pdf", got)

	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, rest)
}

// A reader that cannot be rewound is unusable for the upload that follows, so
// the sniff must surface an error rather than hand back a half-consumed file.
func TestSniffMediaType_RewindError(t *testing.T) {
	seekErr := errors.New("seek boom")
	r := &seekFailReader{Reader: bytes.NewReader(png64x48), seekErr: seekErr}

	_, err := sniffMediaType(r)
	require.Error(t, err)
	assert.ErrorIs(t, err, seekErr)
}

func TestSniffMediaType_ReadError(t *testing.T) {
	readErr := errors.New("read boom")
	r := &seekFailReader{Reader: iotest.ErrReader(readErr)}

	_, err := sniffMediaType(r)
	require.Error(t, err)
	assert.ErrorIs(t, err, readErr)
}
```

The test file's imports become:

```go
import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)
```

`seekFailReader` embeds `io.Reader`, so `&seekFailReader{Reader: iotest.ErrReader(readErr)}` fails on read before any seek is attempted — its zero `seekErr` is never reached.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: sniffMediaType`.

- [ ] **Step 3: Write the implementation**

The `mediatype.go` import block becomes:

```go
import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)
```

Append:

```go
// sniffLen is the number of leading bytes http.DetectContentType inspects.
const sniffLen = 512

// sniffMediaType detects a MIME type from a file's first bytes and rewinds r so
// the caller can still upload it. Only the header is read, so a large upload is
// never held in memory; a reader we cannot rewind is unusable for the upload
// that follows, which makes it the one real error here (as in imageDimensions).
func sniffMediaType(r io.ReadSeeker) (string, error) {
	head := make([]byte, sniffLen)
	n, err := io.ReadFull(r, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", fmt.Errorf("read file header for type detection: %w", err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind file after type detection: %w", err)
	}
	return normalizeMediaType(http.DetectContentType(head[:n])), nil
}
```

A short file yields `io.ErrUnexpectedEOF` and an empty one `io.EOF`; both are normal, so only a third kind of error aborts.

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add upload-service/mediatype.go upload-service/mediatype_test.go
git commit -m "feat(upload-service): sniff an upload's MIME type from its first bytes

Reads only the 512-byte sniff window and rewinds, so the same reader
still streams to Drive afterwards."
```

---

### Task 3: `resolveMediaType` — the layered resolver

**Files:**
- Modify: `upload-service/mediatype.go`
- Test: `upload-service/mediatype_test.go`

**Interfaces:**
- Consumes: `normalizeMediaType`, `defaultUploadContentType` (`handler.go:36`), `sniffMediaType` (Task 2), `mediaTypeByExtension` (Task 1).
- Produces: `resolveMediaType(declared, filename string, r io.ReadSeeker) (string, error)` — the MIME type to record and to filter on, with `r` rewound. Used by Task 4.

- [ ] **Step 1: Write the failing test**

Append to `upload-service/mediatype_test.go`:

```go
func TestResolveMediaType(t *testing.T) {
	tests := []struct {
		name, declared, filename string
		data                     []byte
		want                     string
	}{
		{
			name: "a specific declared type wins over the bytes",
			declared: "text/csv", filename: "data.bin", data: pdfBytes, want: "text/csv",
		},
		{
			name: "declared type keeps only its base, not its parameters",
			declared: "text/csv; charset=utf-8", filename: "data.csv", data: []byte("a,b\n1,2"), want: "text/csv",
		},
		{
			name: "octet-stream is resolved from the bytes",
			declared: "application/octet-stream", filename: "photo.png", data: png64x48, want: "image/png",
		},
		{
			name: "octet-stream is matched case-insensitively",
			declared: "APPLICATION/OCTET-STREAM", filename: "photo.png", data: png64x48, want: "image/png",
		},
		{
			name: "an absent declared type is resolved from the bytes",
			declared: "", filename: "photo.jpg", data: jpeg32x32, want: "image/jpeg",
		},
		{
			name: "a zip sniff defers to the extension, so docx is not application/zip",
			declared: "application/octet-stream", filename: "report.docx", data: zipBytes,
			want: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		},
		{
			name: "a genuine zip still resolves to zip via its own extension",
			declared: "application/octet-stream", filename: "bundle.zip", data: zipBytes, want: "application/zip",
		},
		{
			name: "an xml sniff defers to the extension, so svg is not text/xml",
			declared: "application/octet-stream", filename: "logo.svg", data: svgBytes, want: "image/svg+xml",
		},
		{
			name: "a text sniff defers to the extension",
			declared: "application/octet-stream", filename: "notes.csv", data: []byte("a,b\n1,2"), want: "text/csv",
		},
		{
			name: "a conclusive sniff beats a lying extension",
			declared: "application/octet-stream", filename: "report.docx", data: pdfBytes, want: "application/pdf",
		},
		{
			name: "an inconclusive sniff with an unknown extension keeps the sniff result",
			declared: "application/octet-stream", filename: "notes.zzz", data: []byte("hello"), want: "text/plain",
		},
		{
			name: "opaque bytes with no usable extension stay octet-stream",
			declared: "application/octet-stream", filename: "blob", data: []byte{0x00, 0x01, 0xff}, want: "application/octet-stream",
		},
		{
			name: "an empty file falls back to its extension",
			declared: "", filename: "empty.docx", data: []byte{},
			want: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			got, err := resolveMediaType(tc.declared, tc.filename, r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			rest, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tc.data, rest, "reader must be left at the start for the upload")
		})
	}
}

// A specific declared type is answered without touching the reader at all.
func TestResolveMediaType_SpecificDeclaredTypeSkipsTheSniff(t *testing.T) {
	r := &seekFailReader{Reader: bytes.NewReader(pdfBytes), seekErr: errors.New("seek boom")}

	got, err := resolveMediaType("application/pdf", "report.pdf", r)
	require.NoError(t, err)
	assert.Equal(t, "application/pdf", got)
	assert.Zero(t, r.seeks, "a specific declared type must not read the file")
}

func TestResolveMediaType_RewindError(t *testing.T) {
	seekErr := errors.New("seek boom")
	r := &seekFailReader{Reader: bytes.NewReader(png64x48), seekErr: seekErr}

	_, err := resolveMediaType("application/octet-stream", "photo.png", r)
	require.Error(t, err)
	assert.ErrorIs(t, err, seekErr)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: resolveMediaType`.

- [ ] **Step 3: Write the implementation**

Append to `upload-service/mediatype.go`:

```go
// inconclusiveSniffTypes are the results http.DetectContentType returns for
// whole families of formats: every OOXML document sniffs as application/zip and
// every text-based format as text/plain or text/xml. Specific enough to stop a
// naive sniff-first resolution, so the extension gets to answer these instead.
var inconclusiveSniffTypes = map[string]struct{}{
	"application/zip":          {},
	"application/octet-stream": {},
	"text/plain":               {},
	"text/xml":                 {},
	"text/html":                {},
}

// resolveMediaType picks the MIME type to record for an upload. A specific
// declared type wins — the client knows its own file. Otherwise the bytes and
// then the name decide, so a client that says nothing (or the generic
// application/octet-stream a browser sends for any file the OS cannot type)
// still gets a real type instead of an opaque one. r is rewound before return.
func resolveMediaType(declared, filename string, r io.ReadSeeker) (string, error) {
	if d := normalizeMediaType(declared); d != "" && d != defaultUploadContentType {
		return d, nil
	}
	sniffed, err := sniffMediaType(r)
	if err != nil {
		return "", fmt.Errorf("detect media type from file contents: %w", err)
	}
	if _, weak := inconclusiveSniffTypes[sniffed]; sniffed != "" && !weak {
		return sniffed, nil
	}
	if byExt := mediaTypeByExtension(filename); byExt != "" {
		return byExt, nil
	}
	if sniffed != "" {
		return sniffed, nil
	}
	return defaultUploadContentType, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add upload-service/mediatype.go upload-service/mediatype_test.go
git commit -m "feat(upload-service): resolve an upload's real MIME type

Declared type first when it is specific, then the bytes, then the
extension. A zip or text sniff defers to the extension so OOXML
documents and SVGs resolve to their own type rather than the container's."
```

---

### Task 4: Wire the resolver into `HandleUploadFile`

**Files:**
- Modify: `upload-service/handler.go` (`HandleUploadFile`, currently lines 272-299)
- Test: `upload-service/handler_test.go`

**Interfaces:**
- Consumes: `resolveMediaType(declared, filename string, r io.ReadSeeker) (string, error)` (Task 3).
- Produces: no new exported surface. `fileMeta.mime` and `Attachment.FileType` now carry the resolved type.

- [ ] **Step 1: Write the failing tests**

Append to `upload-service/handler_test.go`. `fileHandler` already wires the production default blacklist (`image/svg+xml`), so the third test exercises the real configuration.

```go
// The reported bug: a client that declares application/octet-stream (every
// browser does, for any file the OS cannot type by extension) used to get that
// value echoed back as fileType, and lost the image fields with it.
func TestHandleUploadFile_OctetStreamResolvesFromBytes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	body, ct := multipartTyped(t, "file", "photo.png", png64x48, "application/octet-stream", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Attachments, 1)
	att := resp.Attachments[0]
	assert.Equal(t, "image/png", att.FileType)
	assert.Equal(t, "image/png", att.ImageType)
	assert.NotEmpty(t, att.ImageURL)
	require.NotNil(t, att.ImageDimensions, "the image branch must run on the resolved type")
	assert.Equal(t, 64, att.ImageDimensions.Width)
	assert.Equal(t, 48, att.ImageDimensions.Height)
}

// A docx sniffs as application/zip, so only the extension can name it.
func TestHandleUploadFile_OctetStreamResolvesFromExtension(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	body, ct := multipartTyped(t, "file", "report.docx", zipBytes, "application/octet-stream", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Attachments, 1)
	assert.Equal(t, "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		resp.Attachments[0].FileType)
	assert.Empty(t, resp.Attachments[0].ImageURL, "a document carries no image fields")
}

// Filtering the resolved type closes the bypass: declaring octet-stream no
// longer smuggles a blacklisted SVG past FILE_UPLOAD_MEDIA_TYPE_BLACKLIST.
func TestHandleUploadFile_BlockedAfterResolution(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	fd := okFileDrive()

	body, ct := multipartTyped(t, "file", "logo.svg", svgBytes, "application/octet-stream", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, fd).HandleUploadFile(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Zero(t, fd.uploadGot.n, "a rejected file must never reach Drive")
}

// A client that names a specific type is still taken at its word.
func TestHandleUploadFile_SpecificDeclaredTypeIsKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	body, ct := multipartTyped(t, "file", "data.bin", []byte("a,b\n1,2"), "text/csv", nil)
	c, w := newUploadCtx(t, "room-1", body, ct, okUser())
	fileHandler(store, okFileDrive()).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Attachments, 1)
	assert.Equal(t, "text/csv", resp.Attachments[0].FileType)
}
```

`zipBytes` and `svgBytes` are declared in `mediatype_test.go` (Task 2) and are visible here — same package. Do not redeclare them.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL. `TestHandleUploadFile_OctetStreamResolvesFromBytes` fails on `"application/octet-stream" != "image/png"`; `TestHandleUploadFile_BlockedAfterResolution` fails with `200 != 400`. `TestHandleUploadFile_SpecificDeclaredTypeIsKept` passes already — that is expected, it is a regression guard for behavior this task must preserve.

- [ ] **Step 3: Write the implementation**

In `upload-service/handler.go`, replace this block (currently lines 272-299) —

```go
	// Normalize the (client-controlled) declared type: lowercase + strip params so
	// the filter and the image branch see a clean value.
	mime := normalizeMediaType(fh.Header.Get("Content-Type"))
	if mime == "" {
		mime = defaultUploadContentType
	}
	if !h.mimeFilter.allowed(mime) {
		errhttp.Write(ctx, c, errcode.BadRequest("file type is not allowed"))
		return
	}

	// The upload is handed to both the header read and Drive as a reader, so the
	// file is never held in memory whatever its type or size.
	driveFile, err := fh.Open()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("open uploaded file: %w", err))
		return
	}
	defer driveFile.Close()
```

— with:

```go
	// The upload is handed to the resolver, the header read and Drive as a reader,
	// so the file is never held in memory whatever its type or size.
	driveFile, err := fh.Open()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("open uploaded file: %w", err))
		return
	}
	defer driveFile.Close()

	// The declared Content-Type is a client-controlled hint — browsers send
	// application/octet-stream for any file the OS cannot type — so the real type
	// comes from the bytes and the name. Filtering THAT rather than the declared
	// value is what stops a blacklisted upload arriving under a generic label.
	mime, err := resolveMediaType(fh.Header.Get("Content-Type"), fh.Filename, driveFile)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("resolve uploaded file media type: %w", err))
		return
	}
	if !h.mimeFilter.allowed(mime) {
		errhttp.Write(ctx, c, errcode.BadRequest("file type is not allowed"))
		return
	}
```

Everything below is unchanged: `imageDimensions(driveFile, mime)`, the Drive upload, and `fileMeta{... mime: mime ...}` all now see the resolved value.

Check the imports afterwards: `normalizeMediaType` may no longer be called from `handler.go`, but it still lives in `mediatype.go` (same package) so nothing to remove. If `make lint` reports `defaultUploadContentType` as unused in `handler.go`, leave the constant where it is — `resolveMediaType` references it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — the four new tests plus every pre-existing one. `TestHandleUploadFile_Success` (declares `application/pdf`), `TestHandleUploadFile_ImageSuccess` (declares `image/png`) and `TestHandleUploadFile_BlockedMIME` (declares `image/svg+xml`) must all still pass untouched. **If any pre-existing test needs editing to go green, stop and report it** — that would mean the change broke behavior this plan intends to preserve.

- [ ] **Step 5: Verify coverage**

Run: `go test -coverprofile=coverage.out ./upload-service/ && go tool cover -func=coverage.out | grep -E "mediatype|HandleUploadFile"`
Expected: `resolveMediaType`, `sniffMediaType`, `mediaTypeByExtension` and `HandleUploadFile` each at or above 80%. Add cases for any uncovered branch. Delete `coverage.out` before committing.

- [ ] **Step 6: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "fix(upload-service): return the real fileType for an uploaded file

The declared multipart Content-Type was echoed straight into the
attachment, so every client that sends application/octet-stream got that
back as fileType and lost the image/audio/video fields with it. Resolve
the type from the file itself, and run the allow/deny lists on the
resolved value so a generic label no longer slips a blocked type past."
```

---

### Task 5: Documentation and full verification

**Files:**
- Modify: `docs/client-api.md` (§2.4 upload/file; §3.0 `Attachment`)
- Modify: `docs/client-api/request-reply.md` (derived view, upload/file entry)

**Interfaces:**
- Consumes: the behavior shipped in Task 4.
- Produces: nothing consumed by later tasks — this is the final task.

- [ ] **Step 1: Update the canonical request table**

In `docs/client-api.md` §2.4, the `file` row of the request field table currently ends with:

> `Its MIME type must pass the server's allow/deny lists (`FILE_UPLOAD_MEDIA_TYPE_WHITELIST`/`BLACKLIST`; `image/svg+xml` is blocked by default).`

Replace that sentence with:

> The part's `Content-Type` is a hint, not the answer: when it is absent or `application/octet-stream`, the server derives the type from the file's leading bytes and its extension. The **resolved** type is what must pass the server's allow/deny lists (`FILE_UPLOAD_MEDIA_TYPE_WHITELIST`/`BLACKLIST`; `image/svg+xml` is blocked by default) and what comes back as `fileType`.

- [ ] **Step 2: Update the success example**

In the same section, the success JSON example omits `fileType`. Add it as the last field of the attachment object:

```json
{
  "success": true,
  "attachments": [
    {
      "id": "drive-file-1",
      "title": "report.pdf",
      "type": "file",
      "description": "Q2 report",
      "titleLink": "api/v1/file/rooms/abc123/file/drive-file-1?drive_host=https://drive.example.com",
      "titleLinkDownload": true,
      "fileType": "application/pdf"
    }
  ]
}
```

- [ ] **Step 3: Update the `Attachment.fileType` row**

In `docs/client-api.md` §3.0, the `fileType` row (currently "Optional. Canonical lowercased MIME type, present on every attachment family.") becomes:

```markdown
| `fileType` | string | Optional. Canonical lowercased MIME type, present on every attachment family. Server-derived on upload — a declared `application/octet-stream` is replaced by the type detected from the file's bytes and extension. |
```

- [ ] **Step 4: Update the derived view**

In `docs/client-api/request-reply.md`, the `POST /api/v1/file/rooms/:roomId/upload/file` entry: append one sentence to its description paragraph, before the "See" link:

> The returned `fileType` is server-derived — the part's declared `Content-Type` is used only when it is specific.

- [ ] **Step 5: Run the full gate**

```bash
make fmt
make lint
make test SERVICE=upload-service
make sast
```

Expected: all four clean. `make sast` must show no medium-or-higher findings — it is a blocking CI gate. If `gosec` flags the `head[:n]` slice or the `make([]byte, sniffLen)` allocation, do not blanket-suppress: re-read the finding, and only add `// #nosec <RULE> -- reason` directly above the statement if it is a genuine false positive.

- [ ] **Step 6: Run the whole test suite**

Run: `make test`
Expected: PASS. Nothing outside `upload-service` should be affected; if something else fails, stop and report it rather than adjusting the other package.

- [ ] **Step 7: Commit and push**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): fileType on upload is server-derived

The upload endpoint no longer echoes the declared Content-Type, and the
allow/deny lists now apply to the resolved type."
git push -u origin claude/upload-service-filetype-yf92un
```

On a network failure, retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s). Do not open a pull request — the user has not asked for one.

---

## Verification Checklist

Before reporting the work complete, confirm each of these with actual command output, not from memory:

- [ ] `make test SERVICE=upload-service` passes, including every pre-existing test, unedited.
- [ ] `make lint` clean.
- [ ] `make sast` clean at medium and above.
- [ ] `make test` (whole repo) passes.
- [ ] Coverage for `resolveMediaType`, `sniffMediaType`, `mediaTypeByExtension`, `HandleUploadFile` is ≥ 80%.
- [ ] `git status` clean — no stray `coverage.out`, no `.env`.
- [ ] The branch is `claude/upload-service-filetype-yf92un` and it is pushed.

## Known Behavior Changes to Call Out in the Handoff

1. An upload declaring `application/octet-stream` whose resolved type is on the blacklist is now **rejected** where it previously succeeded. With the default configuration this is exactly one type: SVG. Say so explicitly when reporting the work.
2. Files that previously came back as plain `type: "file"` attachments may now carry `imageUrl`/`imageDimensions`, `audioUrl` or `videoUrl`, because `buildAttachment` branches on the resolved type. Clients already handle those fields — they are what a correctly-declared upload has always produced.
