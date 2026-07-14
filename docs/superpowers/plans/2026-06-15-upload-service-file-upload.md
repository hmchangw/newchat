# File Upload API (pure HTTP) + remove `File` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /api/v1/rooms/:roomId/upload` to `upload-service` as a pure-HTTP endpoint that stores a file in the Drive and returns a render-ready `attachment`; have `message-gatekeeper` validate `attachments` on `msg.send`; and remove the `File` field/type/column/UDT from the data model, Cassandra schema, message-worker, and history-service.

**Architecture:** upload-service authenticates, checks membership, uploads to Drive, and returns `{success, attachments:[…]}` — no NATS, no message build. The file's identity now lives in `attachment.id`. `File` is deleted everywhere; per-file metadata is carried by the attachment.

**Tech Stack:** Go 1.25, Gin, MongoDB, `pkg/drive`, `golang.org/x/image`, `go.uber.org/mock`, `testify`, gocql/Cassandra.

**Spec:** `docs/superpowers/specs/2026-06-15-upload-service-file-upload-design.md`

**Conventions:** run `make fmt` before each commit (pre-commit hook runs lint + tests). Do NOT put any model identifier in commit messages. Group order is A → B → C; within C, do tasks in numeric order so every commit compiles.

---

# Group A — upload-service (pure HTTP)

## Task A0: Correct the Drive success marker casing

The Drive API returns its per-file status as lowercase `"success"`; the constant
currently holds `"Success"`. Fix the constant and the two test literals that
hardcode the old casing. (All later tasks reference the `driveStatusSuccess`
constant, so they inherit the fix.)

**Files:**
- Modify: `upload-service/handler.go:21`
- Modify: `upload-service/handler_test.go:228,251`

- [ ] **Step 1: Update the test literals to the expected new casing (Red)**

In `upload-service/handler_test.go`:
- line ~228: `{Status: "success", File: drive.GroupImageObject{FileID: "img-xyz", GroupID: "r1", Filename: "a.png"}},`
- line ~251: `if r.Status == "success" {`

- [ ] **Step 2: Run the suite to verify it fails**

Run: `go test ./upload-service/ -run TestHandleUploadImages -v`
Expected: FAIL — the handler still compares against `"Success"`, so the fake's `"success"` response is treated as a failure.

- [ ] **Step 3: Fix the constant (Green)**

In `upload-service/handler.go:21`:

```go
	driveStatusSuccess = "success" // Drive's success marker
```

- [ ] **Step 4: Run the suite to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "fix(upload-service): match Drive's lowercase success marker"
```

---

## Task A1: `Attachment` + `ImageDimensions` (in `pkg/model/cassandra`, aliased in `pkg/model`)

`Attachment` must be defined in `pkg/model/cassandra` (not `pkg/model`): the
history response struct `cassandra.Message` embeds `[]Attachment`, and
`pkg/model` already imports `pkg/model/cassandra`, so the cassandra package
cannot import back into `pkg/model` (import cycle). Aliases in `pkg/model` keep
`model.Attachment` ergonomic for upload-service.

**Files:**
- Create: `pkg/model/cassandra/attachment.go`
- Create: `pkg/model/attachment.go` (aliases)
- Test: `pkg/model/cassandra/attachment_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/model/cassandra/attachment_test.go`:

```go
package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachment_RoundTrip(t *testing.T) {
	att := Attachment{
		ID: "drive-file-1", Title: "photo.png", Type: "file", Description: "team photo",
		TitleLink: "api/v1/rooms/r1/file/drive-file-1?drive_host=h", TitleLinkDownload: true,
		ImageURL: "api/v1/rooms/r1/file/drive-file-1?drive_host=h", ImageType: "image/png",
		ImageSize: 1234, ImageDimensions: &ImageDimensions{Width: 800, Height: 600}, ImagePreview: "b64",
	}
	data, err := json.Marshal(att)
	require.NoError(t, err)
	var got Attachment
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, att, got)

	audio := Attachment{ID: "f2", Title: "a.mp3", Type: "file", TitleLink: "u", TitleLinkDownload: true,
		AudioURL: "u", AudioType: "audio/mpeg", AudioSize: 99}
	ab, err := json.Marshal(audio)
	require.NoError(t, err)
	assert.NotContains(t, string(ab), "imageUrl")
	assert.Contains(t, string(ab), `"audioUrl":"u"`)
	assert.Contains(t, string(ab), `"id":"f2"`)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/model/cassandra/ -run TestAttachment_RoundTrip -v`
Expected: FAIL — `undefined: Attachment`.

- [ ] **Step 3: Create the model + aliases**

Create `pkg/model/cassandra/attachment.go` (json tags only — this type is
serialized whole as a JSON blob and over HTTP; it is never a Cassandra column,
so it needs no `cql`/`bson` tags):

```go
package cassandra

// ImageDimensions is the pixel size of an uploaded image attachment.
type ImageDimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// Attachment is the render-ready descriptor for an uploaded file. upload-service
// returns it over HTTP; the frontend base64-encodes its JSON into each
// Message.Attachments blob; history-service decodes those blobs back into this
// type. ID is the Drive file id; Title is the file name (no separate name).
type Attachment struct {
	ID                string `json:"id"`
	Title             string `json:"title"`
	Type              string `json:"type"`
	Description       string `json:"description,omitempty"`
	TitleLink         string `json:"titleLink"`
	TitleLinkDownload bool   `json:"titleLinkDownload"`

	ImageURL        string           `json:"imageUrl,omitempty"`
	ImageType       string           `json:"imageType,omitempty"`
	ImageSize       int64            `json:"imageSize,omitempty"`
	ImageDimensions *ImageDimensions `json:"imageDimensions,omitempty"`
	ImagePreview    string           `json:"imagePreview,omitempty"`

	AudioURL  string `json:"audioUrl,omitempty"`
	AudioType string `json:"audioType,omitempty"`
	AudioSize int64  `json:"audioSize,omitempty"`

	VideoURL  string `json:"videoUrl,omitempty"`
	VideoType string `json:"videoType,omitempty"`
	VideoSize int64  `json:"videoSize,omitempty"`
}
```

Create `pkg/model/attachment.go`:

```go
package model

import "github.com/hmchangw/chat/pkg/model/cassandra"

// Attachment and ImageDimensions are defined in pkg/model/cassandra so
// cassandra.Message can embed them without an import cycle. These aliases keep
// model.Attachment usable from services that already import pkg/model.
type (
	Attachment      = cassandra.Attachment
	ImageDimensions = cassandra.ImageDimensions
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/model/cassandra/ -run TestAttachment_RoundTrip -v && go build ./pkg/model/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add pkg/model/cassandra/attachment.go pkg/model/cassandra/attachment_test.go pkg/model/attachment.go
git commit -m "feat(model): add Attachment/ImageDimensions in cassandra pkg with model aliases"
```

---

## Task A2: MIME allow/deny filter

**Files:**
- Create: `upload-service/mediatype.go`
- Test: `upload-service/mediatype_test.go`

- [ ] **Step 1: Write the failing test**

Create `upload-service/mediatype_test.go`:

```go
package main

import "testing"

func TestMediaTypeFilter_Allowed(t *testing.T) {
	tests := []struct {
		name, whitelist, blacklist, mime string
		want                             bool
	}{
		{"empty allows all", "", "", "application/pdf", true},
		{"blacklist blocks", "", "image/svg+xml", "image/svg+xml", false},
		{"blacklist case-insensitive", "", "image/svg+xml", "IMAGE/SVG+XML", false},
		{"whitelist allows match", "image/png", "", "image/png", true},
		{"whitelist excludes others", "image/png", "", "image/jpeg", false},
		{"whitelist wildcard", "image/*", "", "image/jpeg", true},
		{"blacklist beats whitelist", "image/*", "image/svg+xml", "image/svg+xml", false},
		{"bare star", "*", "", "anything/here", true},
		{"trims spaces", " image/png , image/jpeg ", "", "image/jpeg", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := newMediaTypeFilter(tc.whitelist, tc.blacklist).allowed(tc.mime); got != tc.want {
				t.Fatalf("allowed(%q) = %v, want %v", tc.mime, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./upload-service/ -run TestMediaTypeFilter -v`
Expected: FAIL — `undefined: newMediaTypeFilter`.

- [ ] **Step 3: Create the implementation**

Create `upload-service/mediatype.go`:

```go
package main

import "strings"

// mediaTypeFilter decides whether an uploaded MIME type is allowed: blacklist
// first (deny wins), then whitelist (when non-empty, the type must match).
type mediaTypeFilter struct {
	whitelist []string
	blacklist []string
}

func newMediaTypeFilter(whitelist, blacklist string) *mediaTypeFilter {
	return &mediaTypeFilter{whitelist: parseMediaTypes(whitelist), blacklist: parseMediaTypes(blacklist)}
}

func parseMediaTypes(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (f *mediaTypeFilter) allowed(mime string) bool {
	m := strings.ToLower(strings.TrimSpace(mime))
	for _, b := range f.blacklist {
		if matchMediaType(b, m) {
			return false
		}
	}
	if len(f.whitelist) == 0 {
		return true
	}
	for _, w := range f.whitelist {
		if matchMediaType(w, m) {
			return true
		}
	}
	return false
}

// matchMediaType supports exact match, "type/*" prefix wildcard, and bare "*".
func matchMediaType(pattern, mime string) bool {
	if pattern == "*" || pattern == "*/*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		return strings.HasPrefix(mime, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == mime
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./upload-service/ -run TestMediaTypeFilter -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add upload-service/mediatype.go upload-service/mediatype_test.go
git commit -m "feat(upload-service): add MIME allow/deny filter"
```

---

## Task A3: image preview + dimensions (`golang.org/x/image`)

**Files:**
- Create: `upload-service/preview.go`
- Test: `upload-service/preview_test.go`
- Modify: `go.mod` / `go.sum`

- [ ] **Step 1: Write the failing test**

Create `upload-service/preview_test.go`:

```go
package main

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{uint8(x * 4), uint8(y * 4), 128, 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

func TestResizeImagePreview_PNG(t *testing.T) {
	out, err := resizeImagePreview(makePNG(t, 64, 48), "image/png")
	require.NoError(t, err)
	require.NotEmpty(t, out)
	raw, err := base64.StdEncoding.DecodeString(out)
	require.NoError(t, err)
	cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Equal(t, "jpeg", format)
	assert.Equal(t, 32, cfg.Width)
	assert.Equal(t, 32, cfg.Height)
}

func TestResizeImagePreview_NonImage(t *testing.T) {
	out, err := resizeImagePreview([]byte("not an image"), "application/pdf")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestResizeImagePreview_Undecodable(t *testing.T) {
	out, err := resizeImagePreview([]byte{0, 1, 2}, "image/heic")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestImageDimensions(t *testing.T) {
	dims := imageDimensions(makePNG(t, 64, 48))
	require.NotNil(t, dims)
	assert.Equal(t, 64, dims.Width)
	assert.Equal(t, 48, dims.Height)
	assert.Nil(t, imageDimensions([]byte("nope")))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./upload-service/ -run "TestResizeImagePreview|TestImageDimensions" -v`
Expected: FAIL — `undefined: resizeImagePreview`.

- [ ] **Step 3: Add the dependency**

Run: `go get golang.org/x/image/draw@latest && go mod tidy`
Expected: `golang.org/x/image` in `go.mod`.

- [ ] **Step 4: Create the implementation**

Create `upload-service/preview.go`:

```go
package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	_ "image/png" // register PNG decoder for image.Decode
	"strings"

	xdraw "golang.org/x/image/draw"

	"github.com/hmchangw/chat/pkg/model"
)

const previewDim = 32

// resizeImagePreview returns a 32x32 blurred JPEG preview of an image as base64.
// Non-image MIME types and undecodable bytes (e.g. heic) yield ("", nil).
func resizeImagePreview(data []byte, mime string) (string, error) {
	if !strings.HasPrefix(strings.ToLower(mime), "image/") {
		return "", nil
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return "", nil
	}
	dst := image.NewRGBA(image.Rect(0, 0, previewDim, previewDim))
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), src, src.Bounds(), xdraw.Over, nil)
	boxBlur(dst) // PERF INVARIANT: blur runs on the 32x32 dst, never the full-size src
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 50}); err != nil {
		return "", fmt.Errorf("encode preview jpeg: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// imageDimensions returns the source pixel size, or nil when undecodable.
func imageDimensions(data []byte) *model.ImageDimensions {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	return &model.ImageDimensions{Width: cfg.Width, Height: cfg.Height}
}

// boxBlur applies one 3x3 averaging pass in place. Cost is O(pixels): on the
// 32x32 preview it is ~68us, but it is dimension-bound, so it MUST only ever be
// called on the downscaled preview, never on a full-size decoded upload.
func boxBlur(img *image.RGBA) {
	src := image.NewRGBA(img.Bounds())
	copy(src.Pix, img.Pix)
	b := img.Bounds()
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			var r, g, bl, a, n int
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					nx, ny := x+dx, y+dy
					if nx < b.Min.X || nx >= b.Max.X || ny < b.Min.Y || ny >= b.Max.Y {
						continue
					}
					c := src.RGBAAt(nx, ny)
					r += int(c.R)
					g += int(c.G)
					bl += int(c.B)
					a += int(c.A)
					n++
				}
			}
			img.SetRGBA(x, y, color.RGBA{uint8(r / n), uint8(g / n), uint8(bl / n), uint8(a / n)})
		}
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./upload-service/ -run "TestResizeImagePreview|TestImageDimensions" -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
make fmt
git add upload-service/preview.go upload-service/preview_test.go go.mod go.sum
git commit -m "feat(upload-service): add image preview and dimensions"
```

---

## Task A4: Drive `fileSize` field

**Files:**
- Modify: `pkg/drive/images_file.go:16-21` (`GroupImageObject`)
- Test: `pkg/drive/images_file_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/drive/images_file_test.go` (create the file if absent, `package drive`):

```go
func TestGroupImageObject_FileSizeJSON(t *testing.T) {
	const body = `{"objectId":"f1","groupId":"g1","fileName":"a.pdf","fileSize":4096}`
	var obj GroupImageObject
	require.NoError(t, json.Unmarshal([]byte(body), &obj))
	assert.Equal(t, "f1", obj.FileID)
	assert.Equal(t, int64(4096), obj.FileSize)
}
```

Ensure imports include `encoding/json`, `testing`, `github.com/stretchr/testify/assert`, `github.com/stretchr/testify/require`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/drive/ -run TestGroupImageObject_FileSizeJSON -v`
Expected: FAIL — `obj.FileSize undefined`.

- [ ] **Step 3: Add the field**

In `pkg/drive/images_file.go`, change `GroupImageObject`:

```go
type GroupImageObject struct {
	FileID   string `json:"objectId"`
	GroupID  string `json:"groupId"`
	Filename string `json:"fileName"`
	FileSize int64  `json:"fileSize"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/drive/ -run TestGroupImageObject_FileSizeJSON -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add pkg/drive/images_file.go pkg/drive/images_file_test.go
git commit -m "feat(drive): expose fileSize on GroupImageObject"
```

---

## Task A5: attachment builder

**Files:**
- Create: `upload-service/attachment.go`
- Test: `upload-service/attachment_test.go`

- [ ] **Step 1: Write the failing test**

Create `upload-service/attachment_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestFileURL(t *testing.T) {
	assert.Equal(t, "api/v1/rooms/room-1/file/f1?drive_host=http://drive",
		fileURL("room-1", "f1", "http://drive"))
}

func TestBuildAttachment_Generic(t *testing.T) {
	m := fileMeta{id: "f1", name: "report.pdf", mime: "application/pdf", size: 10}
	att := buildAttachment(m, "Q2", "http://link", "", nil)
	assert.Equal(t, "f1", att.ID)
	assert.Equal(t, "report.pdf", att.Title)
	assert.Equal(t, "file", att.Type)
	assert.Equal(t, "Q2", att.Description)
	assert.Equal(t, "http://link", att.TitleLink)
	assert.True(t, att.TitleLinkDownload)
	assert.Empty(t, att.ImageURL)
	assert.Empty(t, att.AudioURL)
}

func TestBuildAttachment_Image(t *testing.T) {
	m := fileMeta{id: "f1", name: "p.png", mime: "image/png", size: 99}
	dims := &model.ImageDimensions{Width: 800, Height: 600}
	att := buildAttachment(m, "", "http://link", "b64preview", dims)
	assert.Equal(t, "http://link", att.ImageURL)
	assert.Equal(t, "image/png", att.ImageType)
	assert.Equal(t, int64(99), att.ImageSize)
	assert.Equal(t, "b64preview", att.ImagePreview)
	require.NotNil(t, att.ImageDimensions)
	assert.Equal(t, 800, att.ImageDimensions.Width)
	assert.Empty(t, att.AudioURL)
}

func TestBuildAttachment_Audio(t *testing.T) {
	m := fileMeta{id: "f1", name: "a.mp3", mime: "audio/mpeg", size: 5}
	att := buildAttachment(m, "", "http://link", "", nil)
	assert.Equal(t, "http://link", att.AudioURL)
	assert.Equal(t, "audio/mpeg", att.AudioType)
	assert.Equal(t, int64(5), att.AudioSize)
	assert.Empty(t, att.ImageURL)
}

func TestBuildAttachment_Video(t *testing.T) {
	m := fileMeta{id: "f1", name: "v.mp4", mime: "video/mp4", size: 7}
	att := buildAttachment(m, "", "http://link", "", nil)
	assert.Equal(t, "http://link", att.VideoURL)
	assert.Equal(t, "video/mp4", att.VideoType)
	assert.Equal(t, int64(7), att.VideoSize)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./upload-service/ -run "TestFileURL|TestBuildAttachment" -v`
Expected: FAIL — `undefined: fileMeta` / `buildAttachment` / `fileURL`.

- [ ] **Step 3: Create the implementation**

Create `upload-service/attachment.go`:

```go
package main

import (
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// fileMeta is the processed metadata for an uploaded file.
type fileMeta struct {
	id   string
	name string
	mime string
	size int64
}

// fileURL builds the relative download URL for the protected file-download route.
func fileURL(roomID, fileID, driveHost string) string {
	return fmt.Sprintf("api/v1/rooms/%s/file/%s?drive_host=%s", roomID, fileID, driveHost)
}

// buildAttachment assembles the render-ready attachment, adding media-specific
// fields based on the MIME prefix.
func buildAttachment(m fileMeta, description, url, imagePreview string, dims *model.ImageDimensions) model.Attachment {
	att := model.Attachment{
		ID:                m.id,
		Title:             m.name,
		Type:              "file",
		Description:       description,
		TitleLink:         url,
		TitleLinkDownload: true,
	}
	switch {
	case strings.HasPrefix(m.mime, "image/"):
		att.ImageURL = url
		att.ImageType = m.mime
		att.ImageSize = m.size
		att.ImageDimensions = dims
		att.ImagePreview = imagePreview
	case strings.HasPrefix(m.mime, "audio/"):
		att.AudioURL = url
		att.AudioType = m.mime
		att.AudioSize = m.size
	case strings.HasPrefix(m.mime, "video/"):
		att.VideoURL = url
		att.VideoType = m.mime
		att.VideoSize = m.size
	}
	return att
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./upload-service/ -run "TestFileURL|TestBuildAttachment" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add upload-service/attachment.go upload-service/attachment_test.go
git commit -m "feat(upload-service): add attachment builder"
```

---

## Task A6: `HandleUploadFile` handler + extend `NewHandler`

**Files:**
- Modify: `upload-service/handler.go` (add `Handler` fields + `previewFunc`; extend `NewHandler`)
- Modify: `upload-service/handler_test.go` (fix the 3 `NewHandler` call sites)
- Modify: `upload-service/main.go` (temporary `NewHandler` call fix; finalized in A8)
- Create: `upload-service/handler_file.go`
- Test: `upload-service/handler_file_test.go`

- [ ] **Step 1: Add Handler fields + extend NewHandler**

In `upload-service/handler.go`, after the `driveClient` interface add:

```go
// previewFunc builds a base64 image preview; injected for testability.
type previewFunc func(data []byte, mime string) (string, error)
```

Add to the `Handler` struct:

```go
	maxFileSize int64
	mimeFilter  *mediaTypeFilter
	preview     previewFunc
```

Replace `NewHandler` with:

```go
// NewHandler wires the handler dependencies. maxFiles/maxImageSize gate the image
// endpoint; maxFileSize/mimeFilter/preview gate the file endpoint.
func NewHandler(store Store, dc driveClient, maxFiles int, maxImageSize, maxFileSize int64,
	mimeFilter *mediaTypeFilter, preview previewFunc) *Handler {
	return &Handler{
		store: store, drive: dc, maxFiles: maxFiles, maxImageSize: maxImageSize,
		maxFileSize: maxFileSize, mimeFilter: mimeFilter, preview: preview,
	}
}
```

- [ ] **Step 2: Fix existing call sites so the package compiles**

In `upload-service/handler_test.go`:
- helper around line 90: `return NewHandler(store, dc, testMaxFiles, testMaxImageSize, 0, nil, nil)`
- line ~169: `NewHandler(store, &fakeDrive{}, 1, testMaxImageSize, 0, nil, nil)`
- line ~205: `NewHandler(store, fd, testMaxFiles, 4, 0, nil, nil)`

In `upload-service/main.go` line ~88: `NewHandler(store, driveClient, cfg.MaxFiles, cfg.MaxImageSizeBytes, 0, nil, nil)` (finalized in A8).

Run: `go build ./upload-service/`
Expected: PASS.

- [ ] **Step 3: Write the failing test**

Create `upload-service/handler_file_test.go`:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/model"
)

func multipartFileBody(t *testing.T, field, filename string, data []byte, mime string, fields map[string]string) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	hdr := make(textproto.MIMEHeader)
	hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, filename))
	hdr.Set("Content-Type", mime)
	w, err := mw.CreatePart(hdr)
	require.NoError(t, err)
	_, err = w.Write(data)
	require.NoError(t, err)
	for k, v := range fields {
		require.NoError(t, mw.WriteField(k, v))
	}
	require.NoError(t, mw.Close())
	return body, mw.FormDataContentType()
}

func fileTestHandler(store Store) *Handler {
	fd := &fakeDrive{baseURL: "http://drive", uploadResp: []drive.UploadGroupImageResponse{
		{Status: driveStatusSuccess, File: drive.GroupImageObject{FileID: "drive-file-1", GroupID: "room-1", Filename: "report.pdf", FileSize: 2048}},
	}}
	return NewHandler(store, fd, 0, 0, 100<<20, newMediaTypeFilter("", "image/svg+xml"), resizeImagePreview)
}

func uploadCtx(t *testing.T, field, filename string, data []byte, mime string, fields map[string]string) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body, ct := multipartFileBody(t, field, filename, data, mime, fields)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/room-1/upload", body)
	req.Header.Set("Content-Type", ct)
	c.Request = req
	c.Params = gin.Params{{Key: "roomId", Value: "room-1"}}
	c.Set("request_id", "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	c.Set(ctxUserKey, &AuthenticatedUser{User: model.User{Account: "alice", EngName: "Alice"}, Email: "alice@example.com"})
	return rec, c
}

func TestHandleUploadFile_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	rec, c := uploadCtx(t, "file", "report.pdf", []byte("pdfbytes"), "application/pdf", map[string]string{"description": "Q2"})
	fileTestHandler(store).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Success     bool               `json:"success"`
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Success)
	require.Len(t, resp.Attachments, 1)
	assert.Equal(t, "drive-file-1", resp.Attachments[0].ID)
	assert.Equal(t, "report.pdf", resp.Attachments[0].Title)
	assert.Equal(t, "file", resp.Attachments[0].Type)
	assert.Equal(t, "Q2", resp.Attachments[0].Description)
	assert.Contains(t, resp.Attachments[0].TitleLink, "drive-file-1")
	// No file/message wrapper.
	assert.NotContains(t, rec.Body.String(), `"file"`)
	assert.NotContains(t, rec.Body.String(), `"message"`)
}

func TestHandleUploadFile_ImageSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)

	rec, c := uploadCtx(t, "file", "photo.png", makePNG(t, 64, 48), "image/png", nil)
	fileTestHandler(store).HandleUploadFile(c)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Attachments, 1)
	att := resp.Attachments[0]
	assert.NotEmpty(t, att.ImageURL)
	assert.Equal(t, "image/png", att.ImageType)
	assert.NotEmpty(t, att.ImagePreview)
	require.NotNil(t, att.ImageDimensions)
	assert.Equal(t, 64, att.ImageDimensions.Width)
	assert.Equal(t, 48, att.ImageDimensions.Height)
}

func TestHandleUploadFile_NotMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(false, nil)
	rec, c := uploadCtx(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	fileTestHandler(store).HandleUploadFile(c)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandleUploadFile_RoomNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("", ErrRoomNotFound)
	rec, c := uploadCtx(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	fileTestHandler(store).HandleUploadFile(c)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleUploadFile_BlockedMIME(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	rec, c := uploadCtx(t, "file", "x.svg", []byte("<svg/>"), "image/svg+xml", nil)
	fileTestHandler(store).HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleUploadFile_OverSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	h := NewHandler(store, &fakeDrive{baseURL: "http://drive"}, 0, 0, 4, newMediaTypeFilter("", ""), resizeImagePreview)
	rec, c := uploadCtx(t, "file", "big.pdf", []byte("morethan4"), "application/pdf", nil)
	h.HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleUploadFile_DriveError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	h := NewHandler(store, &fakeDrive{baseURL: "http://drive", uploadErr: assertErr{}}, 0, 0, 100<<20, newMediaTypeFilter("", ""), resizeImagePreview)
	rec, c := uploadCtx(t, "file", "report.pdf", []byte("x"), "application/pdf", nil)
	h.HandleUploadFile(c)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleUploadFile_MissingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "room-1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "room-1").Return("site-a", nil)
	rec, c := uploadCtx(t, "other", "x.txt", []byte("x"), "text/plain", nil)
	fileTestHandler(store).HandleUploadFile(c)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

type assertErr struct{}

func (assertErr) Error() string { return "drive boom" }
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./upload-service/ -run TestHandleUploadFile -v`
Expected: FAIL — `h.HandleUploadFile undefined`.

- [ ] **Step 5: Create the handler**

Create `upload-service/handler_file.go`:

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

const defaultUploadContentType = "application/octet-stream"

// HandleUploadFile uploads one file for a room on behalf of the authenticated
// user and returns a render-ready attachment. It does not publish a message.
func (h *Handler) HandleUploadFile(c *gin.Context) {
	ctx := logCtx(c)

	roomID := c.Param("roomId")
	if roomID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("roomId is required"))
		return
	}
	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}
	if user.Email == "" {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}

	if !h.requireMembership(ctx, c, roomID, user.Account) {
		return
	}

	siteID, err := h.store.GetRoomSiteID(ctx, roomID)
	if err != nil {
		if errIsRoomNotFound(err) {
			errhttp.Write(ctx, c, errcode.NotFound("room not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get room: %w", err))
		return
	}

	fh, err := c.FormFile("file")
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("file is required"))
		return
	}
	if h.maxFileSize >= 0 && fh.Size > h.maxFileSize {
		errhttp.Write(ctx, c, errcode.BadRequest("file size exceeds limit"))
		return
	}

	mime := fh.Header.Get("Content-Type")
	if mime == "" {
		mime = defaultUploadContentType
	}
	if !h.mimeFilter.allowed(mime) {
		errhttp.Write(ctx, c, errcode.BadRequest("file type is not allowed"))
		return
	}

	// Images are buffered once (for preview + dimensions) and the same bytes are
	// reused for the Drive upload, so the file is read exactly once. Non-image
	// types are streamed straight to Drive without buffering (a large video must
	// not be held in memory).
	var data []byte
	var driveFile multipart.File
	if strings.HasPrefix(mime, "image/") {
		if data, err = readMultipartFile(fh); err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("read uploaded file: %w", err))
			return
		}
		driveFile = bytesFile{bytes.NewReader(data)}
	} else if driveFile, err = fh.Open(); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("open uploaded file: %w", err))
		return
	}
	defer driveFile.Close()

	responses, err := h.drive.UploadGroupImages(user.Account, user.DisplayName(), user.Email, roomID, siteID,
		[]drive.MultipartFile{{File: driveFile, Filename: fh.Filename}})
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("upload file to drive: %w", err))
		return
	}
	if len(responses) == 0 || responses[0].Status != driveStatusSuccess {
		errhttp.Write(ctx, c, errcode.Unavailable("drive upload failed"))
		return
	}
	obj := responses[0].File

	meta := fileMeta{id: obj.FileID, name: fh.Filename, mime: mime, size: obj.FileSize}
	url := fileURL(roomID, obj.FileID, h.drive.GetBaseURLFromRoomOrigin(siteID))

	var preview string
	var dims *model.ImageDimensions
	if strings.HasPrefix(mime, "image/") {
		if preview, err = h.preview(data, mime); err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("build image preview: %w", err))
			return
		}
		dims = imageDimensions(data)
	}

	att := buildAttachment(meta, c.PostForm("description"), url, preview, dims)
	c.JSON(http.StatusOK, gin.H{"success": true, "attachments": []model.Attachment{att}})
}

// readMultipartFile opens, reads, and closes a multipart file header's content.
func readMultipartFile(fh *multipart.FileHeader) ([]byte, error) {
	f, err := fh.Open()
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	return io.ReadAll(f)
}

// bytesFile adapts a *bytes.Reader (Read/ReadAt/Seek) to multipart.File by adding
// a no-op Close, so already-buffered image bytes can be handed to Drive without
// re-reading the upload.
type bytesFile struct{ *bytes.Reader }

func (bytesFile) Close() error { return nil }
```

`requireMembership` already exists in `handler.go` (used by the image handlers) and writes the 403; reuse it.

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./upload-service/ -run TestHandleUploadFile -v`
Expected: PASS.

- [ ] **Step 7: Run the whole upload-service unit suite**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
make fmt
git add upload-service/handler.go upload-service/handler_test.go upload-service/handler_file.go upload-service/handler_file_test.go upload-service/main.go
git commit -m "feat(upload-service): add HandleUploadFile pure-HTTP endpoint"
```

---

## Task A7: register the route

**Files:**
- Modify: `upload-service/routes.go`
- Test: `upload-service/handler_file_test.go` (add registration assertion)

- [ ] **Step 1: Write the failing test**

Append to `upload-service/handler_file_test.go`:

```go
func TestRoute_UploadRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, &Handler{}, nil, true)
	found := false
	for _, ri := range r.Routes() {
		if ri.Method == http.MethodPost && ri.Path == "/api/v1/rooms/:roomId/upload" {
			found = true
		}
	}
	assert.True(t, found)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./upload-service/ -run TestRoute_UploadRegistered -v`
Expected: FAIL.

- [ ] **Step 3: Register the route**

In `upload-service/routes.go`, after the `POST .../upload/images` line add:

```go
	api.POST("/rooms/:roomId/upload", h.HandleUploadFile)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./upload-service/ -run TestRoute_UploadRegistered -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt
git add upload-service/routes.go upload-service/handler_file_test.go
git commit -m "feat(upload-service): register POST /rooms/:roomId/upload"
```

---

## Task A8: config + main.go wiring (no NATS)

**Files:**
- Modify: `upload-service/main.go`
- Modify: `upload-service/deploy/docker-compose.yml`

- [ ] **Step 1: Add config fields**

In `upload-service/main.go`, add to the `config` struct:

```go
	FileUploadMaxFileSize        int64  `env:"FILE_UPLOAD_MAX_FILE_SIZE" envDefault:"104857600"`
	FileUploadMediaTypeWhitelist string `env:"FILE_UPLOAD_MEDIA_TYPE_WHITELIST" envDefault:""`
	FileUploadMediaTypeBlacklist string `env:"FILE_UPLOAD_MEDIA_TYPE_BLACKLIST" envDefault:"image/svg+xml"`
```

- [ ] **Step 2: Finalize the handler construction**

Replace the `NewHandler(...)` line in `run()` with:

```go
	mimeFilter := newMediaTypeFilter(cfg.FileUploadMediaTypeWhitelist, cfg.FileUploadMediaTypeBlacklist)
	handler := NewHandler(store, driveClient, cfg.MaxFiles, cfg.MaxImageSizeBytes,
		cfg.FileUploadMaxFileSize, mimeFilter, resizeImagePreview)
```

- [ ] **Step 3: Build + test + lint**

Run: `go build ./upload-service/ && make test SERVICE=upload-service && make lint`
Expected: PASS.

- [ ] **Step 4: Update docker-compose**

In `upload-service/deploy/docker-compose.yml`, add to the upload-service `environment:`:

```yaml
      FILE_UPLOAD_MAX_FILE_SIZE: "104857600"
      FILE_UPLOAD_MEDIA_TYPE_BLACKLIST: "image/svg+xml"
```

(No NATS service is added — upload-service has no NATS dependency.)

- [ ] **Step 5: Commit**

```bash
make fmt
git add upload-service/main.go upload-service/deploy/docker-compose.yml
git commit -m "feat(upload-service): wire file-upload config"
```

---

## Task A9: rename the protected download endpoint `/image/:fileId` → `/file/:fileId`

The single download endpoint already serves arbitrary uploaded files (the handler
streams raw bytes with the stored content-type), and `fileURL` (Task A5) now
points at `/file/`. Rename the route + handler to match. **Hard rename — `/image/`
is dropped (no alias).** `pkg/drive.GetGroupImage` keeps its name (out of scope);
the image *upload* route is unchanged (only its `relativePath` shifts to `/file/`
via the shared `fileURL`).

**Files:**
- Modify: `upload-service/routes.go`, `upload-service/handler.go`
- Modify: `upload-service/handler_test.go`, `upload-service/attachment_test.go`
- Modify: `docs/client-api.md`

- [ ] **Step 1: Update the failing tests (Red)**

In `upload-service/handler_test.go`: rename every `h.HandleDownloadImage(c)` call
to `h.HandleDownloadFile(c)`; change `/image/` → `/file/` in `newDownloadCtx`'s
URL and in the route-auth `ServeHTTP` request path; update the image-upload
`relativePath` assertion (`…/image/img-xyz…` → `…/file/img-xyz…`).
In `upload-service/attachment_test.go`: `TestFileURL` already expects `/file/`
(Task A5) — no change.

Run: `go test ./upload-service/ -run 'TestDownload|TestRoute|TestUpload_Mixed' -v`
Expected: FAIL — `h.HandleDownloadFile` undefined; route serves 404 at `/file/`.

- [ ] **Step 2: Rename the route**

In `upload-service/routes.go`:

```go
	api.GET("/rooms/:roomId/file/:fileId", h.HandleDownloadFile)
```

(replaces the `/rooms/:roomId/image/:fileId` + `HandleDownloadImage` line.)

- [ ] **Step 3: Rename the handler**

In `upload-service/handler.go`, rename `HandleDownloadImage` → `HandleDownloadFile`
(signature/body unchanged). Update its doc-comment ("proxies a protected image" →
"…file") and the client-facing error string:

```go
		errhttp.Write(ctx, c, errcode.Unavailable("failed to retrieve file", errcode.WithCause(err)))
```

- [ ] **Step 4: Run tests (Green)**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 5: Update `docs/client-api.md`**

- §2.3 GET endpoint: rename the heading and `**Endpoint:**` line to
  `GET /api/v1/rooms/:roomId/file/:fileId`; change "protected image" wording in
  that block to "file".
- Update the two example JSONs to `/file/`: the image-upload `relativePath`
  example and the file-upload `titleLink` example.
- Update the image-upload `relativePath` field note ("download the image via the
  GET endpoint below" → "…file…").
- Adjust the §2.3 section title so it covers file (not only image) upload/download.

- [ ] **Step 6: Commit**

```bash
make fmt
git add upload-service/routes.go upload-service/handler.go upload-service/handler_test.go docs/client-api.md
git commit -m "feat(upload-service): rename protected download endpoint /image to /file"
```

---

# Group B — message-gatekeeper attachments

## Task B1: accept + validate `attachments` on `msg.send`

**Files:**
- Modify: `pkg/model/message.go` (add `Attachments` to `SendMessageRequest`)
- Modify: `message-gatekeeper/handler.go` (constants + `processMessage`)
- Test: `message-gatekeeper/handler_test.go`

- [ ] **Step 1: Add the request field**

In `pkg/model/message.go`, inside `SendMessageRequest` (after `TShow`):

```go
	// Attachments carries render-ready attachment blobs produced by upload-service
	// (one JSON object per element). message-gatekeeper validates and copies them
	// onto the canonical Message.
	Attachments [][]byte `json:"attachments,omitempty"`
```

- [ ] **Step 2: Write the failing tests**

Append to `message-gatekeeper/handler_test.go`:

```go
func TestHandler_processMessage_CarriesAttachments(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "room-1",
		Roles: []model.Role{model.RoleOwner}, // owner bypasses the large-room GetRoomMeta lookup
	}, nil).AnyTimes()

	var published []publishedMsg
	h := NewHandler(store, nil, makePublishFunc(&published, nil), func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500)

	att := []byte(`{"id":"f1","title":"a.png","type":"file"}`)
	req := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "", RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		Attachments: [][]byte{att},
	}
	out, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)

	require.Len(t, published, 1)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(published[0].data, &evt))
	require.Len(t, evt.Message.Attachments, 1)
	assert.JSONEq(t, string(att), string(evt.Message.Attachments[0]))

	var replyMsg model.Message
	require.NoError(t, json.Unmarshal(out, &replyMsg))
	require.Len(t, replyMsg.Attachments, 1)
}

func TestHandler_processMessage_EmptyContentRejectedWithoutAttachments(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "room-1", Roles: []model.Role{model.RoleMember},
	}, nil).AnyTimes()
	h := NewHandler(store, nil, makePublishFunc(nil, nil), func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500)
	req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: "", RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f"}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
}

func TestHandler_processMessage_RejectsTooManyAttachments(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "room-1", Roles: []model.Role{model.RoleOwner},
	}, nil).AnyTimes()
	h := NewHandler(store, nil, makePublishFunc(nil, nil), func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500)
	req := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hi", RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		Attachments: [][]byte{[]byte("a"), []byte("b")},
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — attachments not carried; empty-content rejected even with attachments.

- [ ] **Step 4: Add constants**

In `message-gatekeeper/handler.go`, next to `const maxContentBytes = 20 * 1024`:

```go
const (
	maxAttachments     = 1
	maxAttachmentBytes = 8 * 1024 // 8 KiB/attachment; a realistic image Attachment
	// JSON (metadata + long description + ~900 B base64 32x32 preview) is ~1.5 KB.
	// The blob is opaque to gatekeeper, so this is the only bound on what a client
	// can stuff into the encrypted Cassandra row — keep it tight, not generous.
)
```

- [ ] **Step 5: Edit `processMessage`**

Replace the empty-content check:

```go
	// Validate content is non-empty
	if req.Content == "" {
		return nil, errcode.BadRequest("content must not be empty")
	}
```

with:

```go
	// A message with attachments may carry empty content.
	if req.Content == "" && len(req.Attachments) == 0 {
		return nil, errcode.BadRequest("content must not be empty")
	}
	if len(req.Attachments) > maxAttachments {
		return nil, errcode.BadRequest(fmt.Sprintf("too many attachments: max %d", maxAttachments))
	}
	var attachmentBytes int
	for _, a := range req.Attachments {
		attachmentBytes += len(a)
	}
	if attachmentBytes > maxAttachmentBytes {
		return nil, errcode.BadRequest(fmt.Sprintf("attachments exceed maximum size of %d bytes", maxAttachmentBytes))
	}
```

Then in the `msg := model.Message{...}` literal, add:

```go
		Attachments: req.Attachments,
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
make fmt
git add pkg/model/message.go message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): carry and validate attachments on msg.send"
```

---

## Task B2: decode stored attachment blobs into `[]Attachment` on the history read path

Storage stays `LIST<BLOB>` / `[][]byte` (opaque, unchanged on write). The history
**read path** decodes each blob (one JSON-encoded `Attachment` per element) into
a typed `[]Attachment` so clients receive objects, not base64 strings.

Mechanism: `cassandra.Message` keeps its raw `Attachments [][]byte` for the gocql
scan but stops serializing it (`json:"-"`), and gains a `DecodedAttachments
[]Attachment` field (`json:"attachments" cql:"-"` — `structScan` skips `cql:"-"`
fields per `history-service/internal/cassrepo/utils.go`). The service fills the
decoded field after redaction, immediately before returning each response.

**Scope / what is intentionally unchanged (call out in the PR description):**
- The **internal `getMessageByID` RPC** is consumed only by gatekeeper's
  `FetchQuotedParent`, which copies `MessageID/RoomID/Sender/CreatedAt/Msg/
  Mentions/...` and **never reads attachments** (`message-gatekeeper/fetcher_history.go`).
  So dropping raw `attachments` from `cassandra.Message`'s JSON is safe there.
- The **live broadcast** path is decoded too (Task B3), so real-time delivery and
  history return the same `Attachment[]` shape.
- The **quoted-parent** snapshot is decoded too (its `Attachments` is retagged in
  Step 1 and decoded in both paths). Safe because no producer populates
  quoted-parent attachments today, so `json:"-"` drops an always-empty field from
  the canonical event.

**Files:**
- Modify: `pkg/model/cassandra/attachment.go` (+`_test.go`) — shared `DecodeAttachments`
- Modify: `pkg/model/cassandra/message.go` (retag raw, add `DecodedAttachments`)
- Modify: `pkg/model/cassandra/message_test.go` (round-trip now asserts decoded field)
- Create: `history-service/internal/service/attachments.go`
- Test: `history-service/internal/service/attachments_test.go`
- Modify: `history-service/internal/service/messages.go` (decode before each return)
- Modify: `history-service/internal/service/pin.go` (decode pinned; redaction clears decoded)

- [ ] **Step 1: Retag raw + add the decoded field (Message AND QuotedParentMessage)**

In `pkg/model/cassandra/message.go`, change the raw attachments field (it keeps
`cql:"attachments"` for the scan/UDT bind, stops serializing) and add the decoded
field directly after it, on **both** `Message` and `QuotedParentMessage`:

```go
	Attachments        [][]byte     `json:"-" cql:"attachments"`
	DecodedAttachments []Attachment `json:"attachments,omitempty" cql:"-"`
```

(`Message` was: `Attachments [][]byte \`json:"attachments,omitempty" cql:"attachments"\``;
`QuotedParentMessage` was the same with `cql:"attachments"`.)

`cql:"-"` keeps both `DecodedAttachments` out of the gocql mapping — `structScan`
skips it (`history-service/internal/cassrepo/utils.go`), and gocql's UDT
marshaler never matches a column named `-`, so the quoted-parent UDT round-trip is
unaffected (the raw field still binds the `attachments` UDT column). This is safe
for the canonical `model.Message` pipeline because no producer ever populates
quoted-parent attachments (gatekeeper's `FetchQuotedParent` omits them), so
`json:"-"` drops an always-empty field from the NATS event; the encryption
round-trip uses the raw field in-process (not JSON), so it is unaffected too.

- [ ] **Step 2: Write the failing decode test**

Create `history-service/internal/service/attachments_test.go`:

```go
package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSetDecodedAttachments(t *testing.T) {
	good, err := json.Marshal(cassandra.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)

	msgs := []models.Message{
		{MessageID: "m1", Attachments: [][]byte{good}},
		{MessageID: "m2", Attachments: [][]byte{[]byte("{not json")}}, // malformed → skipped
		{MessageID: "m3"}, // no attachments → nil
		{MessageID: "m4", QuotedParentMessage: &cassandra.QuotedParentMessage{Attachments: [][]byte{good}}},
	}
	setDecodedAttachments(context.Background(), msgs)

	require.Len(t, msgs[0].DecodedAttachments, 1)
	assert.Equal(t, "f1", msgs[0].DecodedAttachments[0].ID)
	assert.Empty(t, msgs[1].DecodedAttachments) // malformed blob dropped, not fatal
	assert.Nil(t, msgs[2].DecodedAttachments)
	require.Len(t, msgs[3].QuotedParentMessage.DecodedAttachments, 1)
	assert.Equal(t, "f1", msgs[3].QuotedParentMessage.DecodedAttachments[0].ID)
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./history-service/internal/service/ -run TestSetDecodedAttachments -v`
Expected: FAIL — `undefined: setDecodedAttachments`.

- [ ] **Step 3a: Add the shared lenient decoder (reused by Task B3)**

Both the history read path and the live broadcast path (Task B3) decode the same
blob format, so the decoder lives next to the type. Add to
`pkg/model/cassandra/attachment.go`:

```go
import "encoding/json"

// DecodeAttachments decodes a LIST<BLOB> attachments column (each blob is one
// JSON-encoded Attachment) into typed objects. It is lenient: a malformed blob
// is skipped and counted (returned as skipped) rather than failing the batch, so
// one bad row can't break a history load or a live delivery. Returns (nil, 0)
// for empty input.
func DecodeAttachments(raw [][]byte) (out []Attachment, skipped int) {
	if len(raw) == 0 {
		return nil, 0
	}
	out = make([]Attachment, 0, len(raw))
	for _, b := range raw {
		var a Attachment
		if err := json.Unmarshal(b, &a); err != nil {
			skipped++
			continue
		}
		out = append(out, a)
	}
	return out, skipped
}
```

Add a table test `TestDecodeAttachments` in `pkg/model/cassandra/attachment_test.go`
covering: one good blob → 1 object, 0 skipped; a malformed blob → skipped=1; empty
input → nil, 0. Run `go test ./pkg/model/cassandra/ -run TestDecodeAttachments -v`
(red → green).

- [ ] **Step 4: Implement the history helper on top of the shared decoder**

Create `history-service/internal/service/attachments.go`:

```go
package service

import (
	"context"
	"log/slog"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// setDecodedAttachments fills each message's DecodedAttachments (and its quoted
// parent's) from the raw LIST<BLOB> attachments. Lenient: malformed blobs are
// logged and skipped so one bad row can't fail a history load. Call it after
// redaction and just before returning — a redacted stub has its raw Attachments
// already nil'd, so it decodes to nil.
func setDecodedAttachments(ctx context.Context, msgs []models.Message) {
	for i := range msgs {
		decoded, skipped := cassandra.DecodeAttachments(msgs[i].Attachments)
		if skipped > 0 {
			slog.WarnContext(ctx, "skipped malformed attachment blobs",
				"messageId", msgs[i].MessageID, "skipped", skipped)
		}
		msgs[i].DecodedAttachments = decoded

		if qp := msgs[i].QuotedParentMessage; qp != nil {
			qpDecoded, qpSkipped := cassandra.DecodeAttachments(qp.Attachments)
			if qpSkipped > 0 {
				slog.WarnContext(ctx, "skipped malformed quoted-parent attachment blobs",
					"messageId", msgs[i].MessageID, "skipped", qpSkipped)
			}
			qp.DecodedAttachments = qpDecoded
		}
	}
}
```

- [ ] **Step 5: Run it to verify it passes**

Run: `go test ./history-service/internal/service/ -run TestSetDecodedAttachments -v`
Expected: PASS.

- [ ] **Step 6: Call it on every client read path, after redaction**

In `history-service/internal/service/messages.go`, add a decode call immediately
after each redaction, before the corresponding `return`:
- after line ~105 `redactUnavailableQuotes(page.Data, accessSince)` →
  `setDecodedAttachments(ctx, page.Data)`
- after line ~151 `redactUnavailableQuotes(page.Data, accessSince)` →
  `setDecodedAttachments(ctx, page.Data)`
- after line ~195 `redactUnavailableQuote(&only, accessSince)` →
  `setDecodedAttachments(ctx, []models.Message{only})` is wrong (copies); instead
  decode the slice that is returned: after line ~250
  `redactUnavailableQuotes(messages, accessSince)` →
  `setDecodedAttachments(ctx, messages)`. For the single-message branch at ~195,
  wrap: `decodeSingle(ctx, &only)` — add this one-liner helper to
  `attachments.go`:

```go
// decodeSingle decodes one message in place (single-message read paths).
func decodeSingle(ctx context.Context, m *models.Message) {
	one := []models.Message{*m}
	setDecodedAttachments(ctx, one)
	*m = one[0]
}
```

  and call `decodeSingle(ctx, &only)` after line ~195's redaction.

In `history-service/internal/service/pin.go`, after the redaction loop that sets
`pinned[i].Attachments = nil` for unavailable pins, also clear the decoded field
in that same branch (`pinned[i].DecodedAttachments = nil`) and call
`setDecodedAttachments(ctx, pinned)` before returning the pinned slice.

- [ ] **Step 7: Update the cassandra message/quoted-parent round-trip tests**

In `pkg/model/cassandra/message_test.go`, the raw `Attachments` no longer
serializes to JSON, for both `Message` and `QuotedParentMessage`. Wherever a
round-trip test sets `Attachments: [][]byte{...}` and asserts `got.Attachments`
after a JSON marshal/unmarshal, switch to the decoded field:
- `TestMessage_JSON` (~line 145/174): set `DecodedAttachments: []Attachment{{ID:
  "f1", Title: "a.png", Type: "file"}}` and assert `got.DecodedAttachments`.
- `TestQuotedParentMessage_JSON` (~line 88): the `Attachments: [][]byte{[]byte
  ("file1")}` line and its post-round-trip attachment assertion move to
  `DecodedAttachments: []Attachment{{ID: "f1", Title: "a.png", Type: "file"}}` /
  `got.DecodedAttachments`.

Leave the `got.Attachments` nil-assertions (e.g. lines ~109, ~209) as-is — raw is
now `json:"-"`, so it is correctly absent after a JSON round-trip.

- [ ] **Step 8: Build, test, and find any straggler read paths**

Run: `go build ./... && make test SERVICE=history-service`
Expected: PASS. If any history integration/unit test asserts the client-facing
`attachments` as base64 strings, update it to expect decoded `Attachment`
objects. Confirm every client load path (history, next, surrounding, threads,
pins) returns decoded attachments — grep the response builders for
`setDecodedAttachments` and add the call anywhere a `Messages` slice is returned
without it.

- [ ] **Step 9: Commit**

```bash
make fmt
git add pkg/model/cassandra/attachment.go pkg/model/cassandra/attachment_test.go pkg/model/cassandra/message.go pkg/model/cassandra/message_test.go history-service/internal/service/attachments.go history-service/internal/service/attachments_test.go history-service/internal/service/messages.go history-service/internal/service/pin.go
git commit -m "feat(history-service): decode attachment blobs into objects on read"
```

---

## Task B3: decode attachments on the live broadcast path

`broadcast-worker` delivers created messages to clients as `model.ClientMessage`
(built by `buildClientMessage`). To match loadHistory, the delivered payload must
carry `attachments` as `Attachment[]` objects, not base64 blobs. The canonical
`model.Message` is left unchanged (it must keep raw `Attachments [][]byte` in its
JSON so the gatekeeper→message-worker pipeline persists the blobs); only the
client-delivery struct is reshaped.

Mechanism: `ClientMessage` embeds `Message` inline, so an outer
`Attachments []Attachment` field with `json:"attachments"` **shadows** the
promoted raw `Message.Attachments` in the JSON output (shallower field wins).
`buildClientMessage` fills it via the shared `cassandra.DecodeAttachments`. The
nested **quoted parent** is reshaped by Task B2's `QuotedParentMessage` retag
(raw `json:"-"` + `DecodedAttachments json:"attachments"`); `buildClientMessage`
clones the quoted parent and fills its `DecodedAttachments` so the canonical
`*msg` (shared pointer) is not mutated. Edits/deletes carry no attachments
(`EditRoomEvent` has only `NewContent`), so only the created path changes.

**Files:**
- Modify: `pkg/model/message.go` (add `Attachments` to `ClientMessage`)
- Modify: `broadcast-worker/handler.go` (`buildClientMessage` decodes)
- Test: `broadcast-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/handler_test.go`:

```go
func TestBuildClientMessage_DecodesAttachments(t *testing.T) {
	blob, err := json.Marshal(cassandra.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)
	msg := model.Message{
		ID: "m1", UserAccount: "alice", Attachments: [][]byte{blob},
		QuotedParentMessage: &cassandra.QuotedParentMessage{Attachments: [][]byte{blob}},
	}

	cm := buildClientMessage(&msg, map[string]model.User{})

	require.Len(t, cm.Attachments, 1)
	assert.Equal(t, "f1", cm.Attachments[0].ID)
	require.Len(t, cm.QuotedParentMessage.DecodedAttachments, 1)
	assert.Equal(t, "f1", cm.QuotedParentMessage.DecodedAttachments[0].ID)

	// Cloning the quoted parent must not mutate the caller's canonical message.
	assert.Nil(t, msg.QuotedParentMessage.DecodedAttachments)

	// The delivered JSON carries attachments as objects, not base64 strings.
	out, err := json.Marshal(cm)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"id":"f1"`)
	assert.NotContains(t, string(out), base64.StdEncoding.EncodeToString(blob))
}
```

Ensure the test imports `encoding/base64`, `encoding/json`, and
`github.com/hmchangw/chat/pkg/model/cassandra`.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./broadcast-worker/ -run TestBuildClientMessage_DecodesAttachments -v`
Expected: FAIL — `cm.Attachments undefined`.

- [ ] **Step 3: Add the shadowing field to `ClientMessage`**

In `pkg/model/message.go`, add the decoded field to `ClientMessage` (it overrides
the inlined raw `Message.Attachments` in JSON):

```go
type ClientMessage struct {
	Message `json:",inline" bson:",inline"`
	Sender      *Participant `json:"sender,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}
```

- [ ] **Step 4: Decode in `buildClientMessage`**

In `broadcast-worker/handler.go`, set the field in the returned `ClientMessage`
(add `"github.com/hmchangw/chat/pkg/model/cassandra"` to the imports). Drop the
`skipped` count here (live path: best-effort, no per-message log needed — a bad
blob would already have been logged on write/persist):

```go
	decoded, _ := cassandra.DecodeAttachments(msg.Attachments)
	cm := &model.ClientMessage{
		Message:     *msg,
		Sender:      &sender,
		Attachments: decoded,
	}
	// Clone the quoted parent before filling its decoded attachments so the
	// caller's canonical *msg (a shared pointer) is not mutated.
	if msg.QuotedParentMessage != nil {
		qp := *msg.QuotedParentMessage
		qp.DecodedAttachments, _ = cassandra.DecodeAttachments(qp.Attachments)
		cm.QuotedParentMessage = &qp
	}
	return cm
```

- [ ] **Step 5: Run it to verify it passes**

Run: `go test ./broadcast-worker/ -run TestBuildClientMessage_DecodesAttachments -v`
Expected: PASS.

- [ ] **Step 6: Full broadcast-worker suite + build**

Run: `go build ./... && make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
make fmt
git add pkg/model/message.go broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): deliver decoded attachment objects on live messages"
```

---

# Group C — remove `File` everywhere

> Order matters: C1–C3 remove all *uses* of `File`, C4 removes the *definitions* + schema. Each commit compiles.

## Task C1: remove `File` use in message-worker

**Files:**
- Modify: `message-worker/store_cassandra.go`

- [ ] **Step 1: Edit the two plaintext INSERTs (`SaveMessage`)**

In `message-worker/store_cassandra.go`, for the `messages_by_room` INSERT (~line 87): remove `, file` from the column list (so it ends `attachments, card, card_action)`), remove the last `?` from VALUES (16 placeholders), and remove `msg.File,` from the bound args (the line becomes `msg.Attachments, msg.Card, msg.CardAction,`). Apply the identical change to the `messages_by_id` INSERT (~line 97).

- [ ] **Step 2: Edit the two encrypted INSERTs (`saveMessageEncrypted`)**

For both encrypted INSERTs (~line 138 and ~line 148): remove `file,` from the column list (the line `msg, attachments, card, card_action, file,` becomes `msg, attachments, card, card_action,`) and remove one `null` from the matching VALUES list (there are five `null` literals for `msg, attachments, card, card_action, file`; drop to four). The bound-arg lines are unchanged (they bind no file value).

- [ ] **Step 3: Edit the three thread INSERTs (`SaveThreadMessage` + tshow dual-write + encrypted thread)**

For each of the remaining INSERTs that list `file` (the `messages_by_id` and `thread_messages_by_thread` plaintext inserts ~line 177/189, the tshow dual-write into `messages_by_room` ~line 213, and the encrypted thread inserts ~line 249/264/283): remove `file` from the column list, remove the matching `?` (plaintext) or `null` (encrypted) from VALUES, and remove any `msg.File,` from the bound-arg lines. Use this checklist of the lines to fix (search for `file` and `msg.File`): column lists at lines containing `attachments, card, card_action, file)` and `card_action, file,`; bound args `msg.Attachments, msg.Card, msg.CardAction, msg.File,`.

- [ ] **Step 4: Edit `buildCassandraMessage`**

Remove the `File: msg.File,` line (~line 318) from the `cassandra.Message{…}` literal.

- [ ] **Step 5: Verify it compiles**

Run: `go build ./message-worker/`
Expected: PASS (the `cassandra.Message.File` field still exists but is now unused by message-worker; `model.Message.File` still exists).

- [ ] **Step 6: Run message-worker unit tests**

Run: `make test SERVICE=message-worker`
Expected: PASS. (Integration tests that assert on persisted `file` are updated in C4; if any message-worker integration test references `msg.File`, note it and fix in C4.)

- [ ] **Step 7: Commit**

```bash
make fmt
git add message-worker/store_cassandra.go
git commit -m "refactor(message-worker): stop reading/writing the file column"
```

---

## Task C2: remove `File` use in history-service + atrest

**Files:**
- Modify: `pkg/atrest/atrest.go`, `pkg/atrest/split.go`
- Modify: `history-service/internal/cassrepo/messages_by_room.go`, `thread_messages.go`, `pin.go`, `write.go`
- Modify: `history-service/internal/service/pin.go`

- [ ] **Step 1: atrest — drop `File` from the encrypted bundle**

In `pkg/atrest/atrest.go`, remove the line `File *cassandra.File \`json:"file,omitempty"\`` (~line 31) from the `EncryptedFields` struct.
In `pkg/atrest/split.go`, remove `File: msg.File,` (~line 13), `msg.File = nil` (~line 39), and `msg.File = enc.File` (~line 54).

- [ ] **Step 2: history SELECT column lists**

Remove `file, ` (or ` file,`) from these column-list constants:
- `history-service/internal/cassrepo/messages_by_room.go:14` → `"msg, mentions, attachments, card, card_action, tshow, tcount, " +`
- `history-service/internal/cassrepo/thread_messages.go:15` → `"sender, msg, mentions, attachments, card, card_action, " +`
- `history-service/internal/cassrepo/pin.go:24` and `:32` → remove `file, ` from both column lists.

These reads use `structScan` (maps columns to struct fields by `cql` tag), so removing the column here pairs with removing the struct field in C4.

- [ ] **Step 3: history encrypted-edit UPDATEs + the positional read in write.go**

In `history-service/internal/cassrepo/write.go`:
- In the four `edit*Encrypted` UPDATE constants (lines 33–36), remove `file = null, ` from each `SET` clause.
- Remove the `file        *cassmodel.File` scan var (~line 121).
- In the SELECT (~line 125), remove `file, ` from the column list.
- In the `.Scan(...)` call (~line 128), remove `&file,`.
- Remove the `File:        file,` line (~line 156) from the struct literal it builds.
- Update the comment on ~line 29 to drop `file`.

- [ ] **Step 4: history pin service**

In `history-service/internal/service/pin.go`, remove the `pinned[i].File = nil` line (~line 243).

- [ ] **Step 5: Verify it compiles**

Run: `go build ./history-service/... ./pkg/atrest/`
Expected: PASS (`cassandra.File`/`cassandra.Message.File` still exist but are now unused outside their own package).

- [ ] **Step 6: Run unit tests**

Run: `make test SERVICE=history-service && go test ./pkg/atrest/`
Expected: PASS. (Cassandra integration tests asserting on `File` are updated in C4.)

- [ ] **Step 7: Commit**

```bash
make fmt
git add pkg/atrest/atrest.go pkg/atrest/split.go history-service/internal/cassrepo/messages_by_room.go history-service/internal/cassrepo/thread_messages.go history-service/internal/cassrepo/pin.go history-service/internal/cassrepo/write.go history-service/internal/service/pin.go
git commit -m "refactor(history-service): stop reading the file column"
```

---

## Task C3: remove the `File` definitions in the models

**Files:**
- Modify: `pkg/model/cassandra/message.go` (delete `File` type + `Message.File`)
- Modify: `pkg/model/cassandra/message_test.go` (drop File assertions + `TestFile_JSON`)
- Modify: `pkg/model/message.go` (delete `Message.File`)
- Modify: `history-service/internal/models/message.go` (delete the `File` alias)

- [ ] **Step 1: Delete the cassandra File type + field**

In `pkg/model/cassandra/message.go`:
- Delete the `File` struct (the `type File struct { … }` block, ~lines 21–26).
- Delete the `File *File \`json:"file,omitempty" cql:"file"\`` field from `Message` (~line 82).

- [ ] **Step 2: Update cassandra message_test.go**

In `pkg/model/cassandra/message_test.go`:
- Delete `TestFile_JSON` (~lines 43–onwards for that function).
- Remove the `File: &File{…}` line (~line 146) from the round-trip message literal.
- Delete the `assert.Equal(t, "doc.pdf", got.File.Name)` (~line 175) and `assert.Nil(t, got.File)` (~line 205) assertions.

- [ ] **Step 3: Delete `model.Message.File`**

In `pkg/model/message.go`, delete the `File *cassandra.File …` field (~line 26). If `cassandra` is now unused in this file, leave it — `Message` still references `cassandra.Card`, `cassandra.QuotedParentMessage`, etc., so the import stays.

- [ ] **Step 4: Delete the history models alias**

In `history-service/internal/models/message.go`, delete `type File = cassandra.File` (~line 10).

- [ ] **Step 5: Build the whole tree**

Run: `go build ./...`
Expected: PASS. If any file still references `.File` or `cassandra.File`, the compiler names it — remove that reference (it belongs to one of C1/C2; fix in place).

- [ ] **Step 6: Run the affected unit suites**

Run: `go test ./pkg/model/... ./pkg/atrest/ && make test SERVICE=message-worker && make test SERVICE=history-service && make test SERVICE=message-gatekeeper`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
make fmt
git add pkg/model/cassandra/message.go pkg/model/cassandra/message_test.go pkg/model/message.go history-service/internal/models/message.go
git commit -m "refactor(model): remove the File type and Message.File field"
```

---

## Task C4: remove `File` from Cassandra schema (init DDL + doc) + integration tests

**Files:**
- Delete: `docker-local/cassandra/init/05-udt-file.cql`
- Modify: `docker-local/cassandra/init/10-table-messages_by_room.cql`, `11-table-thread_messages_by_thread.cql`, `12-table-pinned_messages_by_room.cql`, `13-table-messages_by_id.cql`
- Modify: `docs/cassandra_message_model.md`
- Modify: `history-service/internal/cassrepo/messages_by_id_integration_test.go`, `thread_messages_integration_test.go`

- [ ] **Step 1: Remove the column from the four table DDLs**

In each of `10-…`, `11-…`, `12-…`, `13-…` `.cql` files, delete the line `file … FROZEN<"File">,`.

- [ ] **Step 2: Delete the UDT file**

Run: `git rm docker-local/cassandra/init/05-udt-file.cql`

- [ ] **Step 3: Update the schema doc**

In `docs/cassandra_message_model.md`:
- Delete the `#### File` section and its `CREATE TYPE … "File"(…)` block (~lines 36–44).
- Delete the `file FROZEN<"File">,` line in each of the four table definitions (~lines 139, 191, 219, 255).
- In the encryption/plaintext-columns sentence (~line 285), remove `file` from the list `(\`msg\`, \`attachments\`, \`card\`, \`card_action\`, \`file\`, …)`.

- [ ] **Step 4: Update history integration tests**

In `history-service/internal/cassrepo/messages_by_id_integration_test.go` and `thread_messages_integration_test.go`:
- Delete the `file := models.File{…}` lines (~line 60 / ~line 164) and any place that sets `.File = &file` (or `File: &file`) on the message being written.
- Delete the four `msg.File` assertions in each (`require.NotNil(t, msg.File)`, `assert.Equal(t, "f1", msg.File.ID)`, `assert.Equal(t, "doc.pdf", msg.File.Name)`, `assert.Equal(t, "application/pdf", msg.File.Type)`).

- [ ] **Step 5: Verify the init DDL is internally consistent**

Run: `grep -rn "File\|file" docker-local/cassandra/init/ | grep -iv "filename"`
Expected: no remaining `File` UDT reference or `file` column.

- [ ] **Step 6: Build + integration tests (Docker)**

Run: `go build ./... && make test-integration SERVICE=history-service`
Expected: PASS (the integration suite recreates the keyspace from the updated DDL; no `file` column means the removed assertions are gone).

- [ ] **Step 7: Commit**

```bash
make fmt
git add docker-local/cassandra/init docs/cassandra_message_model.md history-service/internal/cassrepo/messages_by_id_integration_test.go history-service/internal/cassrepo/thread_messages_integration_test.go
git commit -m "refactor(cassandra): drop the file column and File UDT from the schema"
```

---

# Final verification

## Task FV: full gate + push

- [ ] **Step 1: Lint + all unit tests**

Run: `make lint && make test`
Expected: PASS.

- [ ] **Step 2: SAST**

Run: `make sast`
Expected: no medium+ findings. If `image.Decode` on user bytes triggers a decompression-bomb finding, reject images whose `imageDimensions` exceed a sane bound before full decode and document; otherwise no suppressions.

- [ ] **Step 3: Integration tests (Docker)**

Run: `make test-integration SERVICE=history-service && make test-integration SERVICE=message-worker && make test-integration SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 4: Update `docs/client-api.md`**

Document `POST /api/v1/rooms/:roomId/upload` under §2.3 (request table: `ssoToken`, `roomId`, `file`, `description`; success response `{success, attachments:[Attachment]}` with the `Attachment` shape added to §3.0 Shared schemas; errors 400/401/403/404/500/503).

Reflect the attachment read/write asymmetry:
- **`msg.send`** (§4): add the optional `attachments` field as `string[]` (base64-encoded bytes — each entry is one base64-encoded JSON `Attachment`), and note empty `content` is allowed when `attachments` is present. This is the **write** shape (unchanged storage).
- **Load History / Next / Surrounding / Threads / Pins** response Message: change the `attachments` row from `string[]` (base64) to **`Attachment[]`** (decoded objects).
- **Live broadcast** `ClientMessage` (real-time created-message delivery): `attachments` is also **`Attachment[]`** (decoded objects) — same shape as history.
- The embedded **`quotedParentMessage.attachments`** is also **`Attachment[]`** (decoded) in both history and live responses.

Commit:

```bash
git add docs/client-api.md
git commit -m "docs: document upload endpoint and msg.send attachments"
```

- [ ] **Step 5: Push**

```bash
git push -u origin claude/funny-noether-cq396t
```

> **PR note for reviewers (not code):** production Cassandra still has the physical `file` column + `File` UDT. After this deploys, ops/IaC should run `ALTER TABLE … DROP file` on `messages_by_room`, `thread_messages_by_thread`, `pinned_messages_by_room`, `messages_by_id`, then `DROP TYPE "File"`. This PR only updates the init DDL (fresh envs) and stops all reads/writes.

---

## Self-review notes (spec coverage map)

- Spec §2 scope / non-goals → groups A (pure HTTP), B (gatekeeper), C (File removal); no-NATS in A8; no frontend.
- Spec §3 (endpoint, flow, errors, config) → A6, A7, A8.
- Spec §4 (`Attachment`/`ImageDimensions`, `id`) → A1.
- Spec §5 (builder, preview, dimensions) → A3, A5.
- Spec §6 (MIME filter) → A2.
- Spec §7 (Drive `fileSize`) → A4 + used in A6.
- Spec §8 (gatekeeper attachments, caps, relaxed content) → B1.
- Spec §9 (remove File: models, schema, message-worker, history, atrest, tests) → C1–C4.
- Spec §10 (testing) → tests in every task; final gate FV.
- Spec §11 (assumptions) → reflected in A6 (single file, description-only, image-only buffering).
