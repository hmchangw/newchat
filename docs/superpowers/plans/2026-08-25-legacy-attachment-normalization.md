# Legacy Attachment Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert pre-migration `snake_case` attachment blobs in Cassandra into the current `Attachment` shape at read time, so legacy attachments render and download through `GET /api/v1/file-upload/:fileId/:fileName`.

**Architecture:** One pure, read-time normalization step inside `cassandra.DecodeAttachments` — the single choke point that `history-service`, `broadcast-worker` and `pkg/searchindex` all already call. A blob that decodes with an empty `id` **and** an empty `titleLink` is re-decoded against a `snake_case` struct and converted; anything else passes through untouched. Cassandra is never rewritten.

**Tech Stack:** Go 1.25, stdlib only (`encoding/json`, `mime`, `net/url`, `path`, `strings`), `stretchr/testify` for assertions.

**Spec:** `docs/superpowers/specs/2026-08-25-legacy-attachment-normalization-design.md`

**Issue:** [#374](https://github.com/hmchangw/newchat/issues/374)

## Global Constraints

- Branch: `claude/issue-374-fix-plan-9mxvme`. Never commit to `master`/`main`.
- No new third-party dependencies. Stdlib only for this change.
- All commands go through `make` targets — never raw `go` commands.
- TDD is mandatory: write the test, watch it fail, then implement. Never write implementation before its test exists.
- `pkg/model/cassandra` uses `encoding/json`, never `sonic`. Do not change the codec.
- No logging inside `pkg/model/cassandra` — these are pure functions. Callers already log the `skipped` count.
- Error wrapping style (`fmt.Errorf("short description: %w", err)`) applies to any new error path. This change introduces none: a failed legacy re-decode falls back to the original decode rather than erroring.
- Minimum 80% coverage per package; target 90%+ for `pkg/` code.
- Comments describe WHY, max ~2 lines, matching the density of `pkg/model/cassandra/attachment.go`.
- Do not touch `upload-service`, `search-service`, or any ES backfill — explicitly out of scope.

## File Structure

| File | Responsibility |
|---|---|
| `pkg/model/cassandra/attachment_legacy.go` (create) | The `legacyAttachment` struct, detection, and all conversion helpers. Self-contained; nothing else imports it. |
| `pkg/model/cassandra/attachment.go` (modify, `DecodeAttachments` loop) | One added call to `normalizeAttachment`. |
| `pkg/model/cassandra/attachment_legacy_test.go` (create) | Table-driven tests for detection, conversion and every fallback. |
| `docs/client-api.md` (modify, §Attachment) | One note that legacy rows are normalized server-side. |

Kept in a separate file from `attachment.go` so the legacy shim is deletable in one `rm` if the underlying rows are ever migrated.

---

### Task 1: Convert the canonical legacy blob

Establishes detection, id extraction, URL rewriting, and the six output fields, driven end-to-end through the public `DecodeAttachments` API using the exact payload from issue #374.

**Files:**
- Create: `pkg/model/cassandra/attachment_legacy.go`
- Create: `pkg/model/cassandra/attachment_legacy_test.go`
- Modify: `pkg/model/cassandra/attachment.go:48-63` (the `DecodeAttachments` loop)

**Interfaces:**
- Consumes: `Attachment` and `DecodeAttachments(raw [][]byte) (out []Attachment, skipped int)` from `pkg/model/cassandra/attachment.go`.
- Produces: unexported `normalizeAttachment(raw []byte, a *Attachment)` — mutates `a` in place when `raw` holds a legacy blob, otherwise leaves it alone. Task 2 extends the same function and its helpers.

- [ ] **Step 1: Write the failing test**

Create `pkg/model/cassandra/attachment_legacy_test.go`:

```go
package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyIssue374Blob is the exact payload reported in issue #374.
const legacyIssue374Blob = `{
  "image_dimensions": {"height": 215, "width": 426},
  "image_preview": "(base64 thumbnail)",
  "image_size": 29283,
  "image_type": "image/png",
  "image_url": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title": "image (2).png",
  "title_link": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title_link_download": true,
  "type": "file"
}`

func TestDecodeAttachments_LegacyIssue374(t *testing.T) {
	out, skipped := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)

	assert.Equal(t, Attachment{
		ID:                "xh3e4jnJDhEvEy7rk",
		Title:             "image (2).png",
		Type:              "file",
		TitleLink:         "api/v1/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
		TitleLinkDownload: true,
		FileType:          "image/png",
	}, out[0])
}

// The legacy thumbnail and geometry are dropped by design (issue #374), so the
// serialized attachment must not carry them back to the frontend.
func TestDecodeAttachments_LegacyDropsImageExtras(t *testing.T) {
	out, _ := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, out, 1)

	raw, err := json.Marshal(out[0])
	require.NoError(t, err)
	for _, key := range []string{"imageUrl", "imageType", "imageSize", "imageDimensions", "imagePreview"} {
		assert.NotContains(t, string(raw), key)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=pkg/model/cassandra`

Expected: FAIL. `TestDecodeAttachments_LegacyIssue374` reports a diff where the actual value is `Attachment{Title: "image (2).png", Type: "file"}` — every other field empty, because `snake_case` keys do not match the `camelCase` tags.

- [ ] **Step 3: Write the minimal implementation**

Create `pkg/model/cassandra/attachment_legacy.go`:

```go
package cassandra

import (
	"encoding/json"
	"mime"
	"net/url"
	"path"
	"strings"
)

const (
	// legacyAPIPrefix is the route prefix every converted legacy URL is rewritten
	// onto: upload-service's GET /api/v1/file-upload/:fileId/:fileName.
	legacyAPIPrefix = "api/v1/"
	// legacyUploadSegment precedes the file id in a legacy download path.
	legacyUploadSegment = "file-upload"
	defaultFileType     = "application/octet-stream"
)

// legacyAttachment is the pre-migration snake_case attachment shape still
// present in Cassandra rows written by the old stack. Only the fields that
// survive conversion to Attachment are declared.
type legacyAttachment struct {
	Title             string `json:"title"`
	Type              string `json:"type"`
	Description       string `json:"description"`
	TitleLink         string `json:"title_link"`
	TitleLinkDownload bool   `json:"title_link_download"`

	ImageURL  string `json:"image_url"`
	ImageType string `json:"image_type"`
	AudioURL  string `json:"audio_url"`
	AudioType string `json:"audio_type"`
	VideoURL  string `json:"video_url"`
	VideoType string `json:"video_type"`
}

// sourceURL is the legacy download URL, from whichever media field carries it.
func (l *legacyAttachment) sourceURL() string {
	for _, u := range []string{l.TitleLink, l.ImageURL, l.AudioURL, l.VideoURL} {
		if u != "" {
			return u
		}
	}
	return ""
}

// mediaType is the declared MIME type, from whichever media family carries it.
func (l *legacyAttachment) mediaType() string {
	for _, t := range []string{l.ImageType, l.AudioType, l.VideoType} {
		if t != "" {
			return t
		}
	}
	return ""
}

// normalizeAttachment rewrites a in place when raw holds a legacy snake_case
// blob. An attachment carrying id or titleLink is already in the current shape,
// so it is left untouched — which is what keeps future Attachment fields safe
// and makes the conversion idempotent.
func normalizeAttachment(raw []byte, a *Attachment) {
	if a.ID != "" || a.TitleLink != "" {
		return
	}
	var l legacyAttachment
	if err := json.Unmarshal(raw, &l); err != nil {
		return
	}
	src := l.sourceURL()
	if src == "" {
		return
	}
	*a = convertLegacy(&l, src)
}

// convertLegacy builds the current-shape attachment from a legacy blob and its
// download URL. Legacy image geometry and thumbnails are intentionally dropped.
func convertLegacy(l *legacyAttachment, src string) Attachment {
	p := legacyURLPath(src)
	att := Attachment{
		ID:                legacyFileID(p),
		Title:             l.Title,
		Type:              l.Type,
		Description:       l.Description,
		TitleLink:         legacyDownloadURL(p),
		TitleLinkDownload: l.TitleLinkDownload,
		FileType:          strings.ToLower(strings.TrimSpace(l.mediaType())),
	}
	if att.Title == "" {
		att.Title = legacyFileName(p)
	}
	if att.Type == "" {
		att.Type = "file"
	}
	if att.FileType == "" {
		att.FileType = fileTypeFromName(att.Title)
	}
	return att
}

// legacyURLPath reduces an absolute legacy URL to its path, preserving
// percent-encoding. A relative URL is returned unchanged.
func legacyURLPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme == "" && u.Host == "") {
		return raw
	}
	return u.EscapedPath()
}

// legacyDownloadURL rewrites a legacy path onto the current download route. A
// path already carrying the prefix is returned as-is, so converting twice is a
// no-op.
func legacyDownloadURL(p string) string {
	trimmed := strings.TrimPrefix(p, "/")
	if strings.HasPrefix(trimmed, legacyAPIPrefix) {
		return trimmed
	}
	return legacyAPIPrefix + trimmed
}

// legacyFileID extracts the file id: the segment after "file-upload". Falls back
// to the second-to-last segment for an unrecognized layout, since the id always
// precedes the file name.
func legacyFileID(p string) string {
	segs := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range segs {
		if s == legacyUploadSegment && i+1 < len(segs) {
			return segs[i+1]
		}
	}
	if len(segs) >= 2 {
		return segs[len(segs)-2]
	}
	return ""
}

// legacyFileName is the percent-decoded last path segment, used when the legacy
// blob carries no title.
func legacyFileName(p string) string {
	base := path.Base(p)
	if name, err := url.PathUnescape(base); err == nil {
		return name
	}
	return base
}

// fileTypeFromName derives a MIME type from the file name's extension. Media
// type parameters (charset) are stripped so the result is the bare type.
func fileTypeFromName(name string) string {
	ext := strings.ToLower(path.Ext(name))
	if ext == "" {
		return defaultFileType
	}
	parsed, _, err := mime.ParseMediaType(mime.TypeByExtension(ext))
	if err != nil {
		return defaultFileType
	}
	return parsed
}
```

- [ ] **Step 4: Wire it into `DecodeAttachments`**

In `pkg/model/cassandra/attachment.go`, replace the loop body of `DecodeAttachments`. Change:

```go
	for _, b := range raw {
		var a Attachment
		if err := json.Unmarshal(b, &a); err != nil {
			skipped++
			continue
		}
		out = append(out, a)
	}
```

to:

```go
	for _, b := range raw {
		var a Attachment
		if err := json.Unmarshal(b, &a); err != nil {
			skipped++
			continue
		}
		normalizeAttachment(b, &a)
		out = append(out, a)
	}
```

Then update the doc comment above `DecodeAttachments`. Change:

```go
// DecodeAttachments decodes a LIST<BLOB> attachments column (each blob is one
// JSON-encoded Attachment) into typed objects. It is lenient: a malformed blob
// is skipped and counted (returned as skipped) rather than failing the batch, so
// one bad row can't break a history load or a live delivery. Returns (nil, 0)
// for empty input.
```

to:

```go
// DecodeAttachments decodes a LIST<BLOB> attachments column (each blob is one
// JSON-encoded Attachment) into typed objects. It is lenient: a malformed blob
// is skipped and counted (returned as skipped) rather than failing the batch, so
// one bad row can't break a history load or a live delivery. Returns (nil, 0)
// for empty input. Pre-migration snake_case blobs are converted to the current
// shape in place — see normalizeAttachment.
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `make test SERVICE=pkg/model/cassandra`

Expected: PASS, including the pre-existing `TestDecodeAttachments`, `TestEncodeAttachments_RoundTrip` and `TestAttachment_RoundTrip`.

- [ ] **Step 6: Commit**

```bash
git add pkg/model/cassandra/attachment.go pkg/model/cassandra/attachment_legacy.go pkg/model/cassandra/attachment_legacy_test.go
git commit -m "fix(model): convert legacy snake_case attachments on decode

Pre-migration Cassandra rows store attachments with snake_case keys.
encoding/json is case-insensitive but not underscore-insensitive, so
title_link/image_url silently dropped and the frontend received an
attachment with no id, no titleLink and no fileType.

Detect those blobs in DecodeAttachments and convert them to the current
shape, rewriting the download URL onto GET /api/v1/file-upload/:fileId/:fileName.

Refs #374"
```

---

### Task 2: Fallbacks, absolute URLs and idempotence

Hardens the conversion against the legacy URL layouts and missing fields that Task 1's canonical payload does not exercise.

**Files:**
- Modify: `pkg/model/cassandra/attachment_legacy_test.go` (append)
- Modify: `pkg/model/cassandra/attachment_legacy.go` only if a case fails

**Interfaces:**
- Consumes: `normalizeAttachment`, `convertLegacy`, `legacyURLPath`, `legacyDownloadURL`, `legacyFileID`, `legacyFileName`, `fileTypeFromName` from Task 1.
- Produces: nothing new. Task 3 consumes no symbols from this task.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/model/cassandra/attachment_legacy_test.go`:

```go
func TestDecodeAttachments_LegacyVariants(t *testing.T) {
	tests := []struct {
		name string
		blob string
		want Attachment
	}{
		{
			name: "generic file without a media type falls back to the extension",
			blob: `{"title":"report.pdf","type":"file","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "unknown extension falls back to octet-stream",
			blob: `{"title":"CHANGELOG","type":"file","title_link":"/file-upload/abc123/CHANGELOG","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "CHANGELOG", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/CHANGELOG",
				TitleLinkDownload: true, FileType: defaultFileType,
			},
		},
		{
			name: "audio uses audio_url and audio_type",
			blob: `{"title":"clip.mp3","type":"file","audio_url":"/file-upload/aud1/clip.mp3","audio_type":"audio/mpeg","title_link_download":true}`,
			want: Attachment{
				ID: "aud1", Title: "clip.mp3", Type: "file",
				TitleLink: "api/v1/file-upload/aud1/clip.mp3",
				TitleLinkDownload: true, FileType: "audio/mpeg",
			},
		},
		{
			name: "absolute URL is reduced to its path",
			blob: `{"title":"report.pdf","type":"file","title_link":"https://legacy.example.com/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "an already-prefixed path is not prefixed twice",
			blob: `{"title":"report.pdf","type":"file","title_link":"/api/v1/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "unrecognized layout takes the segment before the file name",
			blob: `{"title":"report.pdf","type":"file","title_link":"/ufs/uploads/xyz789/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "xyz789", Title: "report.pdf", Type: "file",
				TitleLink: "api/v1/ufs/uploads/xyz789/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "missing title is taken from the path and percent-decoded",
			blob: `{"type":"file","title_link":"/file-upload/abc123/my%20report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "my report.pdf", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/my%20report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "missing type defaults to file",
			blob: `{"title":"report.pdf","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "description survives the conversion",
			blob: `{"title":"report.pdf","type":"file","description":"Q3 numbers","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file", Description: "Q3 numbers",
				TitleLink: "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "declared media type is lowercased",
			blob: `{"title":"photo.PNG","type":"file","image_type":"IMAGE/PNG","image_url":"/file-upload/abc123/photo.PNG","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "photo.PNG", Type: "file",
				TitleLink: "api/v1/file-upload/abc123/photo.PNG",
				TitleLinkDownload: true, FileType: "image/png",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, skipped := DecodeAttachments([][]byte{[]byte(tt.blob)})
			require.Len(t, out, 1)
			assert.Zero(t, skipped)
			assert.Equal(t, tt.want, out[0])
		})
	}
}

// A current-shape attachment must survive decoding byte-for-byte, including the
// image fields the legacy conversion drops.
func TestDecodeAttachments_CurrentFormatUntouched(t *testing.T) {
	current := Attachment{
		ID: "f1", Title: "photo.png", Type: "file", Description: "team photo",
		TitleLink: "api/v1/file/rooms/r1/file/f1?drive_host=h", TitleLinkDownload: true,
		FileType: "image/png",
		ImageURL: "api/v1/file/rooms/r1/file/f1?drive_host=h", ImageType: "image/png",
		ImageSize: 1234, ImageDimensions: &ImageDimensions{Width: 800, Height: 600},
		ImagePreview: "b64",
	}
	raw, err := json.Marshal(current)
	require.NoError(t, err)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, current, out[0])
}

// An attachment with a titleLink but no id is still current-format, not legacy.
func TestDecodeAttachments_TitleLinkOnlyIsNotLegacy(t *testing.T) {
	raw := []byte(`{"title":"a.png","type":"file","titleLink":"api/v1/file/rooms/r1/file/f1"}`)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Empty(t, out[0].ID)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f1", out[0].TitleLink)
}

// A blob with neither a current-shape nor a legacy URL has nothing to convert
// and must be returned as decoded rather than mangled.
func TestDecodeAttachments_NoURLLeftAsDecoded(t *testing.T) {
	raw := []byte(`{"title":"a.png","type":"file"}`)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, Attachment{Title: "a.png", Type: "file"}, out[0])
}

// A mixed column keeps input order, converts only the legacy entry, and still
// counts the malformed blob as skipped.
func TestDecodeAttachments_MixedFormats(t *testing.T) {
	current, err := json.Marshal(Attachment{ID: "f2", Title: "b.pdf", Type: "file",
		TitleLink: "api/v1/file/rooms/r1/file/f2", TitleLinkDownload: true})
	require.NoError(t, err)

	out, skipped := DecodeAttachments([][]byte{
		[]byte(legacyIssue374Blob),
		current,
		[]byte("{not json"),
	})

	require.Len(t, out, 2)
	assert.Equal(t, 1, skipped)
	assert.Equal(t, "xh3e4jnJDhEvEy7rk", out[0].ID)
	assert.Equal(t, "api/v1/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png", out[0].TitleLink)
	assert.Equal(t, "f2", out[1].ID)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f2", out[1].TitleLink)
}

// Re-encoding a converted attachment and decoding it again must be a fixed
// point — message-worker re-encodes quoted-parent snapshots this way.
func TestDecodeAttachments_LegacyConversionIsIdempotent(t *testing.T) {
	first, _ := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, first, 1)

	second, skipped := DecodeAttachments(EncodeAttachments(first))
	require.Len(t, second, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, first[0], second[0])
}
```

- [ ] **Step 2: Run the tests**

Run: `make test SERVICE=pkg/model/cassandra`

Expected: PASS. Task 1's implementation already covers every case here — these tests pin the behaviour against regression. If any case fails, fix `attachment_legacy.go` (not the test) unless the expectation itself contradicts the spec's conversion table.

- [ ] **Step 3: Verify coverage on the new code**

Run:

```bash
go test -race -coverprofile=/tmp/cov.out ./pkg/model/cassandra/... && go tool cover -func=/tmp/cov.out | grep -E "attachment_legacy|total"
```

Expected: every function in `attachment_legacy.go` at 100.0%, package total above 80%.

If `legacyURLPath`'s `err != nil` branch is uncovered, that is acceptable — `url.Parse` rejects very little, and forcing it costs more than it proves. Every other branch must be covered.

- [ ] **Step 4: Commit**

```bash
git add pkg/model/cassandra/attachment_legacy_test.go pkg/model/cassandra/attachment_legacy.go
git commit -m "test(model): pin legacy attachment conversion edge cases

Covers absolute URLs, already-prefixed paths, unrecognized path layouts,
missing title/type/media-type fallbacks, current-format passthrough,
mixed columns and encode/decode idempotence.

Refs #374"
```

---

### Task 3: Document the behaviour and run the full gates

Records the read-time normalization in the client API reference and proves nothing downstream regressed.

**Files:**
- Modify: `docs/client-api.md` (§3.0 Shared schemas → `#### Attachment`, around line 880)

**Interfaces:**
- Consumes: the completed conversion from Tasks 1-2.
- Produces: nothing.

- [ ] **Step 1: Update the Attachment schema note**

In `docs/client-api.md`, in the `#### Attachment` section, change this paragraph:

```markdown
Render-ready descriptor for an uploaded file. Returned by the upload endpoint
([§2.3](#23-http--protected-image-uploaddownload)), carried (base64-encoded JSON)
into `msg.send` (§4), and returned decoded as objects in message payloads. Media
fields are present only for the matching MIME family.
```

to:

```markdown
Render-ready descriptor for an uploaded file. Returned by the upload endpoint
([§2.3](#23-http--protected-image-uploaddownload)), carried (base64-encoded JSON)
into `msg.send` (§4), and returned decoded as objects in message payloads. Media
fields are present only for the matching MIME family.

Attachments stored in the pre-migration format are converted server-side on
every read, so clients always receive the schema below. A converted attachment
carries `id`, `title`, `type`, `titleLink`, `titleLinkDownload` and `fileType`
(plus `description` when present); its `titleLink` points at
`api/v1/file-upload/{fileId}/{fileName}`, and the media fields are absent.
```

No field is added, removed or retyped, so no request/reply or event struct changed and the derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) need no edit. Confirm that before committing:

```bash
grep -rn "imagePreview\|titleLink" docs/client-api/request-reply.md docs/client-api/events.md | head
```

Expected: matches are field-table rows or examples that reference `Attachment` by link; if either file inlines the full Attachment field table, add the same paragraph there too.

- [ ] **Step 2: Run every affected package's tests**

Run each and confirm PASS:

```bash
make test SERVICE=pkg/model
make test SERVICE=pkg/searchindex
make test SERVICE=history-service
make test SERVICE=broadcast-worker
make test SERVICE=message-worker
make test SERVICE=message-gatekeeper
```

Expected: all PASS. These are every package that calls `DecodeAttachments` or `EncodeAttachments`, plus `message-gatekeeper`, which consumes history-service's decoded output.

- [ ] **Step 3: Run the repo-wide gates**

```bash
make fmt
make lint
make test
make sast
```

Expected: `make lint` clean, `make test` all PASS, `make sast` clean at medium+. Fix anything that fails before committing — do not add a `#nosec` suppression for this change; it introduces no unsafe conversion, no filesystem access and no network call.

- [ ] **Step 4: Commit and push**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note server-side legacy attachment conversion

Refs #374"
git push -u origin claude/issue-374-fix-plan-9mxvme
```

If the push fails on a network error, retry up to 4 times with 2s, 4s, 8s, 16s backoff.

---

## Verification Checklist

Run this end-to-end before declaring the work done:

- [ ] `make test SERVICE=pkg/model/cassandra` passes, including the pre-existing `TestDecodeAttachments`.
- [ ] The issue's exact payload converts to exactly the six fields in the issue — verified by `TestDecodeAttachments_LegacyIssue374`.
- [ ] `imageUrl` / `imagePreview` / `imageDimensions` / `imageSize` are absent from a converted attachment's JSON.
- [ ] A current-format attachment round-trips byte-for-byte, image fields intact.
- [ ] `make test` passes repo-wide.
- [ ] `make lint` and `make sast` are clean.
- [ ] `docs/reviews/` is empty (delete any files there before opening a PR).
- [ ] No file under `upload-service/`, `search-service/`, or `data-migration/` was modified.

## Out of Scope — do not implement

- Elasticsearch reindex or backfill of documents already indexed with a broken legacy attachment. `pkg/searchindex` is fixed for future indexing only.
- Any change to `upload-service`. `GET /api/v1/file-upload/:fileId/:fileName` already exists and is the conversion's target.
- Any Cassandra data migration. The conversion is read-time by design.
