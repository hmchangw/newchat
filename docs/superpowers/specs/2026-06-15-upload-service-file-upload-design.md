# Design: File Upload API for `upload-service` (pure HTTP)

**Date:** 2026-06-15
**Status:** Approved design (revised — pure-HTTP upload, `File` removed everywhere) — pending implementation plan

## 1. Goal

Add an authenticated endpoint that lets a room member upload a single file
(image/audio/video/document) and stores it in the internal Drive, returning a
render-ready **attachment** the frontend can later use to compose and publish a
normal chat message. upload-service is a **pure HTTP service** — it does not
publish messages or talk to NATS. The file-message concept on the data model is
collapsed: the `File` field is removed everywhere (including the Cassandra
schema); all per-file metadata now lives inside the attachment.

```
POST /api/v1/rooms/:roomId/upload      (multipart/form-data)
```

## 2. Scope & non-goals

- **In scope (one cohesive PR), three task groups:**
  - **A. upload endpoint** — the pure-HTTP upload-service handler.
  - **B. gatekeeper attachments** — `message-gatekeeper` accepts/validates
    `attachments` on the normal `msg.send` request and carries them into the
    canonical message.
  - **C. `File` removal** — delete the `File` field/type/column/UDT across models,
    Cassandra schema, message-worker, and history-service.
- **Non-goals:**
  - No frontend changes. The frontend later reads the upload response's
    `attachments`, composes a `msg.send` carrying them, and publishes it like a
    normal message — that is a separate follow-up PR.
  - upload-service never publishes a message and has no NATS dependency.
  - The production Cassandra `ALTER TABLE … DROP file` / `DROP TYPE "File"` is an
    ops/IaC migration, flagged in the PR description but **not** executed by this
    code. This PR only changes the init DDL (fresh/local envs) and the Go code.

## 3. upload-service — pure HTTP

### Request — `multipart/form-data`

| Part | Kind | Required | Notes |
|------|------|----------|-------|
| `ssoToken` | header | yes | OIDC SSO token (same as the image endpoints). |
| `roomId` | path | yes | Target room (the Drive group). |
| `file` | file | yes | The single uploaded file. |
| `description` | field | no | Optional attachment description. |

### Flow (`HandleUploadFile`)

1. Resolve `roomId`; require an authenticated user with a non-empty email
   (mirrors `HandleUploadImages`).
2. **Membership** — existing `store.IsMember(ctx, roomID, account)`; not a member
   ⇒ `403 RoomNotMember`. (No user-ID lookup is needed — no message is built.)
3. `store.GetRoomSiteID(ctx, roomID)` → Drive origin; `ErrRoomNotFound` ⇒ `404`.
4. Take the single `file` part. **Validate:**
   - **Size:** reject when `header.Size > FILE_UPLOAD_MAX_FILE_SIZE`
     (default `104857600`; `-1` = unlimited).
   - **MIME:** blacklist first (deny wins), then whitelist (if non-empty, must
     match). See §6.
5. For `image/*` only, read the bytes once (for preview + dimensions). Non-image
   types stream straight to Drive without buffering.
6. **Upload to Drive** (reuse the MIME-agnostic bulk upload with a one-element
   slice) → `fileId` and `fileSize` (see §7).
7. Build the attachment(s) via `buildAttachment` (§5).
8. Respond `200`:

```json
{
  "success": true,
  "attachments": [
    {
      "id": "drive-file-id",
      "title": "report.pdf",
      "type": "file",
      "description": "Q2 report",
      "titleLink": "api/v1/rooms/room-1/image/drive-file-id?drive_host=https://drive",
      "titleLinkDownload": true
    }
  ]
}
```

### Errors (via `errhttp.Write` / `errcode`)

| Status | When |
|--------|------|
| 400 | missing `roomId`/`file`; not multipart; size over cap; MIME blocked. |
| 401 | missing/invalid/expired `ssoToken`. |
| 403 | caller is not a room member (`RoomNotMember`). |
| 404 | room not found. |
| 500 | user not authenticated / missing email; read failure. |
| 503 | Drive upload failure. |

### Config (upload-service)

Keep: existing image-endpoint vars. Add: `FILE_UPLOAD_MAX_FILE_SIZE`
(default `104857600`, `-1` = unlimited), `FILE_UPLOAD_MEDIA_TYPE_WHITELIST`
(default `""`), `FILE_UPLOAD_MEDIA_TYPE_BLACKLIST` (default `"image/svg+xml"`).
**No** `NATS_URL` / `NATS_CREDS_FILE` / reply-timeout — upload-service has no NATS.

## 4. `Attachment` (new in `pkg/model/cassandra`, aliased in `pkg/model`)

The render-ready descriptor. It is returned by the upload endpoint and, later
(frontend follow-up), JSON-encoded + base64-encoded into each
`Message.Attachments` blob. `title` is the file name; there is no separate
`name`. `id` carries the Drive fileId (the only place the file identity is
recorded now that `File` is gone).

**Location:** defined in `pkg/model/cassandra` (so `cassandra.Message` can embed
`[]Attachment` for the decoded read path — see §8.1 — without the
`pkg/model → pkg/model/cassandra` import cycle), aliased as `model.Attachment` /
`model.ImageDimensions` in `pkg/model`. JSON tags only (it is serialized whole as
a blob / HTTP body, never a Cassandra column or Mongo document).

```go
type ImageDimensions struct {
	Width  int `json:"width"  bson:"width"`
	Height int `json:"height" bson:"height"`
}

type Attachment struct {
	ID                string `json:"id"                bson:"id"`
	Title             string `json:"title"             bson:"title"`
	Type              string `json:"type"              bson:"type"`
	Description       string `json:"description,omitempty"       bson:"description,omitempty"`
	TitleLink         string `json:"titleLink"         bson:"titleLink"`
	TitleLinkDownload bool   `json:"titleLinkDownload" bson:"titleLinkDownload"`

	ImageURL        string           `json:"imageUrl,omitempty"        bson:"imageUrl,omitempty"`
	ImageType       string           `json:"imageType,omitempty"       bson:"imageType,omitempty"`
	ImageSize       int64            `json:"imageSize,omitempty"       bson:"imageSize,omitempty"`
	ImageDimensions *ImageDimensions `json:"imageDimensions,omitempty" bson:"imageDimensions,omitempty"`
	ImagePreview    string           `json:"imagePreview,omitempty"    bson:"imagePreview,omitempty"`

	AudioURL  string `json:"audioUrl,omitempty"  bson:"audioUrl,omitempty"`
	AudioType string `json:"audioType,omitempty" bson:"audioType,omitempty"`
	AudioSize int64  `json:"audioSize,omitempty" bson:"audioSize,omitempty"`

	VideoURL  string `json:"videoUrl,omitempty"  bson:"videoUrl,omitempty"`
	VideoType string `json:"videoType,omitempty" bson:"videoType,omitempty"`
	VideoSize int64  `json:"videoSize,omitempty" bson:"videoSize,omitempty"`
}
```

## 5. Attachment construction (`buildAttachment`)

- **fileURL** = `api/v1/rooms/{roomId}/file/{fileId}?drive_host={driveHost}`
  (reuses the existing protected download route; `driveHost` from
  `drive.GetBaseURLFromRoomOrigin(siteID)`).
- Base fields: `id`=fileId, `title`=file name, `type`=`"file"`, `description`
  (omitempty), `titleLink`=fileURL, `titleLinkDownload`=`true`. Then by MIME
  prefix:
  - `image/*`: `imageUrl`=fileURL, `imageType`=mime, `imageSize`=size,
    `imageDimensions` (when decodable), `imagePreview` (`resizeImagePreview`).
  - `audio/*`: `audioUrl`, `audioType`, `audioSize`.
  - `video/*`: `videoUrl`, `videoType`, `videoSize`.
  - otherwise: base fields only.
- **`resizeImagePreview(data, mime) (string, error)`**: decode (`image/jpeg`,
  `image/png`) → 32×32 via `golang.org/x/image/draw` (CatmullRom) → small box
  blur → JPEG encode → base64. Non-decodable types ⇒ `("", nil)`.
- **`imageDimensions(data) *model.ImageDimensions`**: `image.DecodeConfig`
  (header only); nil when undecodable.

## 6. MIME allow/deny filter

Two env vars, comma-separated media-type lists with wildcards
(`*`, `image/*`, exact `audio/mpeg`):
- `FILE_UPLOAD_MEDIA_TYPE_WHITELIST` — default `""` (empty ⇒ all allowed).
- `FILE_UPLOAD_MEDIA_TYPE_BLACKLIST` — default `"image/svg+xml"`.

Order: blacklist match ⇒ reject (`400`); else non-empty whitelist with no match ⇒
reject (`400`); else allow. Matching: case-insensitive exact, `type/*` prefix
wildcard, or bare `*`. Lives in a small unit-testable helper (`mediaTypeFilter`).

## 7. Drive `fileSize`

The Drive bulk-upload per-file object (`drive.GroupImageObject`) gains
`FileSize int64 \`json:"fileSize"\``. The attachment's `imageSize`/`audioSize`/
`videoSize` use this Drive-reported size (authoritative post-upload), not the
multipart header size.

## 8. message-gatekeeper — validate attachments on `msg.send`

The frontend (later) sends a normal `msg.send` carrying `attachments`. Changes:

1. **`model.SendMessageRequest`**: add `Attachments [][]byte \`json:"attachments,omitempty"\``.
   (No `File` — see §9.)
2. **`processMessage`** (`message-gatekeeper/handler.go`):
   - Copy `req.Attachments` onto the canonical `model.Message`.
   - **Relax empty content:** reject empty only when there are no attachments
     (`req.Content == "" && len(req.Attachments) == 0`). The 20 KB content cap
     still applies.
   - Enforce caps: `len(req.Attachments) > maxAttachments` (1) ⇒ reject; total
     attachment bytes `> maxAttachmentBytes` (8192 — a realistic image Attachment
     JSON is ~1.5 KB incl. the ~900 B preview; the blob is opaque to gatekeeper so
     this is the only bound on the encrypted row) ⇒ reject. **Package
     constants** in `handler.go`, matching the existing `maxContentBytes`.
   - Errors are typed `errcode.BadRequest` (reply + Ack).
3. The reply `json.Marshal(msg)` now includes `attachments` (omitempty) naturally.

Storage is unchanged: `Attachments [][]byte` ↔ Cassandra `LIST<BLOB>`, written as
opaque blobs (each blob is one base64-decoded JSON `Attachment`). The gatekeeper
does **not** decode/validate blob contents — only count + total-byte caps.

### 8.1 History read path — decode blobs into `[]Attachment`

Storage stays `LIST<BLOB>`; the **read path** converts. `cassandra.Message`
(the history response struct, `type Message = cassandra.Message`) keeps its raw
`Attachments [][]byte` for the gocql scan but stops serializing it (`json:"-"`),
and gains `DecodedAttachments []Attachment` (`json:"attachments,omitempty"
cql:"-"`; `structScan` skips `cql:"-"`). history-service fills the decoded field
**after redaction, immediately before returning** each client response
(LoadHistory / Next / Surrounding / Threads / Pins), via a lenient
`setDecodedAttachments` (a malformed blob is logged and skipped, never fatal). A
redacted stub has its raw `Attachments` already nil'd, so it decodes to nil.

### 8.2 Live broadcast path — same decoding

Real-time delivery returns the same `Attachment[]` shape as history.
`broadcast-worker` delivers created messages as `model.ClientMessage` (via
`buildClientMessage`). `ClientMessage` embeds `Message` inline, so an outer
`Attachments []Attachment` (`json:"attachments,omitempty"`) **shadows** the
promoted raw `Message.Attachments` in JSON; `buildClientMessage` fills it via the
shared `cassandra.DecodeAttachments`. Edits/deletes carry no attachments
(`EditRoomEvent` has only `NewContent`), so only the created path changes.

**Intentionally unchanged (PR-description callouts):**
- The internal `getMessageByID` RPC consumer (gatekeeper `FetchQuotedParent`)
  never reads attachments, so dropping raw `attachments` from `cassandra.Message`
  JSON is safe.
- The canonical `model.Message` keeps raw `Attachments [][]byte`
  (`json:"attachments"`) for the write pipeline; only the client-delivery
  `ClientMessage` is reshaped.
- The quoted-parent snapshot's attachments are decoded too (same retag + decode in
  both paths). Safe because no producer populates quoted-parent attachments today,
  so `json:"-"` drops an always-empty field from the canonical event; the
  `buildClientMessage` live path clones the quoted parent before filling its
  decoded field so the shared canonical `*msg` is not mutated.

## 9. Remove `File` everywhere

- **Models:** delete `cassandra.File` (`pkg/model/cassandra/message.go`) and the
  `Message.File` field there; delete `model.Message.File`
  (`pkg/model/message.go`); delete `SendMessageRequest.File`; delete the
  `type File = cassandra.File` alias in `history-service/internal/models`.
- **Cassandra schema (init DDL + doc):**
  - Drop the `file FROZEN<"File">` column from `messages_by_room`,
    `thread_messages_by_thread`, `pinned_messages_by_room`, `messages_by_id`
    (`docker-local/cassandra/init/10..13-*.cql`).
  - Delete `docker-local/cassandra/init/05-udt-file.cql`.
  - Update `docs/cassandra_message_model.md` (remove the `File` UDT, the `file`
    column in all four tables, and the `file` mentions in the
    plaintext-columns / encryption sections).
- **message-worker** (`store_cassandra.go`, `handler.go`): remove `file` from the
  5 INSERT column lists + bound values, and remove `File` from the encrypted
  `EncryptedFields` bundle (so plaintext `file` is no longer extracted/nulled).
- **history-service** (`internal/cassrepo/*`): remove `file` from the 4 SELECT
  column lists (`messages_by_room.go`, `pin.go`, `thread_messages.go`,
  `messages_by_id`), drop the `file = null` clauses from the encrypted edit
  UPDATE statements (`write.go`), remove the `file *cassmodel.File` scan var, and
  the `pinned[i].File = nil` line in `internal/service/pin.go`.
- **Tests:** update `pkg/model/cassandra/message_test.go`,
  `history-service/internal/cassrepo/*_integration_test.go`, and any message-worker
  test that asserts on `File` to drop those fields/assertions.

**Backward compatibility:** existing Cassandra rows may still hold `file` data;
once the SELECT column lists drop `file`, it is simply never read. Encrypted
messages predating this change carry `File` inside their enc payload bundle —
decoding ignores the unknown field (the struct no longer has it). No data is
rewritten by this PR.

## 10. Testing (TDD)

- **upload-service:** `HandleUploadFile` — success (asserts `attachments[]`, no
  `file`/`message` wrapper), not-member (403), room-not-found (404), missing file
  (400), oversize (400), blocked MIME (400), Drive error (503); `mediaTypeFilter`
  table tests; `resizeImagePreview`/`imageDimensions` with `testdata` fixtures.
  All unit, mocked store + fake Drive, **no NATS**.
- **gatekeeper:** attachments carried into the canonical `Message`; empty content
  allowed with attachments but rejected without; count/byte caps enforced.
- **File removal:** existing cassandra/history/message-worker tests updated and
  green; `pkg/model` round-trip no longer references `File`; schema-doc / struct /
  init-DDL parity holds (CLAUDE.md Cassandra rule).
- Coverage ≥ 80% (target 90%+ for the upload handler + attachment builder + the
  gatekeeper additions).

## 11. Assumptions

- Single file per request → `attachments` is a one-element array.
- The download URL uses the protected download route (see §12) for all file types.
- `description` is the only non-file form field upload-service reads; `content`,
  thread, and `tshow` belong to the frontend's later `msg.send`.
- Generic (non-media) files carry no MIME field on the attachment (only `type:
  "file"`); media MIME lives in `imageType`/`audioType`/`videoType`.

## 12. Protected download endpoint — rename `/image/:fileId` → `/file/:fileId`

The single protected-download endpoint already serves arbitrary uploaded files
(`HandleDownloadImage` streams raw bytes with the stored content-type, defaulting
to `application/octet-stream`; it is not image-specific), and the upload-file
attachment links (`fileURL`, §5) point at it. Rename the path so it reflects what
it serves. **Hard rename — the `/image/` route is dropped (no alias).**

- **Route + handler:** `GET /api/v1/rooms/:roomId/file/:fileId` replaces
  `…/image/:fileId`; `HandleDownloadImage` → `HandleDownloadFile` (behavior
  unchanged; doc-comment and the client-facing error string `"failed to retrieve
  image"` → `"failed to retrieve file"`).
- **URL builder:** `fileURL` (§5) emits `/file/`. This one builder feeds both the
  file-upload attachment links and the image-upload `relativePath`, so both point
  at the new route.
- **Out of scope:** `pkg/drive.GetGroupImage` keeps its name (a Drive-side "group
  image" concept; the handler still calls it). The image *upload* route
  (`POST …/upload/images`, `HandleUploadImages`) is unchanged — only its emitted
  download URL shifts to `/file/`.
- **Docs (`docs/client-api.md`):** rename the GET endpoint heading/path and its
  "protected image" wording to "file"; update the two example JSONs (image-upload
  `relativePath`, file-upload `titleLink`) to `/file/`; adjust the §2.3 section
  title to cover file (not only image) download.
- **Backward compatibility:** accepted tradeoff — any `/image/` download URLs
  already persisted in stored messages will 404 after this change.
