# Legacy Attachment Normalization — Design

**Issue:** [#374](https://github.com/hmchangw/newchat/issues/374)

## Problem

Some Cassandra `messages_by_room.attachments` blobs were written by the previous
stack in a `snake_case` shape:

```json
{
  "image_dimensions": {"height": 215, "width": 426},
  "image_preview": "(base64 thumbnail)",
  "image_size": 29283,
  "image_type": "image/png",
  "image_url": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title": "image (2).png",
  "title_link": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title_link_download": true,
  "type": "file"
}
```

`cassandra.Attachment` declares `camelCase` JSON tags. `encoding/json` field
matching is case-insensitive but **not** underscore-insensitive, so `title_link`
does not match `titleLink`, `image_url` does not match `imageUrl`, and so on.

A legacy blob therefore decodes today into `{title, type}` and nothing else — no
`id`, no `titleLink`, no `fileType`. The frontend gets an attachment it cannot
render and cannot download. The failure is silent: `DecodeAttachments` reports
`skipped` only for blobs that fail to parse as JSON, and a legacy blob parses
fine.

## Goal

Convert a legacy blob to the current shape at read time, so the frontend
receives:

```json
{
  "id": "xh3e4jnJDhEvEy7rk",
  "title": "image (2).png",
  "type": "file",
  "titleLink": "api/v1/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "titleLinkDownload": true,
  "fileType": "image/png"
}
```

The rewritten `titleLink` resolves against `upload-service`'s existing route
`GET /api/v1/file-upload/:fileId/:fileName` (`upload-service/routes.go:15`,
handler `HandleDownloadMinioS3File`), so downloads work with no upload-service
change.

## Where the conversion goes

`cassandra.DecodeAttachments` (`pkg/model/cassandra/attachment.go:48`) is the
single choke point every read path already funnels through:

| Caller | Path served |
|---|---|
| `history-service/internal/service/attachments.go:26` | history, threads, pins, get-by-id |
| `broadcast-worker/handler.go:1208` | live `ClientMessage` fan-out |
| `pkg/searchindex/messagedoc.go:89` | ES document build |

`message-gatekeeper`'s `quotedParentProjection`
(`message-gatekeeper/fetcher_history.go:59`) consumes history-service's
already-decoded reply, so it inherits the fix transitively.

Converting inside `DecodeAttachments` fixes all four with no service-side edit
and no new interface.

## Detection rule

Decode each blob into `Attachment` first. **If `id` and `titleLink` are both
empty**, re-decode as `legacyAttachment` (`snake_case` tags). If that yields a
legacy URL (`title_link` / `image_url` / `audio_url` / `video_url`), convert;
otherwise return the original decode untouched.

`upload-service`'s `buildAttachment` (`upload-service/attachment.go:26`) sets
`ID` and `TitleLink` unconditionally on every new attachment, so a current-format
blob can never enter the legacy branch. Fields added to `Attachment` in the
future pass through untouched. This is what makes the change forward-compatible.

## Conversion rules

| Target field | Source |
|---|---|
| `id` | path segment immediately following `file-upload`; fallback: second-to-last segment; fallback: `""` |
| `title` | `title`; fallback: percent-decoded last path segment |
| `type` | `type`; fallback: `"file"` |
| `titleLink` | `"api/v1/" + strings.TrimPrefix(path, "/")` |
| `titleLinkDownload` | `title_link_download` |
| `description` | `description` (same key in both formats; feeds ES `attachmentText`) |
| `fileType` | first non-empty of `image_type` / `audio_type` / `video_type`; fallback: `mime.TypeByExtension` on the title; fallback: `application/octet-stream` |

Source URL = first non-empty of `title_link`, `image_url`, `audio_url`,
`video_url`. An absolute URL is reduced to its path via `url.Parse` /
`EscapedPath` before rewriting, so percent-encoding survives verbatim
(`image%20(2).png` stays `image%20(2).png`).

**Dropped by design:** `image_size`, `image_preview`, `image_dimensions`. Output
is exactly the six fields in the issue plus `description` when the legacy blob
carries one. Decided in brainstorming; the legacy thumbnail is not carried
forward.

**Idempotence:** a path already starting `api/v1/` is not prefixed again, so
running the conversion twice is a no-op.

## Data safety

Cassandra is never rewritten. The conversion is a pure read-time function, so:

- It is idempotent on every read.
- A code rollback restores exactly today's behaviour — no migration to undo.
- No write path is touched.

One benign side effect: `message-worker/store_cassandra.go:336` re-encodes a
**quoted-parent snapshot** via `EncodeAttachments`. After this change those
snapshots are persisted in the current format. That is self-healing, not a
regression — the snapshot is a derived copy, never the original message row.

## Out of scope

- **Elasticsearch backfill.** `pkg/searchindex` gets the fix for free, so newly
  indexed messages are correct. Documents already indexed with a broken legacy
  attachment stay as they are; no reindex is committed here. Decided in
  brainstorming.
- **`upload-service`.** The download route already exists and already works; the
  conversion targets it. No handler change.
- **Cassandra data migration.** Read-time conversion is the fix.

## Testing

Unit tests only — the change is a pure function over JSON bytes, so no
container, no NATS, no database. Tests live in
`pkg/model/cassandra/attachment_legacy_test.go` (`package cassandra`, per the
in-package convention).

Coverage must stay above the 80% floor; the new file is small and fully
exercised by the table-driven cases.
