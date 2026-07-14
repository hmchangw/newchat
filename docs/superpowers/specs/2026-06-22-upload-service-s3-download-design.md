# upload-service: backward-compatible S3 file download — Design

**Date:** 2026-06-22
**Service:** `upload-service`
**Status:** Approved

## Goal

Add a backward-compatible endpoint that streams a previously-uploaded file out of
MinIO/S3:

```
GET /api/v1/file-upload/:fileId/:fileName[?download]
```

Files were written to a MinIO bucket by an **external (legacy Node.js) system** —
that upload path is **not** part of this repo. Their metadata lives in the Mongo
`uploads` collection. This endpoint only **reads** metadata and **streams** the
object out; it never writes to S3 or Mongo.

This is distinct from the Drive-backed image upload/download added in PR #353
(`/api/v1/rooms/:roomId/upload/images`, `/api/v1/rooms/:roomId/image/:fileId`),
which remains untouched.

## Decisions (resolved during brainstorming)

| Question | Decision |
|----------|----------|
| Auth rejection status | Reuse the existing `authMiddleware`; missing/invalid `ssoToken` → **401** (the task's "403" is loose wording). |
| Authorization | Authenticated **and** a member of the upload's room (`rid`). |
| Route prefix | Mounted under the existing `/api/v1` group: `/api/v1/file-upload/:fileId/:fileName`. |
| `complete` flag | Ignored — serve any matching document. |
| `docs/client-api.md` | Updated in the same change (§2.3), consistent with PR #353. |

## The `uploads` document

Legacy shape (single owning external system). Example:

```json
{
  "_id": "file_zyz_789",
  "userId": "user_abc123",
  "rid": "r1",
  "name": "quaterly-report.pdf",
  "type": "application/pdf",
  "size": 2458624,
  "store": "AmazonS3:Uploads",
  "complete": true,
  "AmazonS3": { "path": "app-instance-001/uploads/r1/user_abc123/file_xyz789" },
  "uploadedAt": "2026-06-15T10:00:00.000Z"
}
```

Fields this endpoint reads (everything else is ignored, projected out):

| Field | Use |
|-------|-----|
| `_id` | Lookup key (the `:fileId` path param). |
| `rid` | Room ID — membership authorization. |
| `name` | `Content-Disposition` filename. |
| `type` | `Content-Type` (fallback `application/octet-stream` when empty). |
| `size` | `Content-Length`. |
| `AmazonS3.path` | The S3 object key — used **verbatim**, not reconstructed. |

The key format `{uniqueID}/uploads/{roomId}/{userID}/{fileID}` is informational;
the value is read straight from `AmazonS3.path`.

## Request flow — `HandleDownloadMinioS3File`

1. **Auth** is enforced upstream by the `/api/v1` group's `authMiddleware`
   (missing/invalid `ssoToken` → **401**, before the handler runs).
2. Extract `fileId` from the path; empty → **400**. `:fileName` and the
   `?download` query are accepted and **ignored**.
3. Resolve `user` from context; missing → **500** (matches the existing handlers).
4. `store.GetUpload(ctx, fileId)`:
   - not found → **404** (`errcode.NotFound`)
   - store/infra error → **500** (raw wrapped error collapses to `internal`)
5. `requireMembership(ctx, c, upload.RID, user.Account)` (reused unchanged):
   - non-member → **403** with reason `not_room_member`
   - store error → **500**
6. `s3.Open(ctx, upload.AmazonS3.Path)` — `GetObject` then `Stat` so any S3
   failure (missing object, unreachable backend) surfaces **before** the
   response body is written → **503** (`errcode.Unavailable`, infra cause attached).
7. Stream via `c.DataFromReader(200, upload.Size, contentType, reader, extraHeaders)`
   — the body is piped straight from S3 to the client, never fully buffered.

### Response headers

| Header | Value |
|--------|-------|
| `Content-Disposition` | `attachment; filename*=UTF-8''<percent-encoded upload.name>` |
| `Content-Type` | `upload.type` (fallback `application/octet-stream`) |
| `Content-Length` | `upload.size` |
| `Content-Security-Policy` | `default-src 'none'` |
| `Cache-Control` | `max-age=31536000` |

`filename*` uses RFC 5987 percent-encoding (`url.QueryEscape` with `+`→`%20`),
mirroring the legacy `encodeURIComponent(file.name)`.

## Components

### `store.go` / `store_mongo.go`
- New `upload` DB DTO (bson tags only — a read DTO, not a wire model):
  `ID`,`UserID`,`RID`,`Name`,`Type`,`Size`,`AmazonS3.Path`.
- `Store` interface gains `GetUpload(ctx, fileID string) (*upload, error)`.
- `ErrUploadNotFound` sentinel + `errIsUploadNotFound` helper (mirrors `ErrRoomNotFound`).
- `mongoStore` gains the `uploads` collection; `GetUpload` is a `FindOne` by `_id`
  with an **explicit projection** of only the six read fields; `mongo.ErrNoDocuments`
  → wrapped `ErrUploadNotFound`.
- Regenerate `mock_store_test.go` via `make generate SERVICE=upload-service`.

### `objectStore` interface + MinIO impl (`handler.go` / new `store_minio.go`)
- Consumer-defined interface (mirrors `driveClient`):
  ```go
  type objectStore interface {
      Open(ctx context.Context, key string) (io.ReadCloser, error)
  }
  ```
- `minioObjectStore{client *minio.Client, bucket string}`: `Open` calls
  `GetObject` + `Stat`; on `Stat` error it closes and returns a wrapped error so
  the handler maps it to 503. Lives in `store_minio.go`.

### `Handler`
- Gains an `s3 objectStore` field; `NewHandler` signature gains the arg.
  Existing call sites (`main.go`, `handler_test.go` `newHandler`) updated.

### `main.go` config
| Env | Required | Default |
|-----|----------|---------|
| `MINIO_ENDPOINT` | yes | — |
| `MINIO_ACCESS_KEY` | yes (secret) | — |
| `MINIO_SECRET_KEY` | yes (secret) | — |
| `MINIO_USE_SSL` | no | `false` |
| `MINIO_BUCKET` | no | `chat-{SITE_ID}` (computed at startup when empty) |

Connect via `pkg/minioutil.Connect(ctx, endpoint, useSSL, accessKey, secretKey)`,
build `minioObjectStore`, pass into `NewHandler`. No fail-fast bucket probe at
startup (S3 errors are handled per-request as 503).

### `routes.go`
- `api.GET("/file-upload/:fileId/:fileName", h.HandleDownloadMinioS3File)` inside the
  existing authenticated `/api/v1` group.

### Deploy
- Add the `MINIO_*` env block to `upload-service/deploy/docker-compose.yml`.
  Dockerfile and azure-pipelines unchanged.

### Docs
- `docs/client-api.md` §2.3: document the new download endpoint (request params,
  ignored `?download`, response headers, and the 400/401/403/404/503 cases).

## Error mapping summary

| Condition | Status | errcode |
|-----------|--------|---------|
| Missing/invalid `ssoToken` (middleware) | 401 | `Unauthenticated` |
| Empty `fileId` | 400 | `BadRequest` |
| No user in context | 500 | `Internal` |
| Upload not found | 404 | `NotFound` |
| Not a room member | 403 | `Forbidden` (`not_room_member`) |
| Store / membership infra error | 500 | `internal` (raw wrap) |
| S3 GetObject/Stat failure | 503 | `Unavailable` |

## Testing (TDD — Red → Green → Refactor)

**Unit (`handler_test.go`, mocked `Store` + `fakeS3`):**
- empty `fileId` → 400
- no user in context → 500
- `GetUpload` not-found → 404
- `GetUpload` store error → 500
- not a member → 403 / `not_room_member`
- `s3.Open` error → 503 / `unavailable`
- success → 200, body bytes streamed, asserts every header (Content-Disposition,
  Content-Type, Content-Length, CSP, Cache-Control)
- route registration: `GET /api/v1/file-upload/f1/name.pdf` with no `ssoToken` → 401

**Integration:**
- `GetUpload` against `testutil.MongoDB` (found + not-found), explicit projection
  honored.
- MinIO streaming via `testutil.MinIO`: put an object, `Open` + read returns the
  bytes; missing key returns an error.

Coverage target ≥ 80% (handlers/store toward 90%+).

## Out of scope
- The upload-to-S3 path (external system).
- Any change to the Drive image upload/download endpoints.
- Honoring the `complete` flag, `store` discrimination, or multi-bucket routing.
