# Design: `client-update-service`

**Status:** Draft — awaiting review
**Date:** 2026-08-03
**Branch:** `claude/client-update-service-a1orrp`

## 1. Problem

A legacy service distributes client software-update artifacts (a `.yaml` update
descriptor and an executable) over HTTP, backed by object storage. In
production it exhibits a memory blowup and a goroutine/disk leak. Four root
causes:

| # | Bug | Effect |
|---|-----|--------|
| 1 | Whole file read into memory (`io.ReadAll`) on both upload and download | RSS scales with file size × concurrent requests → OOM on large executables |
| 2 | Unbounded async cache-write goroutines (one per download, never bounded/awaited) | Goroutine leak under load |
| 3 | Local disk cache with no eviction / TTL / size limit | Disk fills, never reclaimed |
| 4 | Cache read returns a silent nil buffer on miss | Corrupt/empty downloads served without error |

## 2. Goals / Non-goals

**Goals**
- Re-implement the service in this repo following the flat-service + MinIO
  conventions (mirrors `upload-service` / `media-service`).
- **Stream end-to-end** — constant memory regardless of artifact size; no disk
  staging, no `io.ReadAll`, no local cache.
- Preserve the original HTTP surface (upload a config+executable pair; download
  an artifact by file name; health check).
- ≥80% coverage, TDD (red-green-refactor).
- Document the endpoints in `docs/client-api.md` and the derived
  `docs/client-api/request-reply.md` view.

**Non-goals**
- Authentication/authorization. The original is unauthenticated; we match that,
  documenting it as a ⚠️ network-restricted endpoint exactly as
  `media-service`'s `PUT` avatar/emoji endpoints are. Auth is deferred.
- Versioning/promotion logic, signing, delta updates — out of scope; artifacts
  are stored and served verbatim by file name.
- Cross-site federation — this is a local-site artifact store; no INBOX/OUTBOX
  involvement.

## 3. How the four bugs are eliminated

| Original bug | Fix in this design |
|---|---|
| 1 — file buffered in memory | Upload: stream each `*multipart.FileHeader` reader straight into `PutObject(reader, fileHeader.Size)`. Download: `GetObject` → `c.DataFromReader(...)` streams the object body to the client. Peak memory is the fixed transfer buffer, not the file size. |
| 2 — leaked cache goroutines | No cache. Nothing is launched asynchronously; the request goroutine does all the work and returns. |
| 3 — unbounded disk cache | No disk cache. MinIO is the single source of truth. |
| 4 — silent nil on cache miss | No cache path exists; a missing object is an explicit `NoSuchKey` → `404`, never an empty `200`. |

## 4. Architecture

Flat `package main` service at repo root (CLAUDE.md §1), mirroring
`upload-service`. Reuses `pkg/minioutil` (streaming `ObjectStore`),
`pkg/obs`, `pkg/shutdown`, `pkg/errcode` + `errhttp`.

```
client-update-service/
  main.go              # run(): parse config, obs.Init, minio connect + bucket check, gin wiring, shutdown.Wait
  config.go            # typed env config (caarlos0/env)
  handler.go           # handler struct + NewHandler; HandleHealth
  version.go           # HandleUpload (POST) + HandleDownload (GET)
  store.go             # versionStore interface + //go:generate mockgen directive
  store_minio.go       # streaming MinIO impl (Put + Open), cancelReadCloser (ported from upload-service)
  routes.go            # RegisterRoutes(r, h)
  middleware.go        # request-ID + access-log middleware (ported from upload-service)  [if not already shared]
  handler_test.go      # health + wiring unit tests (mocked store)
  version_test.go      # upload/download handler unit tests (mocked store), table-driven
  integration_test.go  # //go:build integration — real MinIO via testutil.MinIO round-trip
  mock_store_test.go   # generated (mockgen), never hand-edited
  deploy/
    Dockerfile         # golang:1.25.12-alpine builder → alpine:3.21 runtime, context = repo root
    docker-compose.yml # service + minio; BOOTSTRAP not applicable (no JetStream)
    azure-pipelines.yml
```

### 4.1 Store interface (consumer-defined, streaming)

Defined in `store.go` next to its consumer (CLAUDE.md §3). Only the methods the
handlers need; accepts interfaces, returns structs.

```go
//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// ErrObjectNotFound is returned (wrapped) by Open when no object matches the key.
var ErrObjectNotFound = errors.New("object not found")

// blobInfo carries the metadata the download handler needs to set response headers.
type blobInfo struct {
    Size        int64
    ContentType string
}

// versionStore is the subset of object storage the update handlers need.
type versionStore interface {
    // Put streams r (of known size) to the object at key. contentType is stored as-is.
    Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
    // Open returns a streaming reader for key plus its metadata, or ErrObjectNotFound (wrapped) when absent.
    Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
}
```

`store_minio.go` implements this over `minioutil.ObjectStore`:
- `Put` → `client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType})`.
- `Open` → `GetObject` + `Stat()` probe (surfaces missing object / dead backend
  before any body is written), wrapped in the `cancelReadCloser` pattern already
  proven in `upload-service/store_minio.go` so the download timeout context is
  released on `Close()`. `minio.ToErrorResponse(err).Code == "NoSuchKey"` maps to
  `ErrObjectNotFound`.

**Object key layout:** `chat.go/chat-versions/<fileName>` — a fixed prefix
constant (`objectPrefix`), mirroring the original's `defaultFilePath` root, so
artifacts are namespaced within a shared bucket. Bucket from `MINIO_BUCKET`;
existence is checked at startup via `minioutil.NewBucket` (fail-fast, does **not**
create — ops/IaC owns buckets), matching every other MinIO service.

### 4.2 Endpoints

All under `/api/v1`. Registered in `routes.go` (CLAUDE.md §6 HTTP rules).

#### `POST /api/v1/version` — upload an update artifact pair
- `Content-Type: multipart/form-data`.
- Body is bounded by `http.MaxBytesReader` at `MAX_UPLOAD_BYTES` before parsing,
  so an oversized upload is rejected without buffering.
- Two form files required: `configFile` (must be `.yaml`/`.yml`) and
  `executeFile` (the executable). Each `*multipart.FileHeader` is opened and its
  reader streamed to `versionStore.Put` with `fileHeader.Size` and a detected
  content-type (`configFile` → `application/x-yaml`; `executeFile` →
  `application/octet-stream`).
- **Success `200`:** `{"result":"success"}`.
- **Errors:** `400` (malformed multipart, missing/empty either file, wrong config
  extension, body over cap) via `errcode.BadRequest`; infra failures →
  `fmt.Errorf(... %w)` → `internal` `500` at the `errhttp.Write` boundary.

#### `GET /api/v1/version/:fileName` — download an artifact
- Validate non-empty, path-clean `fileName` (reject `/`, `..`).
- `versionStore.Open(ctx, key(fileName))`:
  - `ErrObjectNotFound` → `404` `errcode.NotFound("version not found")`.
  - success → set `Content-Type`, `Content-Length` (from `blobInfo.Size`),
    `Content-Disposition: attachment; filename="<fileName>"`, then
    `c.DataFromReader(200, size, contentType, rc, nil)` — Gin streams the body;
    `rc.Close()` deferred.
  - other store errors → wrapped → `500`.

#### `GET /api/v1/health` — liveness
- `200 {"status":"ok"}`. (CLAUDE.md requires `GET /healthz` for HTTP services;
  the original used `/api/v1/health`. **Open decision — see §8.**)

### 4.3 Errors, logging, config, shutdown
- **Errors:** Tier-1 `pkg/errcode` constructors returned from handlers;
  `errhttp.Write(ctx, c, err)` marshals the envelope (CLAUDE.md §3 / §6). No
  bespoke `errors.go`/`response.go` string maps (a source of the original's
  fragility).
- **Logging:** `log/slog` JSON only; o11y gin middleware + request-ID +
  access-log (method, path, status, latency, request-ID) ported from
  `upload-service/middleware.go`. Never log file bytes.
- **Config (`config.go`, `caarlos0/env`):**

  | Env | Default | Notes |
  |---|---|---|
  | `PORT` | `8080` | |
  | `SITE_ID` | *(required)* | Standard service tag |
  | `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | *(required)* | Secrets never defaulted |
  | `MINIO_USE_SSL` | `false` | |
  | `MINIO_BUCKET` | *(required)* | Fail-fast if absent |
  | `MINIO_DOWNLOAD_TIMEOUT` | `5m` | Bounds Stat+stream |
  | `MAX_UPLOAD_BYTES` | `524288000` (500 MiB) | `MaxBytesReader` cap; tune to real artifact size |
  | `HTTP_WRITE_TIMEOUT` | `10m` | Generous — large executables stream slowly |

- **Shutdown:** `pkg/shutdown.Wait`; HTTP order `nc.Drain()` n/a (no NATS) →
  server graceful shutdown → nothing else to close (MinIO client is stateless).
  25s budget < K8s 30s grace.

## 5. Testing plan (TDD)

Red first, then implement. `package main` tests to reach unexported types.

**Unit (`version_test.go`, mocked `versionStore`), table-driven:**
- Upload: valid pair → `Put` called twice with expected keys/sizes, `200`;
  missing `configFile`; missing `executeFile`; empty file; wrong config
  extension; malformed multipart; store `Put` error → `500`; oversized body → `400`.
- Download: hit → streamed bytes + headers asserted; empty/`..`/`/` name → `400`;
  `ErrObjectNotFound` → `404`; store error → `500`.
- Health: `200 {"status":"ok"}`.

**Integration (`integration_test.go`, `//go:build integration`):**
- `TestMain` → `testutil.RunTests(m)`.
- `testutil.MinIO(t, "clientupdate")` → real round-trip: upload a small
  config+exe pair, download each back, assert byte-identity and content-type.
- Missing object → `404`.

**Coverage:** target ≥90% on `version.go`/`store_minio.go`; verify via
`make test SERVICE=client-update-service` + coverprofile.

## 6. Documentation updates (in this PR)

Per CLAUDE.md §5, HTTP file services are documented in `docs/client-api.md`
(precedent: §7 Media Service) and mirrored in `docs/client-api/request-reply.md`.

1. **`docs/client-api.md` — new `## 12. Client Update Service`** (after §11
   tcard-service), following the §7 Media Service format: per-endpoint
   auth/request/response tables, status-code matrix, a JSON success example for
   the upload, and the ⚠️ unauthenticated warning banner. Add the section to the
   Table of contents.
2. **`docs/client-api/request-reply.md` — new HTTP block** in the `HTTP —`
   family (alongside the Media Service entries), a compact endpoint table
   (`Endpoint | Reply | Purpose`) with `**Emits:** None — HTTP-only.` and a link
   back to `../client-api.md#12-client-update-service`.

`docs/client-api/events.md` is **not** touched — no server→client events.

## 7. Out-of-scope / follow-ups
- Authentication (deferred, documented as network-restricted).
- Artifact listing / delete / GC endpoints.
- Checksums/signatures for integrity verification.

## 8. Open decisions for review

1. **Health path.** CLAUDE.md §6 mandates `GET /healthz` for HTTP services, but
   the original (and its clients) use `GET /api/v1/health`. **Recommendation:**
   expose **both** — `/healthz` (convention) and `/api/v1/health` (compat), same
   handler. Cheap, keeps existing clients working. OK?
2. **Upload field names.** Keeping the original `configFile` / `executeFile`
   multipart field names for client compatibility. Confirm the real clients use
   these exact names (else we rename before release).
3. **`MAX_UPLOAD_BYTES` default.** 500 MiB is a placeholder ceiling; set it to
   the true max executable size + headroom.
4. **Bucket sharing vs. dedicated.** Design assumes a dedicated
   `MINIO_BUCKET` for update artifacts (prefix `chat.go/chat-versions/`). If you
   want it to share an existing bucket, only the env value changes — no code
   change.
```
