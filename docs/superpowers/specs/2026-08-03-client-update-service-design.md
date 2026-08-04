# Client Update Service — streaming MinIO artifact store with TTL download cache

**Date:** 2026-08-03
**Service:** `client-update-service` (new)
**Branch:** `claude/client-update-service-a1orrp`

## Goal

Re-implement the legacy client software-update distribution service in-repo as a
flat Gin service backed by MinIO. It uploads an update-artifact pair (a `.yaml`
descriptor + an executable) and serves artifacts back by file name. The legacy
implementation OOMs and leaks in production; this rewrite keeps the same HTTP
surface but fixes the root causes while retaining a **bounded, TTL-sized
download cache** for hot artifacts.

**Endpoints (unchanged surface):**
- `POST /api/v1/version` — upload a `configFile` + `executeFile` pair (multipart)
- `GET  /api/v1/version/:fileName` — download an artifact by name
- `GET  /api/v1/health` + `GET /healthz` — liveness

## The four legacy defects (and how each is fixed)

| # | Legacy bug | Root cause | Fix |
|---|-----------|-----------|-----|
| 1 | OOM under load | Whole file read into RAM (`io.ReadAll`) on upload **and** download | Upload streams `multipart.FileHeader` → `PutObject(reader, size)`. Download streams `GetObject` → `c.DataFromReader`. Only **cacheable-sized** objects are buffered, and only up to a hard per-object cap; larger artifacts always stream. |
| 2 | Goroutine leak | One unbounded, un-awaited goroutine per download populated the cache | No per-request goroutines. Cache population is synchronous; concurrent misses for the same key collapse via `singleflight` into **one** load. |
| 3 | Disk fills forever | Local disk cache with no eviction / TTL / size bound | In-memory `expirable.LRU` — bounded entry count **and** TTL. No disk. Total memory ≤ `maxEntries × maxObjectBytes`. |
| 4 | Empty/corrupt downloads | Cache read returned a silent nil buffer on miss | A miss is explicit: fall through to MinIO. A missing object is `404 NotFound`, never an empty `200`. Nothing is ever cached as nil. |

**Key insight:** the cache is not removed — it is re-implemented *correctly*. The
legacy cache was unbounded, disk-backed, async-populated, and nil-unsafe. The
new cache is size+TTL-bounded, in-memory, synchronously populated under
singleflight, and miss-explicit.

## Architecture

Flat `package main` service at repo root (CLAUDE.md §1), mirroring
`upload-service`. Reuses `pkg/minioutil` (streaming `ObjectStore`), `pkg/obs`,
`pkg/shutdown`, `pkg/errcode`+`errhttp`, and — for the cache —
`github.com/hashicorp/golang-lru/v2/expirable` + `golang.org/x/sync/singleflight`
(both already in `go.mod`; the exact idiom `media-service/cache.go` uses). **No
new third-party dependencies.**

```
client-update-service/
  main.go              # run(): config, obs.Init, minio connect + bucket check, cache build, gin wiring, shutdown.Wait
  config.go            # typed env config (caarlos0/env)
  handler.go           # Handler struct + NewHandler; HandleHealth
  version.go           # HandleUpload (POST) + HandleDownload (GET)
  cache.go             # blobCache: expirable.LRU[string, cachedBlob] + singleflight
  store.go             # versionStore interface + //go:generate mockgen
  store_minio.go       # streaming MinIO impl (Put, Stat, Open) + cancelReadCloser (ported from upload-service)
  routes.go            # RegisterRoutes(r, h)
  middleware.go        # request-ID + access-log middleware (ported from upload-service)
  handler_test.go      # health/wiring unit tests
  version_test.go      # upload/download handler unit tests (mocked store), table-driven
  cache_test.go        # cache hit/miss/TTL-expiry/eviction/singleflight-collapse tests
  integration_test.go  # //go:build integration — real MinIO round-trip via testutil.MinIO
  mock_store_test.go   # generated (mockgen), never hand-edited
  deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}
```

### Store interface (consumer-defined, streaming) — `store.go`

```go
//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

var ErrObjectNotFound = errors.New("object not found")

// blobInfo is the object metadata the download path needs for headers + the cache decision.
type blobInfo struct {
    Size        int64
    ContentType string
}

type versionStore interface {
    // Put streams r (of known size) to the object at key with the given content type.
    Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
    // Stat returns metadata only (HEAD), or ErrObjectNotFound (wrapped) when absent.
    Stat(ctx context.Context, key string) (blobInfo, error)
    // Open returns a streaming reader + metadata, or ErrObjectNotFound (wrapped) when absent.
    Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
}
```

`store_minio.go` implements it over `minioutil.ObjectStore`:
- `Put` → `PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{ContentType: ct})`.
- `Stat` → `StatObject` (HEAD); `NoSuchKey` → `ErrObjectNotFound`.
- `Open` → `GetObject` + `Stat()` probe, wrapped in the `cancelReadCloser`
  pattern from `upload-service/store_minio.go` so the download-timeout context is
  released on `Close()`; `NoSuchKey` → `ErrObjectNotFound`.

**Object key:** `chat.go/chat-versions/<fileName>` (fixed `objectPrefix`
constant, mirroring the legacy `defaultFilePath` root). Bucket from
`MINIO_BUCKET`, existence verified at startup via `minioutil.NewBucket`
(fail-fast; ops/IaC owns buckets — never created here).

### Download cache — `cache.go`

```go
type cachedBlob struct {
    body        []byte
    contentType string
}

// blobCache fronts version downloads with a size+TTL-bounded LRU and singleflight.
type blobCache struct {
    lru            *lru.LRU[string, cachedBlob]
    sf             singleflight.Group
    maxObjectBytes int64   // objects larger than this are never cached (streamed instead)
}

func newBlobCache(maxEntries int, ttl time.Duration, maxObjectBytes int64) *blobCache
func (c *blobCache) get(key string) (cachedBlob, bool)
func (c *blobCache) add(key string, b cachedBlob)
```

- `expirable.LRU` gives **both** bounds: `maxEntries` capacity (LRU eviction, no
  drop-all cliff) and `ttl` (per-entry expiry, reaped by the library — no
  per-request goroutine). `maxObjectBytes` caps a single cached object so one
  huge artifact can't blow the buffer.
- `maxEntries == 0` (or `ttl == 0`) constructs a disabled cache (`get` always
  misses, `add` is a no-op) so the cache can be turned off by config.

### Download flow (`HandleDownload`) — the changed path

1. Validate `fileName`: non-empty, path-clean, reject `/` and `..` (traversal).
2. `cache.get(key)` → **hit**: `c.Data(200, blob.contentType, blob.body)`; done.
3. **Miss** → `info, err := store.Stat(ctx, key)`:
   - `ErrObjectNotFound` → `404 errcode.NotFound("version not found")`.
   - other error → wrapped `%w` → `500 internal`.
4. **Cacheable** (`info.Size <= maxObjectBytes`): load under `singleflight.Do(key)`
   — re-check cache, `store.Open`, read exactly `info.Size` bytes into a buffer,
   `cache.add(key, blob)`, return blob. Serve `c.Data(200, ct, blob.body)`.
   Concurrent misses for the same key share the one load.
5. **Too large** (`info.Size > maxObjectBytes`): `rc, info, _ := store.Open(ctx, key)`;
   set headers; `c.DataFromReader(200, info.Size, ct, rc, nil)`; `defer rc.Close()`.
   Never cached; constant memory.

Response headers on both success paths: `Content-Type` (from `blobInfo`),
`Content-Length` (`info.Size`), `Content-Disposition: attachment; filename="<fileName>"`.

> Trade-off: the miss path does a `Stat` (HEAD) before `Open` (GET) to decide
> cacheable-vs-stream up front. Cache **hits** — the steady state the cache
> exists for — do neither. Accepted: one extra HEAD on a cold miss avoids
> buffering an oversized object into RAM and keeps the streaming decision clean.

### Upload flow (`HandleUpload`) — unchanged from prior design

- `Content-Type: multipart/form-data`; body bounded by `http.MaxBytesReader` at
  `MAX_UPLOAD_BYTES` before parsing (oversized rejected without buffering).
- Two required form files: `configFile` (`.yaml`/`.yml`, stored
  `application/x-yaml`) and `executeFile` (stored `application/octet-stream`).
  Each `*multipart.FileHeader` reader streamed to `Put` with `fileHeader.Size`.
- On successful overwrite of an existing name, evict that key from the cache so a
  stale copy isn't served within the TTL window.
- Success `200`: `{"result":"success"}`. Errors: `400` (bad multipart,
  missing/empty file, wrong config extension, body over cap) via
  `errcode.BadRequest`; infra → wrapped `%w` → `500`.

### Errors / logging / config / shutdown

- **Errors:** Tier-1 `pkg/errcode` constructors from handlers; `errhttp.Write`
  marshals the envelope. No bespoke error/response string maps.
- **Logging:** `log/slog` JSON only; o11y gin middleware + request-ID +
  access-log (method, path, status, latency, request-ID). Never log file bytes.
- **Config (`caarlos0/env`):**

  | Env | Default | Notes |
  |---|---|---|
  | `PORT` | `8080` | |
  | `SITE_ID` | *(required)* | |
  | `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | *(required)* | Secrets never defaulted |
  | `MINIO_USE_SSL` | `false` | |
  | `MINIO_BUCKET` | *(required)* | Fail-fast if bucket absent |
  | `MINIO_DOWNLOAD_TIMEOUT` | `5m` | Bounds Stat+stream |
  | `MAX_UPLOAD_BYTES` | `524288000` (500 MiB) | `MaxBytesReader` cap |
  | `HTTP_WRITE_TIMEOUT` | `10m` | Large executables stream slowly |
  | `CACHE_MAX_ENTRIES` | `64` | LRU capacity; `0` disables the cache |
  | `CACHE_TTL` | `5m` | Per-entry expiry |
  | `CACHE_MAX_OBJECT_BYTES` | `33554432` (32 MiB) | Per-object cacheable cap; larger streams uncached |

- **Shutdown:** `pkg/shutdown.Wait`; server graceful shutdown (no NATS/DB to
  drain; MinIO client is stateless). 25s budget < K8s 30s grace.

## Behavior / error handling summary

| Endpoint | Status | Condition |
|---|---|---|
| `POST /api/v1/version` | `200` | Both files stored. `{"result":"success"}` |
| | `400` | Bad multipart; missing/empty `configFile`/`executeFile`; config not `.yaml`/`.yml`; body over `MAX_UPLOAD_BYTES` |
| | `500` | MinIO failure |
| `GET /api/v1/version/:fileName` | `200` | Artifact bytes (from cache or MinIO) |
| | `400` | Empty or path-unsafe `fileName` |
| | `404` | No such artifact |
| | `500` | MinIO failure |
| `GET /api/v1/health`, `/healthz` | `200` | `{"status":"ok"}` |

## Testing (TDD, ≥80%; target ≥90% on `version.go`/`cache.go`/`store_minio.go`)

**Unit — `version_test.go` (mocked `versionStore`), table-driven:**
- Upload: valid pair → two `Put` calls with expected keys/sizes → `200`; missing
  `configFile`; missing `executeFile`; empty file; wrong config extension;
  malformed multipart; `Put` error → `500`; oversized body → `400`; overwrite
  evicts the cache key.
- Download: cache hit → served without any store call; miss+cacheable →
  `Stat`+`Open`, cached, served; miss+too-large → streamed, **not** cached;
  empty/`..`/`/` name → `400`; `ErrObjectNotFound` → `404`; store error → `500`.
- Health: `200 {"status":"ok"}`.

**Unit — `cache_test.go`:** hit; miss; TTL expiry (entry gone after `ttl`);
LRU eviction past `maxEntries`; `maxObjectBytes` refusal; singleflight collapses
N concurrent misses into 1 load (assert loader called once); disabled cache
(`maxEntries==0`) always misses.

**Integration — `integration_test.go` (`//go:build integration`):**
`TestMain` → `testutil.RunTests(m)`; `testutil.MinIO(t, "clientupdate")` real
round-trip: upload a config+exe pair, download each back, assert byte-identity +
content-type; missing object → `404`; second download of the same name served
from cache (store not re-hit — assert via a counting wrapper or timing-free hook).

## Docs (same PR)

Client-facing HTTP endpoints → update in the same PR (CLAUDE.md §5):
- `docs/client-api.md` — new `## 12. Client Update Service` (after §11
  tcard-service), modeled on the §7 Media Service format: per-endpoint
  auth/request/response tables, status matrix, a JSON success example for the
  upload, the cache behavior note, and the ⚠️ **unauthenticated — must be
  network-restricted** banner (matching media-service's PUT endpoints). Add to TOC.
- `docs/client-api/request-reply.md` — matching HTTP block in the `HTTP —`
  family: compact `Endpoint | Reply | Purpose` table, `**Emits:** None —
  HTTP-only.`, link to `../client-api.md#12-client-update-service`. Add to TOC.
- `docs/client-api/events.md` — **not** touched (no server→client events).

## Out of scope / follow-ups

- **Authentication.** The legacy service is unauthenticated; this matches it and
  documents the endpoints as network-restricted. Auth deferred to a follow-up.
  *If these artifacts are security-sensitive, this should be revisited before
  release.*
- Artifact listing / delete / GC endpoints; checksums/signatures; versioning &
  promotion; cross-site federation — all out of scope.

## Open decisions for review

1. **Health path** — legacy clients use `/api/v1/health`; CLAUDE.md §6 mandates
   `/healthz`. **Recommendation:** expose both (same handler). OK?
2. **Multipart field names** — keeping legacy `configFile` / `executeFile`.
   Confirm real clients use these exact names.
3. **Cache defaults** — `CACHE_MAX_ENTRIES=64`, `CACHE_TTL=5m`,
   `CACHE_MAX_OBJECT_BYTES=32 MiB`. Tune to real artifact sizes / update cadence.
4. **`MAX_UPLOAD_BYTES`** — 500 MiB placeholder; set to true max executable + headroom.
5. **Bucket** — dedicated `MINIO_BUCKET` for update artifacts assumed; sharing an
   existing bucket is an env-only change (prefix keeps them namespaced).
