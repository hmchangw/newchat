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

**Endpoints:**
- `POST /api/v1/version` — upload a `configFile` + `executeFile` pair (multipart)
- `GET  /api/v1/version/:fileName` — download an artifact by name
- `GET  /healthz` — liveness

## The four legacy defects (and how each is fixed)

| # | Legacy bug | Root cause | Fix |
|---|-----------|-----------|-----|
| 1 | OOM under load | Whole file read into RAM (`io.ReadAll`) on upload **and** download | Upload streams `multipart.FileHeader` → `PutObject(reader, size)`. Download streams `GetObject` → `c.DataFromReader`. Only **cacheable-sized** objects are buffered, up to a hard per-object cap; larger artifacts always stream. |
| 2 | Goroutine leak | One unbounded, un-awaited goroutine per download populated the cache | No per-request goroutines. Cache population is synchronous; concurrent misses for the same key collapse via `singleflight` into **one** load. |
| 3 | Disk fills forever | Local disk cache with no eviction / TTL / size bound | In-memory `expirable.LRU` — bounded entry count **and** TTL. No disk. Worst-case memory ≤ `CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES`. |
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
  main.go              # run(): config, obs.Init, minio connect + ensure-bucket, cache build, gin wiring, shutdown.Wait
  config.go            # typed env config (caarlos0/env)
  handler.go           # Handler struct + NewHandler; HandleHealth
  version.go           # HandleUpload (POST) + HandleDownload (GET)
  cache.go             # blobCache: expirable.LRU[string, cachedBlob] + singleflight
  store.go             # versionStore interface + //go:generate mockgen
  store_minio.go       # streaming MinIO impl (Put, Open) + ensureBucket + cancelReadCloser (ported from upload-service)
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
    // Open returns a streaming reader + metadata (size + content-type), or
    // ErrObjectNotFound (wrapped) when absent. Metadata comes from the opened
    // object's own Stat() — no separate HEAD round-trip.
    Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
}
```

> **Interface note.** `minioutil.ObjectStore` exposes only
> `BucketExists/PutObject/GetObject/ListObjects/RemoveObject` — **no `StatObject`**.
> So the store has no separate `Stat`/HEAD method: `Open` derives `blobInfo` from
> the opened object's `obj.Stat()` (which `GetObject` supports), and the cache
> size decision is made from that. This keeps the store on the shared interface
> with no type assertions.

`store_minio.go` implements it over `minioutil.ObjectStore`:
- `Put` → `PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{ContentType: ct})`.
- `Open` → `GetObject` + `obj.Stat()` probe (surfaces a missing object / dead
  backend before any body is read; also yields `Size` + `ContentType` for
  `blobInfo`), wrapped in the `cancelReadCloser` pattern from
  `upload-service/store_minio.go` so the download-timeout context is released on
  `Close()`; `NoSuchKey` → `ErrObjectNotFound`.

**Object key:** `chat.go/chat-versions/<fileName>` (fixed `objectPrefix`
constant, mirroring the legacy `defaultFilePath` root).

### Bucket bootstrap — create-if-absent (intentional deviation)

Most services here **fail-fast** on a missing bucket via `minioutil.NewBucket`
because ops/IaC owns buckets. This service instead **creates `MINIO_BUCKET` if it
does not exist**, matching the legacy service's behavior (a decided requirement).

`minioutil.ObjectStore` deliberately omits `MakeBucket`, but the concrete clients
`minioutil.Connect` returns (`*minio.Client` / `*o11yminio.Client`) both expose it
(the same call `pkg/testutil/minio.go` uses). So the ensure step is **service-local**
— no change to shared `pkg/minioutil` — via a narrow capability interface asserted
at startup:

```go
// bucketEnsurer is the create-side capability not surfaced by minioutil.ObjectStore.
// Both *minio.Client and *o11yminio.Client satisfy it.
type bucketEnsurer interface {
    BucketExists(ctx context.Context, name string) (bool, error)
    MakeBucket(ctx context.Context, name string, opts minio.MakeBucketOptions) error
}

// ensureBucket creates the bucket when absent; it is idempotent and race-safe
// (a concurrent create surfaces as BucketAlreadyOwnedByYou, treated as success).
func ensureBucket(ctx context.Context, client minioutil.ObjectStore, name string) error
```

Keeping this local (not in shared `pkg/minioutil`) confines the ops-owns-buckets
deviation to the one service that wants it.

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
func (c *blobCache) remove(key string)   // used on overwrite-upload to drop a stale copy

// loadCacheable collapses concurrent misses for key via singleflight. loader
// opens the object and returns (blob, cacheable): a cacheable blob is add()-ed
// and shared with all waiters; a non-cacheable result (object over maxObjectBytes)
// is returned but never stored, so its callers fall back to direct streaming.
func (c *blobCache) loadCacheable(key string, loader func() (cachedBlob, bool, error)) (cachedBlob, bool, error)
```

- `expirable.LRU` gives **both** bounds: `maxEntries` capacity (LRU eviction, no
  drop-all cliff) and `ttl` (per-entry expiry, reaped by the library — no
  per-request goroutine). `maxObjectBytes` caps a single cached object.
- `maxEntries == 0` (or `ttl == 0`) constructs a disabled cache (`get` always
  misses, `add`/`remove` no-op) so the cache can be turned off by config.

> **Memory note.** A cached entry holds the whole artifact in RAM, so worst-case
> cache footprint ≈ `CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES`. With the decided
> `CACHE_MAX_OBJECT_BYTES = 512 MiB`, the default `CACHE_MAX_ENTRIES` is set to
> **4** (≈ 2 GiB ceiling), not 64. Tune the two together for the deployment's
> memory budget; raising the object cap without lowering the entry count can OOM.

### Download flow (`HandleDownload`) — the changed path

Because there is no HEAD, the size is not known until the object is opened. The
flow therefore opens once under singleflight, decides cacheable-vs-stream from
the opened object's size, and only then buffers or streams:

1. Validate `fileName`: non-empty, path-clean, reject `/` and `..` (traversal).
2. `cache.get(key)` → **hit**: `c.Data(200, blob.contentType, blob.body)`; done.
3. **Miss** → `res, err := cache.loadCacheable(key, loader)` where `loader`:
   `rc, info, err := store.Open(ctx, key)` (`defer rc.Close()`); if
   `info.Size <= maxObjectBytes` read exactly `info.Size` bytes into a buffer and
   return a **cacheable** `cachedBlob`; else return a **non-cacheable** marker
   carrying only `info` (size + content-type), body discarded. `singleflight`
   collapses concurrent misses for the same key into one `Open`.
   - loader error `ErrObjectNotFound` → `404 errcode.NotFound("version not found")`;
     other error → wrapped `%w` → `500 internal`.
4. **Cacheable result:** `cache.add(key, blob)` (done inside `loadCacheable`) and
   serve `c.Data(200, blob.contentType, blob.body)`.
5. **Non-cacheable result** (`info.Size > maxObjectBytes`): re-open and stream —
   `rc, info, _ := store.Open(ctx, key)`; set headers;
   `c.DataFromReader(200, info.Size, info.ContentType, rc, nil)`; `defer rc.Close()`.
   Never cached; constant memory.

Response headers on both success paths: `Content-Type` (from `blobInfo`),
`Content-Length` (`info.Size`), `Content-Disposition: attachment; filename="<fileName>"`.

> Trade-off: a too-large object is opened twice on a cold miss — once in the
> singleflight loader to learn its size, once to stream. That path is rare (only
> objects over `CACHE_MAX_OBJECT_BYTES`) and the first open reads no body (closed
> after `Stat()`), so the cost is a second GET setup, not a double transfer.
> Cacheable objects — the common case — open exactly once; cache **hits** open
> zero times. Singleflight matters here: an update rollout is a thundering herd
> of clients hitting one brand-new version at the same moment.

### Upload flow (`HandleUpload`)

- `Content-Type: multipart/form-data`. **No upload size cap** — the legacy service
  imposed none, and streaming keeps memory bounded regardless of body size
  (Gin spills large multipart parts to temp files; each part reader is streamed
  straight to MinIO, never `ReadAll`-ed).
- Two required form files: `configFile` (`.yaml`/`.yml`) and `executeFile`. Each
  `*multipart.FileHeader` reader is streamed to `Put` with `fileHeader.Size`. The
  stored content type is taken from the part's `Content-Type` header, falling back
  to `application/x-yaml` (config) / `application/octet-stream` (executable) when
  the client sends none.
- On a successful overwrite of an existing name, `cache.remove(key)` so a stale
  copy isn't served within the TTL window.
- Success `200`: `{"result":"success"}`. Errors: `400` (bad multipart,
  missing/empty file, wrong config extension) via `errcode.BadRequest`; infra →
  wrapped `%w` → `500`.

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
  | `MINIO_BUCKET` | *(required)* | Created at startup if absent (see Bucket bootstrap) |
  | `MINIO_DOWNLOAD_TIMEOUT` | `5m` | Bounds a single Open (Stat probe + streamed body) |
  | `HTTP_WRITE_TIMEOUT` | `10m` | Large executables stream slowly |
  | `CACHE_MAX_ENTRIES` | `4` | LRU capacity; `0` disables the cache |
  | `CACHE_TTL` | `24h` | Per-entry expiry |
  | `CACHE_MAX_OBJECT_BYTES` | `536870912` (512 MiB) | Per-object cacheable cap; larger streams uncached |

  No `MAX_UPLOAD_BYTES` — uploads are uncapped (matches legacy).

- **Shutdown:** `pkg/shutdown.Wait`; server graceful shutdown (no NATS/DB to
  drain; MinIO client is stateless). 25s budget < K8s 30s grace.

## Behavior / error handling summary

| Endpoint | Status | Condition |
|---|---|---|
| `POST /api/v1/version` | `200` | Both files stored. `{"result":"success"}` |
| | `400` | Bad multipart; missing/empty `configFile`/`executeFile`; config not `.yaml`/`.yml` |
| | `500` | MinIO failure |
| `GET /api/v1/version/:fileName` | `200` | Artifact bytes (from cache or MinIO) |
| | `400` | Empty or path-unsafe `fileName` |
| | `404` | No such artifact |
| | `500` | MinIO failure |
| `GET /healthz` | `200` | `{"status":"ok"}` |

## Testing (TDD, ≥80%; target ≥90% on `version.go`/`cache.go`/`store_minio.go`)

**Unit — `version_test.go` (mocked `versionStore`), table-driven:**
- Upload: valid pair → two `Put` calls with expected keys/sizes → `200`; missing
  `configFile`; missing `executeFile`; empty file; wrong config extension;
  malformed multipart; `Put` error → `500`; overwrite evicts the cache key.
- Download: cache hit → served without any store call; miss+cacheable →
  one `Open`, cached, served (second request is a cache hit, no store call);
  miss+too-large → opened, streamed, **not** cached; empty/`..`/`/` name → `400`;
  `ErrObjectNotFound` → `404`; store error → `500`.
- Health: `200 {"status":"ok"}`.

**Unit — `cache_test.go`:** hit; miss; TTL expiry (entry gone after `ttl`);
LRU eviction past `maxEntries`; `add` refuses a body over `maxObjectBytes`;
`remove` drops an entry; `loadCacheable` caches a cacheable result and shares it,
returns (but does not store) a non-cacheable result; singleflight collapses N
concurrent misses into 1 loader call; disabled cache (`maxEntries==0`) always misses.

**Integration — `integration_test.go` (`//go:build integration`):**
`TestMain` → `testutil.RunTests(m)`; `testutil.MinIO(t, "clientupdate")` real
round-trip: upload a config+exe pair, download each back, assert byte-identity +
content-type; missing object → `404`; second download of the same name served
from cache (store not re-hit — assert via a counting store wrapper). Also cover
`ensureBucket` creating an absent bucket.

## Docs (same PR)

Client-facing HTTP endpoints → update in the same PR (CLAUDE.md §5):
- `docs/client-api.md` — new `## 12. Client Update Service` (after §11
  tcard-service), modeled on the §7 Media Service format: per-endpoint
  request/response tables, status matrix, a JSON success example for the upload,
  the cache behavior note, and the ⚠️ **unauthenticated — must be
  network-restricted** banner (matching media-service's PUT endpoints). Add to TOC.
- `docs/client-api/request-reply.md` — matching HTTP block in the `HTTP —`
  family: compact `Endpoint | Reply | Purpose` table, `**Emits:** None —
  HTTP-only.`, link to `../client-api.md#12-client-update-service`. Add to TOC.
- `docs/client-api/events.md` — **not** touched (no server→client events).

## Resolved decisions

1. **Health path** — `/healthz` only, per CLAUDE.md §6. (Legacy `/api/v1/health` dropped.)
2. **Multipart field names** — keep legacy `configFile` / `executeFile`.
3. **Cache defaults** — `CACHE_TTL = 24h`, `CACHE_MAX_OBJECT_BYTES = 512 MiB`;
   `CACHE_MAX_ENTRIES = 4` so worst-case footprint stays ≈ 2 GiB (see Memory note).
4. **Upload cap** — none; `MAX_UPLOAD_BYTES` removed (matches legacy).
5. **Bucket** — `MINIO_BUCKET` names the bucket; created at startup if absent.

## Out of scope / follow-ups

- **Authentication.** The legacy service is unauthenticated; this matches it and
  documents the endpoints as network-restricted. Auth deferred to a follow-up.
  *If these artifacts are security-sensitive, revisit before release.*
- Artifact listing / delete / GC endpoints; checksums/signatures; versioning &
  promotion; cross-site federation — all out of scope.
