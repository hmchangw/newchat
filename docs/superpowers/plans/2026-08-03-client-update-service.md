# Client Update Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new flat Gin service `client-update-service` that uploads a client update-artifact pair (`configFile` + `executeFile`) to MinIO and serves artifacts back by file name, streaming end-to-end with a bounded TTL+size download cache. Replaces a legacy service that OOMs and leaks (in-memory buffering, leaked cache goroutines, unbounded disk cache, silent nil-on-miss).

**Spec:** `docs/superpowers/specs/2026-08-03-client-update-service-design.md` (authoritative — read it first).

**Architecture:** Streaming upload (`multipart.FileHeader` → `PutObject(reader, size)`) and streaming download (`GetObject` → `c.DataFromReader`), fronted by an `expirable.LRU` + `singleflight` cache (`media-service/cache.go` idiom). Cacheable objects (≤ `CACHE_MAX_OBJECT_BYTES`) are buffered and served from RAM; larger ones stream uncached. Store interface is consumer-defined; MinIO impl reuses the `cancelReadCloser` pattern from `upload-service/store_minio.go`. The bucket is created at startup if absent (service-local `ensureBucket`).

**Tech Stack:** Go 1.25, Gin, `minio-go/v7`, `caarlos0/env/v11`, `hashicorp/golang-lru/v2/expirable`, `golang.org/x/sync/singleflight`, `stretchr/testify`, `go.uber.org/mock`, `pkg/{minioutil,obs,shutdown,errcode}`.

## Global Constraints

- Go 1.25; use `make` targets, never raw `go` (`make test SERVICE=client-update-service`, `make generate SERVICE=client-update-service`, `make lint`, `make sast`, `make test-integration SERVICE=client-update-service`).
- All config via `caarlos0/env` typed struct — no `os.Getenv`. `SCREAMING_SNAKE_CASE` names; `envDefault` for non-secrets; secrets/`MINIO_ENDPOINT`/`MINIO_BUCKET`/`SITE_ID` `required`, never defaulted.
- Client-facing errors via `pkg/errcode` (Tier-1 constructors) + `errhttp.Write`; infra failures via `fmt.Errorf("...: %w", err)` (collapse to `internal`). No bespoke error/response string maps.
- Logging: `log/slog` JSON only; never log file bytes/paths-as-secrets. Request-ID + access-log middleware ported from `upload-service/middleware.go`.
- TDD: no production code before a failing test (Red → Green → Refactor → Commit). `-race` always (Makefile handles it).
- Minimum 80% coverage; target ≥90% on `version.go`, `cache.go`, `store_minio.go`. **No new third-party dependencies.**
- Streaming discipline: never `io.ReadAll` a request body or object body except the bounded cache buffer (exactly `info.Size` bytes, gated by `≤ CACHE_MAX_OBJECT_BYTES`).
- Generated mocks in `mock_store_test.go` (never hand-edit); run `make generate SERVICE=client-update-service` after any `store.go` change.
- New HTTP service → `deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`, `GET /healthz`, Gin server + Resty (none needed) timeouts, graceful shutdown via `pkg/shutdown.Wait`.
- Client-facing HTTP endpoints → update `docs/client-api.md` + `docs/client-api/request-reply.md` in the same PR.

## File Structure

```
client-update-service/
  main.go              # run(): config, obs.Init, minio connect + ensureBucket, cache, gin, shutdown.Wait
  config.go            # config struct (caarlos0/env)
  handler.go           # Handler struct + NewHandler + HandleHealth
  version.go           # HandleUpload + HandleDownload + objectKey/validation helpers
  cache.go             # blobCache (expirable.LRU + singleflight)
  store.go             # versionStore interface, blobInfo, ErrObjectNotFound, //go:generate
  store_minio.go       # minioVersionStore (Put/Stat/Open) + ensureBucket + cancelReadCloser
  routes.go            # RegisterRoutes
  middleware.go        # requestID + access-log (ported)
  cache_test.go        # unit
  handler_test.go      # unit (health/wiring)
  version_test.go      # unit (upload/download, mocked store+cache)
  integration_test.go  # //go:build integration
  mock_store_test.go   # generated
  deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}
docs/client-api.md                 # new §12
docs/client-api/request-reply.md   # matching HTTP block
```

Reference files to mirror (read before writing): `upload-service/{main.go,middleware.go,store_minio.go,routes.go,deploy/*}`, `media-service/cache.go`, `pkg/minioutil/minio.go`, `pkg/testutil/minio.go`.

---

### Task 1: `blobCache` — bounded TTL+size download cache

**Files:** Create `client-update-service/cache.go`, `client-update-service/cache_test.go`.

**Interfaces:**
- Produces: `type cachedBlob struct { body []byte; contentType string }`; `type blobCache struct{...}`; `newBlobCache(maxEntries int, ttl time.Duration, maxObjectBytes int64) *blobCache`; `(*blobCache).get(key string) (cachedBlob, bool)`; `(*blobCache).add(key string, b cachedBlob)`; `(*blobCache).remove(key string)`; `(*blobCache).loadOrStore(key string, size int64, load func() (cachedBlob, error)) (cachedBlob, error)` — singleflight wrapper that refuses to cache when `size > maxObjectBytes` (still returns the loaded blob).
- Consumes: `lru "github.com/hashicorp/golang-lru/v2/expirable"`, `golang.org/x/sync/singleflight`.

- [ ] **Step 1 (Red):** Write `cache_test.go`:
  - `get` miss on empty; `add` then `get` hit; `remove` drops entry.
  - TTL expiry: construct with a short ttl, `add`, assert `get` miss after expiry (use the LRU's own ttl; drive time via a short real ttl + `assert.Eventually`, no `time.Sleep` for sync — poll).
  - LRU eviction: `maxEntries=2`, add 3 keys, oldest evicted.
  - `maxObjectBytes` refusal: `loadOrStore` with `size > cap` returns the blob but a subsequent `get` misses.
  - Singleflight collapse: N concurrent `loadOrStore` on the same key with a loader that counts calls + blocks on a channel; assert loader invoked exactly once, all callers get the value (use `sync.WaitGroup`, no sleeps).
  - Disabled cache: `newBlobCache(0, ttl, cap)` → `get` always miss, `add`/`remove` no-op, `loadOrStore` still loads.
  Run `make test SERVICE=client-update-service` → confirm FAIL (undefined).

- [ ] **Step 2 (Green):** Implement `cache.go`. `newBlobCache` returns a disabled cache (nil lru) when `maxEntries<=0 || ttl<=0`. `loadOrStore` uses `sf.Do(key, ...)`: re-check `get`, call `load`, and `add` only when `int64(len(blob.body)) <= maxObjectBytes && lru != nil`. `get`/`add`/`remove` nil-guard the lru.

- [ ] **Step 3 (Refactor + Commit):** `make lint`; commit `feat(client-update-service): bounded TTL+size download cache`.

---

### Task 2: Store interface + MinIO streaming impl + ensureBucket

**Files:** Create `store.go`, `store_minio.go`; generate `mock_store_test.go`. Integration coverage lands in Task 5.

**Interfaces:**
- Produces: `blobInfo{Size int64; ContentType string}`; `ErrObjectNotFound`; `versionStore` (Put/Stat/Open per spec); `minioVersionStore` implementing it over `minioutil.ObjectStore`; `newMinioVersionStore(client minioutil.ObjectStore, bucket string, downloadTimeout time.Duration) *minioVersionStore`; `ensureBucket(ctx, client minioutil.ObjectStore, name string) error`; `bucketEnsurer` capability interface; `cancelReadCloser`.
- Consumes: `minioutil.ObjectStore`, `minio.PutObjectOptions/GetObjectOptions/StatObjectOptions/MakeBucketOptions`, `minio.ToErrorResponse`.

- [ ] **Step 1 (Red):** Write the `store.go` interface + `//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main`. Run `make generate SERVICE=client-update-service`. Add a small `store_minio_test.go` unit for `ensureBucket` against a hand-rolled fake `bucketEnsurer` (exists→no MakeBucket; absent→MakeBucket called; `BucketAlreadyOwnedByYou`/`BucketAlreadyExists` from a racing create→treated success; other MakeBucket error→wrapped). Confirm FAIL.
  - Also unit-test the `NoSuchKey → ErrObjectNotFound` mapping helper (`isNotFound(err)`), table-driven.

- [ ] **Step 2 (Green):** Implement `store_minio.go`:
  - `Put` → `PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{ContentType: ct})`, wrap errors.
  - `Stat` → `StatObject`; map `NoSuchKey` → `ErrObjectNotFound`; return `blobInfo{Size, ContentType}`.
  - `Open` → timeout ctx + `GetObject` + `obj.Stat()` probe (surfaces missing/dead backend before body), map `NoSuchKey` → `ErrObjectNotFound`, wrap in `cancelReadCloser` (ported verbatim from `upload-service/store_minio.go`).
  - `ensureBucket`: `BucketExists`; if absent, type-assert client to `bucketEnsurer` and `MakeBucket`; treat already-owned/exists as success; wrap everything with `%w`.

- [ ] **Step 3 (Refactor + Commit):** `make lint`; commit `feat(client-update-service): streaming MinIO store + ensure-bucket`.

---

### Task 3: Handlers — upload, download, health, routes, middleware

**Files:** Create `handler.go`, `version.go`, `routes.go`, `middleware.go`; tests `handler_test.go`, `version_test.go`. Uses `mock_store_test.go` (Task 2).

**Interfaces:**
- Produces: `Handler{store versionStore; cache *blobCache; log *slog.Logger}`; `NewHandler(store versionStore, cache *blobCache) *Handler`; `(*Handler).HandleUpload`, `HandleDownload`, `HandleHealth` (Gin handlers); `RegisterRoutes(r *gin.Engine, h *Handler)`; `objectKey(fileName string) string`; `validFileName(name string) (string, bool)`; `requestIDMiddleware`, `accessLogMiddleware` (ported).
- Consumes: `versionStore` mock, `blobCache`, `errcode`, `errhttp`, `gin`.

- [ ] **Step 1 (Red):** Write `version_test.go` (table-driven, `gomock` store + real `blobCache` via `newBlobCache`, `httptest` + `gin.CreateTestContext`):
  - **Upload:** valid `configFile`(.yaml)+`executeFile` → `Put` called twice with keys `chat.go/chat-versions/<name>` and matching sizes → `200 {"result":"success"}`; missing `configFile` → `400`; missing `executeFile` → `400`; empty file (0 bytes / no filename) → `400`; config named `.txt` → `400`; malformed multipart body → `400`; store `Put` error → `500`; overwrite of a cached name calls `cache.remove` (assert a prior `get` hit becomes a miss).
  - **Download:** cache hit → `200`, **zero** store calls (assert via `EXPECT().Times(0)`); miss + cacheable → `Stat` then `Open`, bytes served, second call is a cache hit; miss + too-large (`Size > maxObjectBytes`) → `Open` streamed, `200`, **not** cached; `Stat` returns `ErrObjectNotFound` → `404`; empty name → `400`; `..`/`/` in name → `400`; `Stat` infra error → `500`; assert `Content-Type`/`Content-Length`/`Content-Disposition` headers.
  - **Health:** `GET /healthz` → `200 {"status":"ok"}`.
  Run → confirm FAIL.

- [ ] **Step 2 (Green):** Implement `handler.go` (struct, `NewHandler`, `HandleHealth`), `version.go` (upload/download + helpers), `routes.go`, `middleware.go`:
  - Upload: `c.Request.MultipartReader`-free path — use `c.FormFile("configFile")`/`c.FormFile("executeFile")` (Gin streams parts to temp files; `FileHeader.Open()` gives a streaming reader). Validate presence, non-zero `Size`, config extension `.yaml`/`.yml`. Stream each to `store.Put`. On overwrite, `cache.remove(objectKey(name))` for both keys.
  - Download: `validFileName` (reject empty, `strings.Contains(name, "/")`, `..`, and `filepath.Clean` mismatch). `cache.get` → serve. Miss → `store.Stat`; `errors.Is(err, ErrObjectNotFound)` → `errcode.NotFound`. Cacheable → `cache.loadOrStore(key, size, load)` where `load` does `store.Open`+read exactly `size` bytes; serve `c.Data`. Too-large → `store.Open` + `c.DataFromReader`.
  - `RegisterRoutes`: `/healthz`; group `/api/v1` → `POST /version`, `GET /version/:fileName`; attach middleware.

- [ ] **Step 3 (Refactor + Commit):** `make lint`; commit `feat(client-update-service): upload/download/health handlers`.

---

### Task 4: Wiring — `main.go`, `config.go`, deploy

**Files:** Create `main.go`, `config.go`, `deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`.

**Interfaces:**
- Produces: `config` struct (all env from spec table), `run() error`, `main()`.
- Consumes: `env.Parse`, `obs.Init`, `minioutil.Connect(+WithObservability)`, `ensureBucket`, `newMinioVersionStore`, `newBlobCache`, `NewHandler`, `RegisterRoutes`, `shutdown.Wait`.

- [ ] **Step 1 (Red):** Add a `config_test.go` (or `main_test.go`) asserting defaults parse (`CACHE_TTL=6h`, `CACHE_MAX_ENTRIES=4`, `CACHE_MAX_OBJECT_BYTES=536870912`, `PORT=8080`, `MINIO_DOWNLOAD_TIMEOUT=5m`, `HTTP_WRITE_TIMEOUT=10m`) and that missing `MINIO_BUCKET`/`SITE_ID`/`MINIO_*` secrets error. Confirm FAIL.

- [ ] **Step 2 (Green):** Implement `config.go` + `main.go` mirroring `upload-service/main.go`: parse config → `obs.Init` → `minioutil.Connect(..., minioutil.WithObservability(sdk))` → `ensureBucket(ctx, client, cfg.MinioBucket)` → build store, cache, handler → gin engine with middleware + `RegisterRoutes` → `http.Server{WriteTimeout: cfg.HTTPWriteTimeout, ...}` → `shutdown.Wait` (server graceful shutdown; MinIO stateless). Fail-fast + `os.Exit(1)` on any startup error.

- [ ] **Step 3:** Deploy files mirroring `upload-service/deploy/`: multistage `golang:1.25.12-alpine`→`alpine:3.21` Dockerfile (context = repo root); `docker-compose.yml` with a `minio` dependency + all env (incl. `MINIO_BUCKET`, cache vars, `BOOTSTRAP` n/a — no JetStream); `azure-pipelines.yml` copied and renamed. `make build SERVICE=client-update-service` succeeds.

- [ ] **Step 4 (Commit):** `make lint`; commit `feat(client-update-service): main wiring + deploy`.

---

### Task 5: Integration test (real MinIO)

**Files:** Create `integration_test.go` (`//go:build integration`).

- [ ] **Step 1 (Red → Green):** `TestMain(m)` → `testutil.RunTests(m)`. Use `testutil.MinIO(t, "clientupdate")` for a real client+bucket. Build `newMinioVersionStore` over it. Cases:
  - Round-trip: `Put` a small yaml + a small "exe" blob → `Open`/`Stat` each back → assert byte-identity + content-type.
  - `Stat`/`Open` on an absent key → `ErrObjectNotFound`.
  - `ensureBucket` on a fresh random bucket name → bucket exists afterward (`BucketExists` true); idempotent on second call.
  - Handler-level: wire `Handler` (real store + real cache), drive `httptest` upload then two downloads; assert the second download does **not** re-hit MinIO (wrap the store in a call-counting decorator).
  Run `make test-integration SERVICE=client-update-service`.

- [ ] **Step 2 (Commit):** commit `test(client-update-service): MinIO integration round-trip`.

---

### Task 6: Client API docs

**Files:** Modify `docs/client-api.md`, `docs/client-api/request-reply.md`.

- [ ] **Step 1:** `docs/client-api.md` — add `## 12. Client Update Service` after §11 (tcard-service), modeled on §7 Media Service:
  - Intro + ⚠️ **unauthenticated, must be network-restricted** banner (mirror the media-service PUT banner).
  - `POST /api/v1/version`: request (multipart `configFile` .yaml/.yml + `executeFile`), response table (`200`/`400`/`500`), JSON success example `{"result":"success"}`.
  - `GET /api/v1/version/:fileName`: response matrix (`200` bytes / `400` / `404` / `500`), streaming + `Content-Disposition` note, and the TTL cache behavior note (served from cache within `CACHE_TTL`; overwrite busts it).
  - `GET /healthz`.
  - Add the section to the Table of contents (line ~38–86 region).

- [ ] **Step 2:** `docs/client-api/request-reply.md` — add an `HTTP — Client Update Service` block near the other `HTTP —` entries: compact `Endpoint | Reply | Purpose` table for the three routes, `**Emits:** None — HTTP-only.`, and a link to `../client-api.md#12-client-update-service`. Add to its TOC. (`events.md` untouched — no events.)

- [ ] **Step 3 (Commit):** commit `docs(client-api): client-update-service endpoints`.

---

### Task 7: Final verification & push

- [ ] `make generate SERVICE=client-update-service` (mocks current), `make lint`, `make test SERVICE=client-update-service` (coverage ≥80%, verify via coverprofile), `make test-integration SERVICE=client-update-service`, `make sast`.
- [ ] Confirm no `docs/reviews/` artifacts to remove (none created).
- [ ] Push `claude/client-update-service-a1orrp` (`git push -u origin`, retry-with-backoff on network error). Do **not** open a PR unless asked.

## Notes / risks

- **Bucket auto-create** deviates from the repo's ops-owns-buckets norm — intentional, spec-documented, confined to this service's `ensureBucket`.
- **Cache memory:** worst case ≈ `CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES` (default ≈ 2 GiB). Deploy env must budget for it; raising the object cap without lowering entries risks OOM.
- **No auth** — matches legacy; flagged for a security follow-up before production exposure.
- **Content-type on upload** is assigned by field (`application/x-yaml` / `application/octet-stream`), not sniffed — deterministic and sufficient for these two artifact types.
