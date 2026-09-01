# client-update-service — Production Readiness Review

**Service:** `client-update-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

A small, unusually well-crafted service. The upload path never buffers the artifact — parts stream from the multipart header straight into `Put`, and a peak-heap assertion proves it at a 48 MiB artifact under a 24 MiB ceiling. Concurrent cache misses collapse through `singleflight` with a generation counter that stops an upload-racing fill from reviving stale bytes. Credentials are compared in constant time with no early break, and every discarded error carries a justification comment.

One defect undercuts all of it. **`ReadTimeout` is hardcoded to 30 seconds**, and `ReadTimeout` bounds reading the *entire request including the body* — so with `UPLOAD_MAX_BYTES` at 2 GiB, a full-size artifact needs a sustained ~570 Mbps to finish in time, and a 200 MiB one needs ~57 Mbps. `HTTP_WRITE_TIMEOUT` (10m) does not help; it governs the response. This silently breaks `admin-service`'s documented 10-minute relay budget, and the sibling `upload-service` gets it right *and says why*. It is invisible to tests because it lives in `run()`, which is 0% covered — the same 50 statements that account for nearly the whole coverage shortfall.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 5 | 13 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-wrapped, secret-safe code that follows CLAUDE.md Section 3 closely; the only real defects are a non-`errors.Is` sentinel comparison and a missing response hardening header.

### Findings
- `low` — `err != http.ErrServerClosed` compares a sentinel by identity instead of `errors.Is` — `client-update-service/main.go:101`
  It works only because `ListenAndServe` returns the sentinel unwrapped; any future wrapping silently turns a clean shutdown into a fatal error.
- `low` — download responses echo an uploader-supplied `Content-Type` (`fh.Header.Get("Content-Type")`, `version.go:105`) back to unauthenticated GETs with no `X-Content-Type-Options: nosniff` — `client-update-service/version.go:199-203`
  `Content-Disposition: attachment` is the only mitigation; a `text/html` part header stored today is served from this origin tomorrow.
- `low` — audit-coverage gap, not a service defect: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress) — `GLOBAL_PREP.md:8-9`
- `nitpick` — `errhttp.Write(ctx, c, fmt.Errorf(...))` at `version.go:152` and `:188` is the correct Tier-1 pattern for infra errors, and nothing in the service logs-and-returns; noted as verified, not as a defect.

Confirmed clean: `errcode` Tier-1 constructors only (`BadRequest`/`NotFound`/`Unauthenticated`), no `WithCause` over an `*errcode.Error`, no `WithReason` where the frontend does not branch; `log/slog` structured k/v only, no `fmt.Println`; token never reaches an error string or a log line (`config.go:113`, asserted by `config_test.go:103` and `middleware_test.go:157`); constant-time token compare with no early break (`middleware.go:36-47`); every discarded error carries a justification comment (`version.go:102-104`, `store_minio.go:45`).

### Recommendations
- `low` — replace `err != http.ErrServerClosed` with `errors.Is(err, http.ErrServerClosed)` (`main.go:101`).
- `low` — set `X-Content-Type-Options: nosniff` in `serveBytes` and `streamObject`, or allow-list the stored content type to the two the upload path already fallbacks to.
- `low` — record the `govulncheck`/registry-pack gap in the PR so the CI `sast` gate is understood to have run partially in this environment.

---

---

## 3. Architecture — 4 / 5

Textbook consumer-defined interface, constructor DI and file layout; the one substantive breach is re-declaring MinIO connection knobs that two sibling services also declare, with no owning package config.

### Findings
- `medium` — MinIO knobs are re-declared per service instead of being declared once in the owning package: `MINIO_ENDPOINT`/`ACCESS_KEY`/`SECRET_KEY`/`BUCKET` at `client-update-service/config.go:19-23`, again at `upload-service/main.go:95-99`, again at `media-service/config.go:70-74`; `MINIO_DOWNLOAD_TIMEOUT` at `client-update-service/config.go:24` and `upload-service/main.go:101`. `pkg/minioutil` exposes no `Config` type (`ls pkg/minioutil` — only `minio.go`/`observability.go`).
  This is exactly the CLAUDE.md §6 shared-knob rule ("declared once, in the package that owns the thing it configures, mounted as a named field"). The drift is already visible: `MINIO_BUCKET` is `required` here, `envDefault:"avatars"` in media-service, and defaulted-to-empty in upload-service.
- `low` — request-handling logic lives in `version.go`, while `handler.go` holds only the struct and the health probe — the per-service layout in CLAUDE.md §1 puts handling logic in `handler.go` — `client-update-service/version.go:43`, `handler.go:22`
- `low` — on an early `ListenAndServe` failure `run` returns at `main.go:102` without `<-shutdownDone`, leaving the `shutdown.Wait` goroutine parked; harmless only because `main` then calls `os.Exit(1)` — `client-update-service/main.go:90-103`

Verified correct: `versionStore` defined in the consumer with exactly the two methods used (`store.go:22-28`); `bucketClient` likewise narrowed at its use site (`store_minio.go:62`); `NewHandler` accepts interfaces and returns a struct (`handler.go:17`); `caarlos0/env` typed struct with `required` on every secret/connection string and `envDefault` on every non-critical knob, no `os.Getenv` anywhere; `pkg/shutdown.Wait` with a 25s budget under the 30s grace period (`main.go:93`); no JetStream surface, so `bootstrap.go`/`BOOTSTRAP_STREAMS` is correctly absent; exported `Handler`/`NewHandler` matches the repo-majority convention (16 services export it).

### Recommendations
- `medium` — add `minioutil.Config` (endpoint, keys, SSL, download timeout) and mount it as `Minio minioutil.Config` in all three services; keep `MINIO_BUCKET` service-local since the buckets genuinely differ.
- `low` — either rename `version.go` → `handler.go` (folding the current `handler.go` in), or note the split in the service's own doc as a deliberate deviation.
- `low` — close over `shutdownDone` on every `run` return path (`defer func(){ <-shutdownDone }()` after the goroutine starts).

---

---

## 4. Test coverage — 2 / 5

76.8% is below the CLAUDE.md §4 80% floor, which floors this dimension at 2 — but the shortfall is almost entirely the untested `main.go` wiring, and the tested surface is genuinely excellent.

### Findings
- `high` — coverage is **76.8% (276 statements)**, under the CLAUDE.md §4 80% floor — `coverage_by_service.txt`
- `high` — `run()` is 0% covered and accounts for **50 of the ~64 uncovered statements** (every statement in `main.go`) — `client-update-service/main.go:27-106`
  Excluding `main.go`, the package is ~93.8% covered. This is not a testing-effort problem; it is that server construction is inlined into `run` and therefore unassertable — and the `ReadTimeout` defect reported under D6 lives in exactly that block.
- `medium` — `store_minio.go` `Open` is 38.5% and `Close` is 0% (11 uncovered statements): the `Stat`→`ErrObjectNotFound` mapping (`store_minio.go:47-49`), the generic `Stat` failure (`:50`), and the `cancelReadCloser.Close` cancel contract (`:93-97`) are reached only under `//go:build integration`. Nothing asserts that `cancel()` actually runs on `Close`, so a leaked download timeout context would go unnoticed in `make test`.
- `low` — `storeFormFile`'s `fh.Open()` error branch (`version.go:99-101`) and `HandleUpload`'s residual 6.2% are unreachable through `net/http`'s multipart reader.
- `low` — `TestHandleUpload_DoesNotBufferTheArtifactInMemory` asserts a hard 24 MiB ceiling on a process-wide `runtime.MemStats` sample taken every 10 ms (`version_test.go:410-422`, `pkg/testutil/memtest.go:71-91`); it is a valuable test but a load-sensitive one.

Quality is otherwise strong: table-driven subtests with descriptive names (`config_test.go:92`, `middleware_test.go:83`); `t.Setenv` throughout so no test mutates shared env (`config_test.go:19-58`); no `t.Parallel` and no shared mutable state; mocks generated by `go.uber.org/mock` into `mock_store_test.go` (repo-wide `make generate` produced zero diff); no real MinIO in unit tests; integration file carries the tag and `TestMain(m) { testutil.RunTests(m) }` (`integration_test.go:1,22`) with containers from `testutil.MinIO` and a `t.Cleanup` `RemoveBucket` for the one extra bucket (`:61-75`). Error paths that matter *are* covered: 404, 500, oversize-body, malformed multipart, empty file, wrong extension, equal names, traversal names, cache-invalidation-during-fill, singleflight collapse.

### Recommendations
- `high` — extract `newServer(cfg, handler) *http.Server` out of `run()` and unit-test the timeout matrix; that single refactor lifts the package over 80% *and* is the test that would have caught the `ReadTimeout` defect.
- `medium` — unit-test `cancelReadCloser.Close` (assert the returned `context.Context` is done after `Close`) and the `Stat` not-found mapping via a fake, so `make test` covers the contract rather than only `make test-integration`.
- `medium` — assert `run()`'s `bucketClient` type-assertion failure path (`main.go:55-58`) — today a minioutil change that stops satisfying `bucketClient` fails only at runtime startup.
- `low` — soften the peak-heap assertion to a ratio of the artifact size rather than a fixed 24 MiB, to keep it meaningful without being load-fragile.

---

---

## 5. Maintainability — 4 / 5

Small, single-purpose files with genuinely WHY-shaped comments and no dead code; one comment has gone stale and one cross-service invariant survives only as prose.

### Findings
- `low` — stale comment: "Only `CacheMaxObjectBytes` is an int64 here" is no longer true — `UploadMaxBytes` is also `int64` and therefore also routed through `parseByteSize` — `client-update-service/config.go:57-59` vs `config.go:47`
- `low` — the upload/download deadline invariant is documented in *prose in another service* ("client-update-service's own `HTTP_WRITE_TIMEOUT` should be at least this value", `admin-service/config.go:78-80`) with nothing on either side enforcing it; `admin-service` has a `checkHandlerTimeout` validator for its own knobs (`admin-service/config.go:124`) but none spans the pair.
- `nitpick` — the oversize-object path opens the object twice (`loadObject` then `streamObject`, `version.go:159` → `:181`); intentional and commented, but a `loadOrStream` that returns the still-open reader would remove the second `Stat` round trip and one code path.

Positives worth recording: largest file is 207 lines (`version.go`); largest function is `run` at ~50 statements and is pure wiring; every non-obvious decision carries a WHY comment that a reviewer can check (`config.go:26-37` on the `,`/`:` separator behaviour of caarlos0/env v11.4.0; `routes.go:20-24` on middleware ordering; `cache.go:78-83` on the generation counter; `config.go:70-75` on `MaxMultipartMemory`). No duplicated logic, no dead code (`bytesBufferString`, `rc`, `testTokens`, `enabled`, `maxInt64AsFloat` all have live callers).

### Recommendations
- `low` — fix the `config.go:57-59` comment to name both int64 fields.
- `low` — add a startup check that `HTTP_WRITE_TIMEOUT` (and the read timeout, once configurable) is at least the artifact-transfer budget, so the admin-service constraint is enforced rather than described.
- `nitpick` — consider collapsing `loadObject`/`streamObject` into one function returning `(cachedBlob, io.ReadCloser, blobInfo, error)` to drop the double `Stat`.

---

---

## 6. Integration — 3 / 5

The service has no NATS, JetStream, Mongo or Cassandra surface, so most of the integration checklist is inapplicable; its one real cross-service contract — the `admin-service` upload relay — is broken by an unenforced timeout assumption.

### Findings
- `high` — the contract with `admin-service` is violated at the read side: `admin-service` budgets a full artifact upload at `CLIENT_UPDATE_UPLOAD_TIMEOUT` (default **10m**, `admin-service/config.go:81`) and this service's `HTTP_WRITE_TIMEOUT` is correspondingly 10m (`config.go:40`) — but the *upload* is the read direction, and `ReadTimeout` is hardcoded to **30s** (`client-update-service/main.go:80`). See D6 for the mechanism.
- `medium` — `UPLOAD_MAX_BYTES` (`config.go:47`, default 2147483648) and `CLIENT_UPDATE_MAX_UPLOAD_BYTES` (`admin-service/config.go:86`, default 2147483648) are two independently-declared knobs that must agree; nothing validates them, so raising one alone turns a relayed upload into an opaque 400 at the far end.
- `low` — the shared dev credential is coupled by convention only: `UPLOAD_TOKENS=admin-service:${CLIENT_UPDATE_TOKEN:-…}` (`client-update-service/deploy/docker-compose.yml:29-35`) against `CLIENT_UPDATE_TOKEN` (`admin-service/config.go:69`); the account-name prefix is duplicated as a literal on this side.

Not applicable / verified N/A: no `pkg/subject` usage (no NATS subjects are constructed anywhere in the service — no raw `fmt.Sprintf` subject drift possible); no JetStream consumer, no `pkg/stream`, no `bootstrap.go` — correct, since the service publishes and consumes nothing; no OUTBOX/INBOX participation, so the `pkg/outbox` partition rule and the `chat.outbox.{siteID}.{destSiteID}.{eventType}` subject shape do not apply; no `pkg/model` event structs, so the `Timestamp int64` publish-site rule does not apply; no Mongo/Cassandra, so `MESSAGE_BUCKET_HOURS`, `ROOM_KEY_RETIRED_TTL` and `pkg/idgen` entity-format rules do not apply (the only `idgen` uses are `IsValidUUID`/`GenerateRequestID` for the correlation ID, `middleware.go:69-71`, which matches the CLAUDE.md §3 36-char hyphenated UUID rule). `docs/client-api.md` is correctly untouched: this service registers no `chat.user.` subject and is not `auth-service`, so the §5 documentation trigger does not fire.
- `low` — the object key namespace `chat.go/chat-versions/` is a bare local constant (`version.go:21`); no other service references it, so the download URL→object mapping is undiscoverable from outside this file.

### Recommendations
- `high` — make the read deadline configurable and default it to `HTTP_WRITE_TIMEOUT` (see D6), restoring the documented `admin-service` constraint.
- `medium` — validate the byte caps at the boundary: either derive both from one operator-facing knob, or have `admin-service` surface a 413 that names both limits.
- `low` — export the object prefix as a documented constant (or a tiny `pkg/` type shared with `admin-service`), so the relay and the store agree by construction rather than by string.

---

---

## 7. Performance — 3 / 5

The streaming upload path and the singleflight LRU are well-engineered and proven by tests, but a hardcoded 30s read deadline caps every real upload and the cache's memory budget is bounded by entry count rather than bytes.

### Findings
- `high` — `ReadTimeout: 30 * time.Second` (`client-update-service/main.go:80`) bounds reading the **entire request including the body**, so any upload taking longer than 30s is severed mid-body. With `UPLOAD_MAX_BYTES` defaulting to 2 GiB (`config.go:47`), a full-size artifact needs a sustained ~570 Mbps to finish in time; a 200 MiB artifact needs ~57 Mbps. `HTTP_WRITE_TIMEOUT` (10m, `config.go:40`) does not help — it governs the response. The sibling service gets this right and says why: "equal because WriteTimeout starts at end-of-header and so must cover the body", `upload-service/main.go:38`, with `ReadTimeout: httpTimeout` at `:206`. Uncovered by tests, because `run()` is 0%.
- `medium` — the download cache is bounded by entry count, not bytes: `CACHE_MAX_ENTRIES` 4 × `CACHE_MAX_OBJECT_BYTES` 512 MiB = **2 GiB** of resident heap at steady state, plus in-flight fills — `config.go:49-51`, `cache.go:31-37`, allocation at `version.go:173` (`make([]byte, info.Size)`). Nothing couples these defaults to the pod memory limit, and `deploy/docker-compose.yml:26-28` ships exactly those values.
- `medium` — every download of an object above `CACHE_MAX_OBJECT_BYTES` performs two `GetObject`+`Stat` round trips: `loadObject` opens and closes it to learn the size (`version.go:164-178`), then `streamObject` re-opens it (`version.go:159`, `:181-195`). The oversize case is precisely the latency-sensitive one.
- `low` — no `ReadHeaderTimeout` is set (`main.go:77-82`); `upload-service/main.go:205` sets 5s. Today `ReadTimeout` covers slowloris, but that stops being true the moment the read deadline is raised to fix the finding above.
- `low` — the unauthenticated `GET /api/v1/version/:fileName` has no rate limit (`routes.go:17`); a cache miss or a cache-disabled deployment turns each request into a MinIO round trip.

Verified good: the upload never buffers the artifact — parts stream from `*multipart.FileHeader` straight into `Put` (`version.go:97-115`) with `MaxMultipartMemory` lowered to 1 MiB (`config.go:75`), proven by a peak-heap assertion at 48 MiB artifact / 24 MiB ceiling (`version_test.go:383-424`); concurrent cache misses collapse through `singleflight` with a generation counter that prevents an upload-racing fill from reviving stale bytes (`cache.go:65-91`, tested at `cache_test.go:104`); `Open` bounds itself with `MINIO_DOWNLOAD_TIMEOUT` and releases the context on `Close` (`store_minio.go:37`, `:93-97`); no `time.Sleep` synchronization and no goroutine without a termination path apart from the D2 shutdown note. `pkg/jsretry`, consumer `BackOff`, `MaxAckPending`/`MAX_WORKERS`, sonic codec and the Cassandra `USING TIMESTAMP` rules are all inapplicable — the service has no JetStream or Cassandra surface.

### Recommendations
- `high` — introduce `HTTP_READ_TIMEOUT` defaulting to the same value as `HTTP_WRITE_TIMEOUT` (10m) and set `ReadHeaderTimeout: 5s` alongside it, mirroring `upload-service/main.go:205-207`; assert the matrix in the `newServer` unit test from D3.
- `medium` — bound the cache by total bytes, not entries: either add a `CACHE_MAX_TOTAL_BYTES` budget enforced in `blobCache.add`, or document `CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES` as the pod's required memory headroom in the compose/deploy manifests.
- `medium` — return the already-open reader from `loadObject` when the object is oversized, so the stream path reuses it instead of re-opening.
- `low` — add a modest per-IP rate limit (or rely on the ingress) for the unauthenticated download route, and set `nosniff` while doing so.
- `low` — surface `MINIO_DOWNLOAD_TIMEOUT` (5m) and `HTTP_WRITE_TIMEOUT` (10m) in one startup log line, so an operator can see the two deadlines that bound a large download.

---

**Consolidated summary.** `client-update-service` is a small, unusually well-crafted service: consumer-defined store interface, constructor DI, streaming-proven upload path, singleflight-and-generation cache, constant-time credential comparison, and comments that explain *why*. Three items are worth fixing before it carries production traffic, in order: (1) the hardcoded 30s `ReadTimeout` that caps every real artifact upload and silently breaks `admin-service`'s documented 10m relay budget (`main.go:80`); (2) the 76.8% coverage floor breach, which is 50 statements of untested `main.go` wiring — the same wiring that hides finding (1), fixable by extracting `newServer`; (3) the re-declared MinIO knobs, which the CLAUDE.md §6 shared-knob rule forbids and which have already drifted across three services (`MINIO_BUCKET` is required here, defaulted to `avatars` in media-service). Everything else is `low` or `nitpick`.
