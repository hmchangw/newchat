# client-update-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `bf684cf` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

client-update-service distributes the desktop client's binaries to the whole fleet, and it is the weakest link in that chain in ways that matter more than its size suggests. Nothing verifies the integrity of what it serves: no checksum, no signature, no digest header, and the download route is unauthenticated, so a client cannot tell a tampered executable from a good one and no record exists of which bytes were published or by whom. The read timeout is hardcoded at thirty seconds while the service advertises a two-gigabyte upload cap, and Go's read timeout bounds the whole request body — so admin-service's documented ten-minute relay budget is silently capped, and any artifact needing more than about a hundred megabytes of transfer is severed mid-body. This was flagged in the previous audit and is still unfixed. Publication is not atomic either: the two objects are written by independent uploads with no locking, so a mid-pair failure or two concurrent uploads leave a mismatched descriptor and executable, and both callers get a success. Cache invalidation is process-local with a twenty-four-hour default, so on more than one replica the documented "a re-upload busts the cache" claim holds only on the pod that received the upload, and a client can be handed a new descriptor with an old binary. On the download path there is no conditional-request support, no range support and no cache directives, so every startup poll re-transfers the whole body, no proxy can absorb a release herd, and an interrupted download restarts from zero. The singleflight fill runs on the first requester's context, so one client hanging up fails every concurrent waiter. Coverage is 76.8% and the entire gap is `run`, which is where the read timeout lives.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 10 high, 21 medium, 13 low, 3 nitpick (47 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] `MINIO_DOWNLOAD_TIMEOUT` is re-declared with its own `envDefault` in two services — `client-update-service/config.go:24` (also `upload-service/main.go:101`) — CLAUDE.md §Configuration: "A knob shared by more than one service is declared once, in the package that owns the thing it configures… Never re-declare the env tag and `envDefault` in a service." Both currently say `5m`, so there is no live divergence, but `pkg/minioutil` exposes no config type at all (`grep 'env:' pkg/minioutil/*.go` → no hits), so nothing prevents one side drifting. This is the exact failure mode the rule exists to stop.
- [medium] `contentDisposition` duplicates an existing helper with weaker behaviour — `client-update-service/version.go:205-207` vs `upload-service/handler.go:485-491` — same function name, same purpose, two implementations. This one is `fmt.Sprintf("attachment; filename=%q", fileName)`; Go's `%q` leaves printable non-ASCII as raw UTF-8, which RFC 6266 forbids in a quoted-string (must be ISO-8859-1), so `réport.exe` reaches the client mojibake'd. upload-service emits RFC 5987 `filename*=UTF-8''…` and has table tests for it (`upload-service/handler_test.go:704`). No injection risk — `%q` escapes CR/LF — so this is correctness/consistency, not security.
- [medium] Access-log lines are emitted with no trace correlation — `client-update-service/middleware.go:85` — `slog.Info(...)` passes `context.Background()` to the handler chain, and the o11y SDK derives `trace_id`/`span_id` solely from the context (`flywindy/o11y@v0.12.0/internal/log/handler.go:70-71`, `trace.SpanFromContext(ctx)`). Every request line therefore lands uncorrelated, against the CLAUDE.md observability contract ("all trace-correlated"). `c.Request.Context()` already carries the o11ygin span (installed at `main.go:70`, before this middleware at `:73`); the repo convention is `slog.InfoContext` (`pkg/ginutil/middleware.go:67`, `media-service/middleware.go:50`, `botplatform-service/middleware.go:27`). One-token fix.
- [medium] Request-ID and access-log middleware are hand-rolled instead of `pkg/ginutil` — `client-update-service/middleware.go:67-94` — `pkg/ginutil` is documented as "the serving-layer toolkit shared by the HTTP services: request-ID, access log and CORS middleware" and seven services use `ginutil.RequestID()`/`AccessLog()` (auth, user, portal, admin, tcard, botplatform, teams-room-inspector). The local copy bypasses `idgen.ResolveRequestID` — the stated "single owner of mint-vs-pass-through policy" (`pkg/idgen/idgen.go:179`) — and so silently discards a malformed inbound `X-Request-ID` where `ginutil.RequestID` logs a Warn, losing the signal that a caller is sending bad correlation IDs.
- [low] `err != http.ErrServerClosed` instead of `errors.Is` — `client-update-service/main.go:101` — works today because `ListenAndServe` returns the sentinel unwrapped, but the repo majority (11 of 15 non-tool sites, e.g. `admin-service/main.go:174`, `media-service/main.go:143`) uses `errors.Is`, which is what CLAUDE.md §Error Handling asks for and what survives a future wrap.
- [nitpick] Four bare `return err` pass-throughs — `cache.go:77`, `cache.go:87`, `version.go:167`, `store_minio.go:96` — all are internal hand-offs where the callee already wrapped (`store_minio.go:41,48,50`) and the caller re-wraps (`version.go:152,188`), so no context is actually lost. Flagged only so a reviewer does not re-derive it.

### Recommendations

- [high] Move `MinioDownloadTimeout` into a shared `minioutil.Config` (or equivalent named field, following `mongoutil.PoolConfig`) and mount it in both services — `config.go:24`, `upload-service/main.go:101` — removes the only CLAUDE.md MUST violation in the service.
- [medium] Delete the local `contentDisposition` and lift upload-service's RFC 5987 version into a shared helper used by both — `version.go:205` — fixes non-ASCII filenames and kills a name collision that reads as "same function" but is not.
- [medium] Change `slog.Info` → `slog.InfoContext(c.Request.Context(), …)` — `middleware.go:85` — restores `trace_id`/`span_id` on every access-log line for a one-word edit. upload-service/middleware.go:76 shares the defect.
- [medium] Replace `requestIDMiddleware` with `ginutil.RequestID()` and keep only the access log local (or extend `ginutil.AccessLog` with an optional extra-attrs hook) — `middleware.go:67`, `main.go:72-73` — restores the malformed-inbound Warn.
- [low] Use `errors.Is(err, http.ErrServerClosed)` — `main.go:101`.
- [low] Promote `"request_id"` to a named const beside `ctxServiceAccount` — `middleware.go:19,72,86`

### Reviewer notes

- SAST folded in as instructed: `gosec` medium+/confidence-low is clean repo-wide and the 20 repo-owned semgrep rules report 0 findings, so there is **no SAST finding under `client-update-service/` to escalate**. `govulncheck` and the semgrep registry packs (`p/golang`, `p/security-audit`) were blocked by the sandbox egress policy (403) and were NOT retried — dependency-vulnerability and registry-rule coverage for this service is therefore UNVERIFIED, not clean. I invented no CVE findings.
- Verified locally and cleanly: `go vet ./client-update-service/...` → exit 0; `go build ./client-update-service/...` → exit 0.
- Positives that held up under reading and are not re-litigated above: `pkg/errcode` tiering is correct throughout (Tier-1 constructors in handlers, exactly one Tier-2 adapter — `errhttp.Write` — and no log-AND-return anywhere); infra failures are returned as raw wrapped `fmt.Errorf` so they collapse to `internal` (`version.go:152,188`); `errors.Is`/`errors.As` are used for sentinel and `*http.MaxBytesError` checks (`version.go:51,148,184`), never string compares; no token ever reaches a log or an error string (`config.go:114-133` names accounts only, asserted by `config_test.go:103` and `middleware_test.go:157`); comments explain WHY, not WHAT.
- Unit coverage for this tree is 76.8% (given, not re-derived) — below the 80% floor, but that is the test-coverage dimension's finding, not scored here. The one gap that touches this dimension is that `contentDisposition`'s output encoding is only partially asserted (`version_test.go:201,243` use `Contains`), which is why the `%q` defect survived.

## 3. Architecture — score 3

### Evidence

- [high] MinIO knobs re-declared per service, against CLAUDE.md §6 "declared once, in the package that owns the thing it configures" — `client-update-service/config.go:19-24` — the same five `MINIO_*` tags exist in `media-service/config.go:70-74` and `upload-service/main.go:95-101`, and have already drifted: `MINIO_BUCKET` is `required` here, `envDefault:"avatars"` in media-service, bare in upload-service. `pkg/minioutil` exposes no Config to mount, so every new consumer repeats it.
- [high] Cache invalidation is process-local, so the published contract "a re-upload of the same name busts the cache" holds only on the replica that received the upload — `version.go:113`, `cache.go:53-61`, `docs/client-api.md:8981` (and `:8919`). `blobCache` is an in-process LRU with no shared invalidation channel (the service touches no NATS) and no revalidation against MinIO — the bucket is the source of truth but an entry is never re-`Stat`ed. With N>1 replicas, N-1 pods serve the superseded artifact for up to `CACHE_TTL` (default 24h, `config.go:50`), and since descriptor and executable are cached independently a client can get a new `.yaml` with an old `.exe`.
- [high] `ReadTimeout` hardcoded at 30s while the service advertises a 2 GiB upload cap — `main.go:80` vs `config.go:47`. `http.Server.ReadTimeout` bounds the whole request body, and nothing here extends it per request (no `http.NewResponseController` in the tree), unlike `admin-service/client_update.go:401-410` on the relaying side. Only `WriteTimeout` is configurable, so admin-service's documented 10m relay budget (`docs/client-api.md:8309`) is silently capped at 30s of transfer.
- [medium] Nothing verifies the integrity of what this software-distribution endpoint serves — `version.go:97-115`, `store.go:22-28`. `Put` records no checksum or signature, `blobInfo` carries none, no `ETag`/`Digest` header reaches the client, and the design doc lists "checksums/signatures" as out of scope (`docs/superpowers/specs/2026-08-03-client-update-service-design.md:313`). The only integrity boundary is the upload token plus bucket ACLs; a downloader (unauthenticated, `routes.go:17`) cannot tell a tampered executable from a good one.
- [medium] No version identity, and publication is not atomic — `version.go:84-91`. Despite the name there is no version-comparison logic at all (grep finds only the token compare, `middleware.go:42`); "a version" is two mutable object names under a fixed prefix (`version.go:21`). The two `storeFormFile` calls are independent `Put`s with no locking, so a mid-pair failure or two concurrent uploads leave a mismatched pair and both callers get 200. Documented (`docs/client-api.md:8940-8948`), but it means no rollback, no immutable release, no coherence signal.
- [low] `blobCache.gen` is one global counter, not per key — `cache.go:23,56,81`. An upload of `app.exe` makes an in-flight singleflight fill of `app.yaml` discard its result, so unrelated keys re-fetch from MinIO.
- [low] `obsShutdown` is skipped on every startup failure after `obs.Init` — `main.go:51-61`. A MinIO connect or `ensureBucket` failure returns without flushing the exporter, dropping the telemetry that explains the crash-loop.
- [nitpick] Handler methods split across `handler.go` (health only) and `version.go` — `handler.go:22`, `version.go:43,123`. Defensible, but a reader following the CLAUDE.md layout finds almost nothing in `handler.go`.

### Recommendations

- [high] Move the `MINIO_*` block into a `minioutil.Config` mounted as a named field (`envPrefix` where a service needs one) and migrate all three consumers — `config.go:19-24` — one declaration, no further drift.
- [high] Make `ReadTimeout` configurable (or extend the read deadline in the upload handler as admin-service does) and assert it is ≥ the relaying service's budget at startup — `main.go:80`.
- [high] Either cut `CACHE_TTL` to minutes, revalidate a hit against the object's `ETag` before serving, or add an authenticated purge the publisher fans out to all replicas — `cache.go:31-37`, `version.go:113`. Until then narrow the doc's cache-bust claim to single-replica deployments.
- [medium] Store the SHA-256 as object metadata on `Put`, surface it in `blobInfo`, emit it as a response header so the fleet can verify before executing — `store.go:22-28`, `version.go:181-195`.
- [medium] Make a publish atomic and versioned: write the pair under an immutable `<version>/` prefix and flip one small pointer object last — `version.go:84-91` — removes the mismatched-pair window, gives rollback, and makes cache entries immutable by key.
- [low] Key the invalidation generation by object key — `cache.go:23,56,81`.
- [low] Run the shutdown functions on the startup-failure path so `obsShutdown` always flushes — `main.go:51-61`.
