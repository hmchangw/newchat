# media-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `032ec71` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.3, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

media-service is well built for what it does — errcode discipline is textbook in every handler bar one, eight of its nine Mongo reads project precisely, blobs stream rather than buffer, all three image paths honour conditional requests, and its boundary with upload-service is genuinely clean (separate buckets, separate collections, no shared key convention). Two defects stand out. The bot-avatar upload decodes an image with no header or dimension pre-check, so a one-megabyte file declaring huge dimensions allocates gigabytes and kills the pod — the emoji path guards exactly this case a few files away, and the comment there calls it decompression-bomb hardening. And error responses inherit a six-hour public cache lifetime, because the cache headers are set before the blob fetch: a transient storage blip gets pinned in browsers and any shared CDN for hours. Around those, the hot read path is under-protected: the avatar lookup is the one unprojected find in the service, the two highest-volume reads are uncached and even a conditional-request hit pays the database round trip, redirects carry no cache directive so every render of a normal user's avatar re-hits the service, and nothing bounds in-flight HTTP requests or puts a deadline on the metadata hops. The `drive.members` route is a third sub-domain with its own hand-rolled error envelope, no inline justification and no entry in the client API doc; a `wrong_cluster` reason reaches the wire without being in the reason table, and can ship a dangling "upload to " when the owning site is missing from the domain map. Coverage reads 70% but that understates it — the store and blob tiers are container-tested and simply invisible to the unit profile; the real gaps are `run`'s validation gates and two store methods untested at any tier.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 3 |

**Findings by severity:** 0 critical, 2 high, 26 medium, 16 low, 5 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] `request failed` error logs carry no request_id on any public route — `media-service/middleware_auth.go:33` — `errcode.WithLogValues` is installed only inside `requireSession`, i.e. only on the two session-gated PUTs. `requestIDMiddleware` (`media-service/middleware.go:18-21`) stores the id in a gin key and a `natsutil` ctx key, neither of which `errcode.Classify` reads (`pkg/errcode/logctx.go:23-27` falls back to `slog.Default()`). So every `errhttp.Write` on the GET routes (`handler.go:88,128,150,170,185,200`, `emoji_serve.go:31,50,55,69,74`) emits an unattributed line, breaking CLAUDE.md §3 "include in all log lines". The inline comment at `middleware_auth.go:31-32` claims the inverse of what the code does.
- [medium] Error responses inherit a 6-hour public cache lifetime — `media-service/handler.go:75` — `setImageCacheHeaders` runs *before* the blob fetch, so the 500 written at `handler.go:89` goes out with `Cache-Control: public, max-age=21600` plus the avatar's `ETag`. Same shape at `emoji_serve.go:59` for the 404/500 at `:69`/`:74`. A transient MinIO blip is then pinned in browsers and any shared CDN for hours. (The deliberately-cached 404 in `serveDefault`, `handler.go:52-59`, is fine and documented; the failure paths are not.)
- [medium] `drive.members` bypasses the mandated errcode/errhttp boundary — `media-service/drive.go:46` — `writeDriveError` + `driveErrorResponse` (`drive.go:32-36`) is a second hand-rolled client-facing error envelope, and `drive.go:70,81,92` log at Error level by hand instead of letting `Classify` do it once. It *is* a deliberate external contract (`docs/superpowers/specs/2026-07-17-drive-members-endpoint-design.md` §4), but nothing in the file says so, so it reads as a plain §3 violation and invites copy-paste.
- [low] A recovered panic produces no access-log line — `media-service/middleware.go:47-50` — `accessLogMiddleware` logs after `c.Next()` with no `defer`, and is registered *after* `gin.Recovery()` (`main.go:104-106`), so the panicking request unwinds past it. Relatedly, `o11ygin.Middleware` is installed at `main.go:103`, before `requestIDMiddleware` at `:105`, so the o11y span never sees the resolved request id.
- [low] Response Content-Type is echoed from MinIO metadata rather than the validated stored value — `media-service/handler.go:93` and `emoji_serve.go:79` — uploads are format-checked (`upload.go:66-71`, `emoji_upload.go:61-80`) and the bucket is media-service-only, so exploitability today is low; but `model.Avatar.ContentType` (`pkg/model/avatar.go:24`) is already loaded and is the allowlisted value. For a service streaming blobs to browsers, pinning to it (alongside the existing `nosniff` + `default-src 'none'`, `handler.go:43-44`) is the cheap hardening.
- [low] The coalesced employee-id load runs with no deadline — `media-service/cache.go:50` — `context.WithoutCancel(ctx)` correctly detaches from one caller's cancel but also drops the deadline, so the singleflight Mongo lookup is bounded only by the driver's server-selection timeout. Callers do bail via `ctx.Done()` (`cache.go:68`), so this is tail-latency/goroutine-lifetime, not a leak.
- [nitpick] `res.Val.(string)` is an unchecked assertion (`cache.go:66`); `/healthz` returns an untyped `gin.H` (`handler.go:35`) where every other response in the service is a typed struct; `main.go:53,56` return bare `err` from config validation — the underlying messages are self-describing env-var text and this matches all 20 peer services, so it is noted only.

### Recommendations

- [medium] Move `errcode.WithLogValues(ctx, "request_id", id)` into `requestIDMiddleware` — `media-service/middleware.go:18-21` — one line makes every `Classify` line on every route correlate, and lets `middleware_auth.go:33` drop its special case (and its now-wrong comment).
- [medium] Set cache headers only on the success/304 paths — `media-service/handler.go:75`, `emoji_serve.go:59` — move `setImageCacheHeaders` below the `h.blobs.Get` error branches, or explicitly `Cache-Control: no-store` before `errhttp.Write`, so a MinIO outage is not cached for 6h.
- [medium] Add an inline justification on the bespoke drive envelope — `media-service/drive.go:32` — e.g. `// errcode bypass justification: external Drive contract, see docs/…/2026-07-17-drive-members-endpoint-design.md §4`, matching how the repo grandfathers `$lookup` sites.
- [low] `defer` the access-log write and register `requestIDMiddleware` before `o11ygin.Middleware` — `media-service/middleware.go:47`, `main.go:103-106` — panicked requests then still produce a request line, and traces carry the id.
- [low] Serve `av.ContentType` / the doc's stored content type instead of the MinIO stat value — `media-service/handler.go:93`, `emoji_serve.go:79` (extend the `EmojiDoc` projection at `store_mongo.go:142` by `contentType`) — guarantees only allowlisted image types leave the service.
- [low] Wrap the detached fetch in `context.WithTimeout` — `media-service/cache.go:50` — keeps the dedup guarantee while bounding the shared load.

## 3. Architecture — score 4

### Evidence

- [medium] Shutdown order inverts the repo's HTTP-service pattern and puts the blob-stream drain last — `media-service/main.go:131-140` — the chain is `router.Shutdown` → `natsutil.Drain` → `srv.Shutdown` → `obsShutdown`, while `upload-service/main.go:219-224` and `auth-service/main.go:137-143` close the HTTP listener first. `pkg/shutdown/shutdown.go:39-50` gives all steps one shared 25s budget sequentially, so the two NATS drains can consume it before the listener — exactly the step that needs it, since the avatar/emoji routes deliberately have no per-route deadline (`main.go:107-110`). The HTTP side never calls NATS, so nothing is gained by that order.
- [medium] If the 25s budget expires before `srv.Shutdown` is reached, `run()` hangs until SIGKILL — `media-service/main.go:143-146` — `shutdown.Wait` returns on timeout without having closed the listener, so `ListenAndServe` never returns and the blocking `<-srvErr` never fires. Bounded only by Kubernetes' 30s grace period, but it turns a slow drain into an ungraceful kill.
- [medium] `drive.members` is an off-contract boundary: bespoke error envelope and undocumented — `media-service/drive.go:213-215`, `routes.go:225` — `writeDriveError` writes `{success,error,errorType}` instead of `errhttp.Write`/`pkg/errcode`, the only client-facing HTTP route in the service that does, with no inline justification. It is also absent from `docs/client-api.md` §7, whose index lists five routes and whose prose says "the three GETs are public" while `routes.go` registers four public GETs.
- [medium] The only in-process cache has no invalidation model and no negative caching — `media-service/cache.go:194-222`, `config.go:98` — `eidCache` is LRU+TTL only (24h default) with nothing consuming a user-updated event, so a changed or cleared `employeeId` stays wrong for a day; and because misses are deliberately not cached (`cache.go:206-209`), every avatar render for an account without an `employeeId` is a fresh Mongo `FindOne` on the `<img src>` hot path, with singleflight collapsing only concurrent ones.
- [low] `avatarStore` has outgrown its name and its segregation — `media-service/store.go:244-264` — seven methods across `users`, `subscriptions` and `avatars`, two of which (`UserByAccount`, `RoomMember`) exist solely for the drive probe and have nothing to do with avatars. `emojiStore` shows the right granularity; splitting a `driveStore` out would restore it.
- [low] `Avatar()` fetches the whole document — `media-service/store_mongo.go:109` — the only find in the file without `SetProjection`, against CLAUDE.md's "always project precisely"; callers use just `ETag` and `MinioKey` (`handler.go:76,81`). (Perf dimension owns the wider projection sweep.)
- [low] `blobStore` is declared beside its implementation, not with its consumer — `media-service/minio.go:206-210` — CLAUDE.md puts store interfaces in `store.go`, and the consequence is concrete: the `//go:generate mockgen -source=store.go` directive (`store.go:240`) does not cover it, so `handler_test.go:53` hand-rolls `fakeBlobStore`.
- [nitpick] Middleware order costs correlation and panic coverage — `media-service/main.go:102-106` — `requestIDMiddleware` runs after the o11y middleware, so o11y spans/metrics are emitted without the request id the rest of the service logs; and `gin.Recovery()` is third, so a panic in the CORS or o11y middleware is unrecovered.
- [nitpick] Blob key namespacing is inconsistent under a non-site-scoped bucket — `emoji_upload.go:139` keys emoji as `emoji/{siteID}/{shortcode}` but `avatar.go:176` keys bot avatars as `bot/{account}`, with `MINIO_BUCKET` defaulting to a flat `avatars` (`config.go:74`) rather than upload-service's `chat-{siteID}`. `docker-local/README.md:309-312` already records the resulting cross-site collision surface.

### Recommendations

- [medium] Move `srv.Shutdown` to the first step of `shutdown.Wait` and fold `mongoutil.Disconnect` into the chain — `media-service/main.go:131-140` — matches upload-service/auth-service, guarantees the listener closes inside the budget, and removes the hang-until-SIGKILL path at `main.go:143`.
- [medium] Either route `drive.members` through `errcode`/`errhttp` or keep the legacy envelope with an inline `// legacy Drive contract: …` justification, and add the endpoint to `docs/client-api.md` §7 (fixing the "three GETs" count) — `drive.go:213-283`.
- [medium] Give `eidCache` a bounded negative-result TTL and shorten `EID_CACHE_TTL`, or subscribe to a user-updated signal — `cache.go:206-209`, `config.go:98` — removes a per-render Mongo query for every non-employee account and caps the stale window.
- [low] Split `avatarStore` into the avatar/site lookups and a `driveStore` (`UserByAccount`, `RoomMember`) — `store.go:244-264` — restores one interface per concern and shrinks the mock surface.
- [low] Add `SetProjection(bson.M{"minioKey":1,"etag":1,"contentType":1})` to `Avatar()` — `store_mongo.go:109` — brings the last unprojected find in line with the other seven.
- [low] Move `blobStore`, `blobInfo` and `errBlobNotFound` into `store.go` so mockgen generates the mock — `minio.go:197-210` — drops the hand-written `fakeBlobStore`.
