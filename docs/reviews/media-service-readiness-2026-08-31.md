# media-service — Production Readiness Review

**Service:** `media-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

A conventional, well-built Gin service: consumer-defined store interfaces, constructor DI, `pkg/subject` builders, shared knobs mounted correctly, streaming with ETag/304, and a singleflight LRU. The handler, upload, serve, drive and middleware layers all sit at 95–100% coverage with real error-path assertions.

Two performance findings have security consequences. **`Cache-Control: public, max-age=21600` and the `ETag` are written before the blob fetch**, so a MinIO 500 or a not-found inherits them and becomes a **shared-cache-storable error response** — on both the avatar and the emoji serve paths. And **the bot-avatar upload calls `image.Decode` with no `DecodeConfig` dimension pre-check**, so a small compressed PNG declaring huge dimensions allocates the full pixel buffer before any bound applies — while the emoji path one file over *does* have that guard, having diverged from a shared shape that was copy-pasted rather than factored.

Around those: `drive.members` bypasses `pkg/errcode` entirely for a bespoke envelope and is absent from `docs/client-api.md`; `Access-Control-Allow-Origin: *` is hardcoded onto two authenticated PUT routes with `x-auth-token` in the allowed headers; and authorization is asymmetric across transports — HTTP emoji upload is admin-gated while the NATS `emoji.delete` RPC is open to any authenticated account.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 21 | 18 | 10 | **52** |

---

## 2. Go code quality — 4 / 5

Idiomatic, consistently wrapped, precisely projected Go with clean errcode Tier-1 usage; the deductions are a handful of narrow CLAUDE.md Section 3 violations (bare `return err`, a mis-chosen `reason`, a message that can render half-empty) rather than anything structural.

### Findings
- `medium` — bare `return err` twice in startup validation, violating "Never return bare `err`" — `media-service/main.go:53`, `media-service/main.go:56`
  Every other error in `run()` gets a `fmt.Errorf("...: %w", err)` frame; these two lose which knob failed at the `slog.Error("media-service exited")` site.
- `medium` — a cross-site session is rejected with `errcode.AdminNotAuthorized` (wire value `not_admin`), the same reason emitted for a genuine role failure — `media-service/middleware_auth.go:47` vs `:79`, `:98`
  The frontend cannot branch "wrong site" from "not an admin", which is exactly what `WithReason` exists for. It also borrows a reason from `pkg/errcode/codes_admin.go:3`, whose own comment says "Emitted by admin-service handlers/middleware", while media-service already owns `codes_avatar.go`.
- `medium` — client-facing message interpolates a base URL that can be empty — `media-service/upload.go:51`
  `clusterBaseURL` returns `""` for a site absent from `CLUSTER_DOMAINS` (`config.go:43`); the 409 then reads `bot is owned by another cluster; upload to `. The read path explicitly handles the same `""` case (`handler.go:103`); the write path does not.
- `low` — `drive.members` bypasses `pkg/errcode`/`errhttp` for a bespoke `{success,error,errorType}` envelope — `media-service/drive.go:46`, `:60-95`
  CLAUDE.md Section 3 says errcode for ALL client-facing errors. It is justified (legacy Drive consumer) in `docs/superpowers/specs/2026-07-17-drive-members-endpoint-design.md:102`, but nothing in `drive.go` records the WHY or points at the spec, so the next reader reads it as drift.
- `low` — gin-context keys are scattered string literals while one of them is a const — `media-service/handler.go:51,56,61`, `emoji_serve.go:25`, `emoji_upload.go:94`, `middleware.go:19,57-59` vs the `ctxSessionKey` const at `middleware_auth.go:15`
  Consequence, not just style: `upload.go` and `drive.go` never set `media_kind`/`media_outcome`, so bot-avatar uploads and every `drive.members` call emit an access-log line with both fields blank — a silent hole in the one observability field this service adds.
- `low` — the singleflight load detaches from cancellation but gains no deadline of its own — `media-service/cache.go:50`
  `context.WithoutCancel(ctx)` is right for coalescing, but a hung `EmployeeID` now has no termination path of its own; callers escape via `ctx.Done()` (`:68`) while the goroutine and the singleflight key stay parked. CLAUDE.md Concurrency: "Never launch goroutines without a clear termination path."
- `low` — store errors reach `errhttp.Write` verbatim, so the handler adds no frame — `media-service/handler.go:88,128,150,170,185,200`, `upload.go:42`
  Permitted by the Tier-1 "raw wrapped error collapses to internal" rule, and the store does wrap (`store_mongo.go:76`), but the logged cause says only `find room subscription`, never which of the four call sites was serving. `emoji_serve.go:50` and `emoji_upload.go:135` show the better local pattern.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (egress blocked), so dependency-CVE and third-party-pattern coverage is unverified for this service.
- `nitpick` — two spellings of the same conditional-GET guard: `emoji_serve.go:60` guards `e.ETag != ""` on the stored side, `handler.go:76` guards `m != ""` on the request side, and `handler.go:65` guards neither.

Notable strengths, verified: no `fmt.Println`/`log.Println` anywhere; no error comparison by string; `errors.Is(err, mongo.ErrNoDocuments)` at all seven miss-expected sites; a proper sentinel + `errors.Is(err, errBlobNotFound)` across the blob boundary (`minio.go:15`, `handler.go:82`); an explicit projection on every find but `Avatar` (`store_mongo.go:39,55,71,84,96,142,155,203`) and no `$lookup`; no `os.Getenv`; shared knobs mounted as named fields (`config.go:68,110`), not re-declared.

### Recommendations
- `medium` — wrap both startup validations: `fmt.Errorf("validate mongo pool config: %w", err)` and `fmt.Errorf("validate nats guard config: %w", err)` at `main.go:53,56`.
- `medium` — add an `AvatarSessionWrongSite` (or similar) `Reason` to `pkg/errcode/codes_avatar.go` and use it at `middleware_auth.go:47`, leaving `AdminNotAuthorized` for the two genuine role checks.
- `medium` — at `upload.go:49`, branch on `base == ""` and emit a message without the dangling URL (e.g. `fmt.Sprintf("bot is owned by cluster %s", siteID)`), mirroring the read path's handling at `handler.go:103`.
- `low` — promote `media_kind`, `media_outcome`, `request_id`, `bot_account` to consts beside `ctxSessionKey`, and set kind/outcome in `HandleBotUpload` and `HandleDriveMembers` so the access log is complete for all seven routes.
- `low` — give the detached load in `cache.go:50` its own bounded deadline (`context.WithTimeout(context.WithoutCancel(ctx), …)`) so the goroutine has a guaranteed exit independent of driver behaviour.
- `low` — add a one-line WHY comment at `drive.go:45` naming the external Drive contract and linking the design spec, so the errcode bypass reads as sanctioned rather than as drift.
- `low` — re-run `govulncheck` and the semgrep registry packs in an environment with egress before treating this service's SAST posture as fully verified.

---

## 3. Architecture — 4 / 5

Clean, conventional Gin service — consumer-defined store interfaces, constructor DI, `pkg/subject` builders, typed env config with shared knobs mounted correctly, and `pkg/shutdown.Wait` — with a handful of localized boundary deviations (bespoke error envelope, interface placed with its implementer, best-effort index that a correctness path depends on).

### Findings
- `medium` — `drive.members` bypasses `pkg/errcode`/`errhttp` entirely, writing a bespoke `{success,error,errorType}` envelope with its own four-constant error taxonomy, and logs-then-writes on every failure path — `media-service/drive.go:32-48`, `:70-71`, `:81-82`, `:92-93`
  CLAUDE.md mandates `errcode` for ALL client-facing errors and forbids log-AND-return (`Write` classifies and logs once). No justification comment, and the route is absent from `docs/client-api.md`, so the divergent contract is undocumented.
- `medium` — the `(siteId, shortcode)` unique index is created best-effort and only warned on failure, yet `UpsertEmoji`'s duplicate-key retry is written on the assumption that the index exists — `media-service/main.go:78-80`, `media-service/store_mongo.go:189-193`
  Without the index two concurrent first-time creates both insert, leaving two docs for one shortcode; `EmojiDoc`/`DeleteEmoji` then resolve non-deterministically. This is a correctness invariant, not an optimization — it should fail startup.
- `medium` — authorization is asymmetric across the two transports for the same resource: HTTP emoji upload is admin-gated, the NATS `emoji.delete` RPC is open to any authenticated account, guarded only by a kill-switch — `media-service/routes.go:20`, `media-service/emoji_nats.go:37-43`
  A shortcode is site-wide shared state (the handler's own comment says so); create requires admin, destroy does not.
- `medium` — `Access-Control-Allow-Origin: *` is hardcoded and applied to the two authenticated PUT routes, with `x-auth-token` in the allowed-headers list — `media-service/middleware.go:29-42`, `media-service/routes.go:19-20`
  The repo's other blob service parameterizes this (`upload-service/main.go:48-50`, `CORS_ALLOWED_ORIGINS` with an empty default). Public GETs justify `*`; the write routes should not inherit it.
- `low` — `blobStore` is declared in `minio.go`, the file that implements it, not with its consumer — `media-service/minio.go:23-27`
  CLAUDE.md: "Define interfaces in the consumer, not the implementer"; `avatarStore`/`emojiStore` follow this in `store.go:13,38`. Consequence: `blobStore` is outside the `//go:generate mockgen -source=store.go` directive (`store.go:9`), so its double is a hand-written `fakeBlobStore` (`handler_test.go:53-91`) instead of a generated mock.
- `low` — the comment asserting "No blanket HTTP timeout … a short deadline would cancel a slow up/download mid-stream" sits three lines above a blanket `WriteTimeout: 30s` / `ReadTimeout: 15s` — `media-service/main.go:107-119`
  `WriteTimeout` starts at end-of-header and bounds the whole stream (see the correct treatment in `upload-service/main.go:34-41`). The values happen to be generous for ≤1 MiB payloads, so this is a misleading comment rather than a live bug — but it will mislead whoever next raises `MAX_UPLOAD_BYTES`.
- `low` — `avatarStore` carries `UserByAccount` and `RoomMember`, which exist solely for the `drive.members` probe and have nothing to do with avatars — `media-service/store.go:13-33`
  The name no longer describes the seam; `handler` now straddles avatars, the drive membership probe, and custom emoji with one struct and four dependencies.
- `nitpick` — `requestIDMiddleware`/`accessLogMiddleware` are registered after `gin.Recovery()`, so a panicking request is recovered upstream and produces no access-log line (the `slog` call after `c.Next()` is not deferred) — `media-service/main.go:104-106`, `media-service/middleware.go:46-61`
- `nitpick` — `srv.Shutdown` runs third, after `router.Shutdown` and `natsutil.Drain`, so the HTTP listener keeps accepting new requests while NATS is already drained — `media-service/main.go:131-140`. Harmless today (no HTTP handler publishes to NATS) but inverts the usual stop-accepting-first order.

Compliance confirmed (no finding): flat repo-root service with the mandated file set; `GET /healthz` at `routes.go:12`; routes wholly in `routes.go`; Resty via `restyutil` with a 5s timeout (`main.go:111-112`); request-ID middleware using `idgen.ResolveRequestID` and propagated via context (`middleware.go:15-27`); no `os.Getenv`; `mongoutil.PoolConfig` and `natsrouter.GuardConfig` mounted as named fields, never re-declared (`config.go:68,110`); no JetStream usage anywhere, so the absent `bootstrap.go` is correct, not a gap.

### Recommendations
- `medium` — Convert `drive.go` to return `*errcode.Error` through `errhttp.Write`; if the `{success,errorType}` shape is a hard external contract, keep it but add a justification comment and document the route in `docs/client-api.md`.
- `medium` — Make `EnsureEmojiIndexes` fatal at startup (`return fmt.Errorf(...)` instead of `slog.Warn`), since `UpsertEmoji`'s race handling depends on it.
- `medium` — Gate `HandleEmojiDelete` on the admin role to match `requireAdmin()` on upload, or document why delete is deliberately open.
- `medium` — Replace `Access-Control-Allow-Origin: *` on the PUT routes with a configured allowlist, mirroring `upload-service`'s `CORS_ALLOWED_ORIGINS`.
- `low` — Move `blobStore` (+ `blobInfo`, `errBlobNotFound`) into `store.go` so mockgen generates its mock and the interface sits with its consumer.
- `low` — Rename `avatarStore`, or split the two drive-probe methods into a `memberStore`, so the interface names match the seams.
- `nitpick` — Reorder middleware to `requestID → accessLog → Recovery`, and move `srv.Shutdown` ahead of the NATS drain.

---

## 4. Test coverage — 2 / 5

Unit coverage is **70.0% (614 stmts)** — below the CLAUDE.md Section 4 floor, so the score is floored at 2; the quality of what *is* tested is genuinely strong (handler/upload/serve/drive/middleware layers are at 95–100% with real error-path assertions), and almost the entire deficit is the Mongo/MinIO/`main.go` layers.

### Findings
- `high` — Unit coverage 70.0% (614 stmts), under the 80% floor — `media-service/store_mongo.go:27`
  Per-file breakdown from the profile: `store_mongo.go` 84 stmts @ 0%, `main.go` 69 @ 0%, `minio.go` 18 @ 0%; those three account for 171 of the 184 uncovered statements. Excluding them, the business logic is ~97% covered (430/443). The number is a build-tag artifact, not vanity coverage — but the floor is the floor.

- `medium` — `UserByAccount` and `RoomMember` have **no integration test at all**; their Mongo filters/projections are only ever mocked — `media-service/store_mongo.go:81`, `media-service/store_mongo.go:94`
  `integration_test.go` covers `EmployeeID`/`BotSite`/`RoomSite`/`Avatar`/`SetBotAvatar` and `emoji_integration_test.go` covers the emoji CRUD, but neither touches these two. `drive_test.go:82-180` mocks them exhaustively, so a wrong field name or projection in the `drive.members` probe (the membership authorization decision) would pass every test in the repo.

- `medium` — the second, decompression-bomb hardening layer of `validateEmojiImage` is never exercised — `media-service/emoji_upload.go:76`
  Uncovered blocks are `73.51,75.3` (post-`image.Decode` error / format disagreement) and `76.59,78.3` (decoded-bounds check). `TestEmojiUpload_OversizeDimensions_400` (`emoji_upload_test.go:128`) only trips the *header* check at `:68`. The comment at `emoji_upload.go:55-60` explicitly frames the decoded-bounds check as the defence against a header that lies about its dimensions — that is the branch with zero tests.

- `medium` — `run()` is monolithic, so its five startup guards are unreachable from tests (0%) — `media-service/main.go:45`, `media-service/main.go:48`
  `EID_CACHE_CAPACITY <= 0` / `EID_CACHE_TTL <= 0` are fail-fast guards for a value that is passed straight to `lru.NewLRU` (`cache.go:29`); `config_test.go:183` only calls `cfg.Pool.Validate()` / `cfg.Guard.Validate()` directly and never the composed check. Extracting a `validate(cfg) error` would make all five testable without touching wiring.

- `low` — `handler_test.go` is 25 near-identical single-scenario functions over two handlers rather than table-driven, contrary to "prefer table-driven tests when testing multiple input/output variations of the same logic" — `media-service/handler_test.go:107-344`
  Names (`TestEndpoint1_…`, `TestEndpoint2_…`) also don't follow the `Test<Type>_<Method>_<Scenario>` convention and don't say which handler is under test.

- `low` — `minioBlobStore` error paths untested despite being trivially fakeable — `media-service/minio.go:41`, `media-service/minio.go:47`
  `client` is the `minioutil.ObjectStore` **interface**, yet the only tests are integration happy paths (`integration_test.go:111,131`, `emoji_integration_test.go:108`). The `GetObject` failure, the non-`NoSuchKey` `Stat` failure, and the `PutObject`/`RemoveObject` wrap paths are all uncovered — a unit fake would close them with no container.

- `low` — the "missing principal" defensive branches in both role middlewares are uncovered — `media-service/middleware_auth.go:68`, `media-service/middleware_auth.go:91` (and `sessionFromContext`'s nil return, `:20`)
  These fire only if a route is registered without `requireSession`; a `registerRoutes` mis-wiring is exactly what they exist to catch, and `TestRegisterRoutes_AuthWiring` (`middleware_auth_test.go:188`) doesn't reach them.

- `nitpick` — real-time `time.Sleep(60ms)` drives the TTL-expiry assertion — `media-service/cache_test.go:115`
  Not goroutine synchronization (the singleflight tests use channels correctly), but it is load-flaky in the same way as the two `pkg/` tests flagged in GLOBAL_PREP.

Compliance is otherwise clean: one `TestMain` calling `testutil.RunTestsWithPrewarm` (`integration_test.go:20`), all containers from `pkg/testutil` with no inline `testcontainers.GenericContainer`, `//go:build integration` on both integration files, tests in `package main`, per-test `gomock.NewController(t)` with no package-level mutable state, generated `mock_store_test.go` unedited, `blobStore` injected as an interface so no unit test touches real infra.

### Recommendations
- `high` — Close the floor with the two integration gaps first: add `TestMongoStore_UserByAccount` and `TestMongoStore_RoomMember` to `integration_test.go` (hit/miss + inactive-user), which also validates the projections behind the membership check.
- `medium` — Add a `validateEmojiImage` unit table covering a GIF/PNG whose decoded bounds exceed `maxDim` while the header claims smaller, plus a truncated body that passes `DecodeConfig` but fails `Decode`.
- `medium` — Extract `validateConfig(cfg *config) error` from `main.go:36-56` and table-test all five guards; `run()` then holds only wiring.
- `low` — Add `minio_test.go` with a fake `minioutil.ObjectStore` covering `GetObject` error, `Stat` non-`NoSuchKey` error, `NoSuchKey` → `errBlobNotFound`, and `PutObject`/`RemoveObject` failures (~18 stmts, no container).
- `low` — Collapse `handler_test.go`'s room- and account-avatar cases into two tables keyed on scenario name, and rename `TestEndpoint1/2_*` to `TestHandler_HandleAccountAvatar_*` / `TestHandler_HandleRoomAvatar_*`.
- `low` — Cover the nil-principal branches by mounting `requireAdmin()`/`requireBotSelfOrAdmin()` on a bare `gin.New()` without `requireSession` and asserting the 500 envelope.
- `nitpick` — Replace the `cache_test.go:115` sleep with a 1ns TTL plus an explicit `lru.Get` miss assertion, or drive expiry via an injectable clock.

---

## 5. Maintainability — 3 / 5

Functions are small, naming is consistent and the comments are genuinely WHY-oriented, but the two upload paths and the string-keyed telemetry contract are copy-paste surfaces that have *already* diverged, and route paths are duplicated across six sites.

### Findings
- `medium` — The two upload handlers duplicate ~60 lines of identical shape (name validate → `MaxBytesReader`+`ReadAll` → image decode → `blobs.Put` → store upsert → etag/contentType/size/updatedAt response) and have already diverged on hardening: `emoji_upload.go:61-80` does a `image.DecodeConfig` header pre-check (decompression-bomb guard) plus a decoded-bounds cap, while `upload.go:66-70` does a bare `image.Decode` with no pre-check and no dimension cap at all — `upload.go:30`.
  A hardening fix landed on one path and never reached the sibling; nothing in the code links them.
- `medium` — The access-log telemetry contract is 35 raw `c.Set("media_kind"/"media_outcome", …)` string literals with no constants and no closed set of outcome values (`handler.go:51-208`, `emoji_serve.go:25-78`, `emoji_upload.go:94-164`). `HandleBotUpload` (`upload.go:30`) and `HandleDriveMembers` (`drive.go:54`) set neither, so `accessLogMiddleware` emits empty `media_kind`/`media_outcome` for those two routes — `middleware.go:57`.
  The contract is unenforceable by the compiler; a new handler silently opts out.
- `medium` — Route paths are duplicated between registration and URL construction: `/api/v1/avatar/room/`, `/api/v1/avatar/`, `/api/v1/emoji/` appear in `routes.go:13-20` and again at `handler.go:118`, `handler.go:139`, `handler.go:179`, `emoji_serve.go:43`, `emoji_upload.go:38` — `routes.go:13`.
  `emojiImagePath` is persisted into every emoji doc's `imageUrl`, so a route rename desyncs stored data as well as live redirects.
- `low` — `handler.go` is two responsibilities in one file: the shared media-serving helpers (`setImageCacheHeaders`, `serveDefault`, `serveStored`, `redirectCrossCluster` — also used from `emoji_serve.go:59`) and the avatar handlers themselves — `handler.go:41-110`.
- `low` — A second, hand-rolled error envelope (`{success, error, errorType}` plus four `errType*` constants) lives beside the repo-standard `errcode`/`errhttp` used by every other handler, with no in-code note of why (the rationale exists only in `docs/superpowers/specs/2026-07-17-drive-members-endpoint-design.md`) — `drive.go:32-48`.
- `low` — Dead field: `blobInfo.ETag` is populated on every blob read but never consumed — all ETag consumers read the Mongo doc's `etag` (`handler.go:75`, `emoji_serve.go:59`) — `minio.go:51`.
- `nitpick` — `newHandler(store, store, blobs, &cfg)` passes the same value for two adjacent interface params; a future third store makes an argument-order mistake compile cleanly — `main.go:93`.
- `nitpick` — `run()` is 112 lines covering config validation, four client connections, two routers, HTTP server and shutdown; `EIDCacheCapacity`/`EIDCacheTTL` are validated inline while `Pool`/`Guard` use `.Validate()` — `main.go:46-57`.
- `nitpick` — The service has outgrown the single-`handler.go` layout in CLAUDE.md §1 (eight production handler/helper files at the flat level) without taking the sanctioned sub-package escape hatch — `media-service/`.

### Recommendations
- `medium` — Extract a shared `serveUpload` helper (or a small `uploadSpec{maxBytes, maxDim, allowedFormats, key, persist}` struct) covering read-cap → validate-image → `blobs.Put` → persist → respond, and route both `HandleBotUpload` and `HandleEmojiUpload` through it. Make `validateEmojiImage` the single image validator (parameterised by allowed formats and an optional dimension cap) so the avatar path inherits the `DecodeConfig` pre-check.
- `medium` — Replace the `media_kind`/`media_outcome` literals with typed constants and a single `setOutcome(c, kind, outcome)` helper in one file; call it from `HandleBotUpload` and `HandleDriveMembers` too so every route populates the access log.
- `medium` — Define the six route paths once as package constants (or path-builder funcs `avatarRoomPath(roomID)`, `emojiPath(shortcode)`) and use them from both `registerRoutes` and every redirect/`imageUrl` construction site.
- `low` — Split `handler.go` into `serve.go` (cache headers, `serveDefault`, `serveStored`, `redirectCrossCluster` — the helpers emoji also uses) and `avatar.go`'s handler half, so the shared media layer is visibly shared rather than incidentally living next to the avatar handlers.
- `low` — Add a one-line `// drive.members keeps a bespoke envelope for the external drive client; see docs/…-drive-members-endpoint-design.md` above `writeDriveError`, or migrate it to `errhttp` if the external contract permits.
- `low` — Delete `blobInfo.ETag`, or start using it as the `serveStored` validator; carrying an unread field invites a future reader to trust it.
- `nitpick` — Move the `EIDCache*` checks into a `func (c *config) validate() error` alongside `Pool.Validate()`/`Guard.Validate()`, and pull the Gin/HTTP wiring out of `run()` into `newHTTPServer(cfg, h, sessions, sdk)`.

---

## 6. Integration — 4 / 5

Cross-service contracts are clean where the strict rules bind — subject builders used exclusively, docs and both derived views match the emoji RPCs, and the service publishes no events at all — but the undocumented, non-standard `drive.members` endpoint and the missing MinIO bucket fail-fast leave real integration gaps.

**docs/client-api.md obligation: BINDS.** `media-service` registers two `chat.user.…` handlers — `chat.user.{account}.request.emoji.{siteID}.list` and `.delete` (`media-service/emoji_nats.go:69-70` via `pkg/subject/subject.go:1068,1081`). Both are documented (`docs/client-api.md:6147-6248`) and both derived views are in sync (`docs/client-api/request-reply.md:2201-2246`); `docs/client-api/events.md` correctly has no entry (no events emitted). Reason strings match the `errcode` constants exactly (`emoji_delete_disabled`, `emoji_shortcode_reserved`, `wrong_cluster`, `not_admin` — `pkg/errcode/codes_emoji.go:8,12`, `codes_avatar.go:7`, `codes_admin.go:5`). Federation rules (OUTBOX partition, INBOX lanes, `Timestamp` at publish sites, `BOOTSTRAP_STREAMS`, msgbucket, `ROOM_KEY_RETIRED_TTL`) are **vacuous here**: no `Publish`, no JetStream, no stream config (`grep Publish|jetstream` over `media-service/*.go` → only `natsmetrics` and `idgen.ResolveRequestID`).

### Findings
- `medium` — MinIO bucket is never probed at startup; `newMinioBlobStore` binds the name blind — `media-service/minio.go:34`, `media-service/main.go:82-86`
  `pkg/minioutil/minio.go:128` explicitly documents `BucketExists`/`NewBucket` as "the bucket-scoped fail-fast hook", and no service in the repo calls it. A wrong `MINIO_BUCKET` starts cleanly, `/healthz` returns 200, and every avatar/emoji `PUT` and stored-blob `GET` 500s at runtime — the exact "fail fast on config" case CLAUDE.md §Configuration calls for.
- `medium` — `GET /api/v1/drive.members` is a client-facing HTTP endpoint absent from `docs/client-api.md` §7 — `media-service/routes.go:15`, `media-service/drive.go:54`
  §7 documents the other four routes (avatar GET ×2, avatar PUT, emoji GET/PUT); this one is missing entirely. The strict CLAUDE.md rule covers only `chat.user.*` and auth-service HTTP, so this is a convention gap rather than a rule breach — but it is an unauthenticated endpoint exposing room name/type and user identity to an external "drive" integration with no written contract.
- `medium` — `drive.members` bypasses `pkg/errcode`/`errhttp` with a bespoke `{success, error, errorType}` envelope — `media-service/drive.go:32-48,60-95`
  CLAUDE.md: "Use `pkg/errcode` for ALL client-facing errors; reply via … `errhttp.Write`". These four `errorType` strings (`MISSING_PARAMETER`, `ROOM_NOT_FOUND`, `ACCOUNT_NOT_FOUND`, `INTERNAL_ERROR`) are a second, undocumented error vocabulary that no `codes_*.go` file owns. If it exists to match a legacy consumer, that needs an inline justification.
- `medium` — stale cross-service contract claim: `custom_emojis` is documented as read by the `pkg/emoji` validator, which does not read Mongo — `pkg/model/custom_emoji.go:6-7`, `media-service/store.go:36-37`
  `pkg/emoji/emoji.go` exposes only `Canonicalize`/`CanonicalizeReaction`/`IsStandard` — no collection access — and reactions validate format only (`docs/client-api.md:3916`, `history-service/internal/service/reactions_test.go:117`). The comments overstate the blast radius of `emoji.delete` and would mislead the next change to the collection.
- `low` — ETag wire inconsistency: stored ETags are unquoted, the generated default is quoted — `media-service/handler.go:46-47`, `media-service/avatar.go:530`, `media-service/minio.go:59`
  minio-go strips the quotes (`minio-go/v7@v7.2.0/utils.go:48-51`), so the `ETag` response header for stored blobs is not the RFC 9110 quoted-string form, while `defaultETag` emits `"v1-…"`. `If-None-Match` echo still matches, but a normalizing CDN/proxy can break it. The docs mirror the split: bot-upload shows `"etag": "\"9b2cf...\""` (`docs/client-api.md:7089`) while emoji upload/list show unquoted (`:7166`, `:6183`) — same code path, two documented shapes.
- `nitpick` — `Avatar` lookup fetches the whole document with no projection — `media-service/store_mongo.go:109`
  Every other find in the file projects precisely (`:39,55,71,84,96,142,155,203`).
- `nitpick` — `avatars`/`custom_emojis` `_id`s are deterministic composites (`"bot:"+account`, `"{siteID}:{shortcode}"`) rather than `pkg/idgen` — `media-service/upload.go:82`, `media-service/emoji_upload.go:240`
  Defensible (same rationale as `BuildDMRoomID`: no separate dedup needed) but neither entity appears in CLAUDE.md's primary-key enumeration.

### Recommendations
- `medium` — Probe the bucket at startup: call `client.BucketExists(ctx, cfg.MinioBucket)` in `run()` right after `minioutil.Connect` and return a wrapped error, or widen `minioutil.NewBucket` to cover binary blobs so the hook is actually used.
- `medium` — Add a `### GET /api/v1/drive.members` subsection to `docs/client-api.md` §7 (query params `roomId`/`accountName`, the `{success,data:{members,count,roomName,roomType}}` body, the four `errorType` values) and note its auth model, or restrict it to an internal listener.
- `medium` — Either migrate `drive.members` to `errhttp.Write` + `errcode`, or add an inline comment at `drive.go:32` naming the legacy consumer that pins the bespoke envelope.
- `medium` — Correct the `custom_emojis` reader comments in `pkg/model/custom_emoji.go:6-7` and `media-service/store.go:36-37`: the collection currently has no in-repo reader besides media-service.
- `low` — Pick one ETag shape (quote on emit, in `setImageCacheHeaders`) and fix the bot-upload example at `docs/client-api.md:7089` so the two upload endpoints document the same format.
- `nitpick` — Add the projection to `store_mongo.go:109` (`minioKey`, `etag`, `contentType`).

---

## 7. Performance — 3 / 5

Streaming, ETag/304 and the singleflight LRU are done well, but public cache headers leak onto error responses, avatar upload decodes untrusted images with no dimension pre-check, and the two hottest `<img src>` paths hit MongoDB uncached on every request.

### Findings
- `high` — bot-avatar upload calls `image.Decode` with no `image.DecodeConfig` dimension pre-check, so a small compressed PNG declaring huge dimensions allocates the full pixel buffer before any bound is applied — `media-service/upload.go:66`
  `MaxBytesReader` caps the *body* at 1 MiB (`config.go:79`) but not the decoded raster. The emoji path already does this correctly in two phases (`emoji_upload.go:64-70`) with an explicit decompression-bomb comment; the avatar path has no `MAX_DIMENSION` knob at all.
- `high` — `Cache-Control: public, max-age=21600` + `ETag` are written *before* the blob fetch, so a MinIO 500 or a not-found inherits them and becomes a shared-cache-storable error — `media-service/handler.go:75` then `:86-89`; same shape at `media-service/emoji_serve.go:59` then `:66-76`
  A single transient MinIO blip is then pinned in CDN/browser caches for 6 h per key. `serveDefault`'s cacheable 404 is deliberate (`handler.go:52-59`); these are not.
- `medium` — `Avatar` is the one find in the service with no projection, fetching the whole avatars doc when only `MinioKey`/`ETag` are read — `media-service/store_mongo.go:109`
  CLAUDE.md §6 MongoDB: "every find/aggregation MUST specify an explicit projection". Every other query here projects precisely.
- `medium` — no server-side cache on the avatar/emoji hot paths: an uncached room-avatar GET costs two sequential Mongo round trips (`RoomSite` at `media-service/handler.go:125`, then `Avatar` at `media-service/handler.go:147`) plus a MinIO `Stat`; the emoji GET costs `EmojiDoc` + `Stat` per image — `media-service/emoji_serve.go:47,65`
  Only the account→employeeId lookup is cached (`cache.go:22-33`). A room list of N rooms is 2N Mongo finds on any cold browser cache; these docs are as near-immutable as the eid mapping and want the same LRU+TTL+singleflight treatment.
- `medium` — `srv.WriteTimeout = 30 * time.Second` (`media-service/main.go:119`) directly contradicts the comment three lines above it claiming there is "no blanket HTTP timeout … a short deadline would cancel a slow up/download mid-stream" — `media-service/main.go:107-110`
  `WriteTimeout` is exactly that blanket deadline: it spans the whole `c.DataFromReader` body write, truncating a slow mobile client mid-blob.
- `medium` — uploads are stored and served at original resolution with no downscale or thumbnail, although every consumer renders 120 px (`avatar.go:63` viewBox 120, `avatar.go:79` `_120.JPG`) — `media-service/upload.go:75`
  A 1 MiB original ships in full on every cache miss; a single 120 px derivative written at upload time would cut avatar egress by ~an order of magnitude.
- `low` — `HandleEmojiList` returns the site's entire `custom_emojis` collection per RPC with no pagination, no `Limit`, and no cached copy — `media-service/emoji_nats.go:19`, `media-service/store_mongo.go:152-165`
  Mitigated: the `(siteId, shortcode)` unique index (`store_mongo.go:130-133`) covers the sort, and the projection is tight. It is a per-login fan-in that grows unboundedly with the emoji set.
- `low` — `drive.members` issues three sequential Mongo round trips; `UserByAccount` and `RoomMember` are independent and could run concurrently — `media-service/drive.go:68,79,90`
- `low` — the singleflight loader detaches with `context.WithoutCancel(ctx)` and adds no deadline of its own — `media-service/cache.go:50`
  `MONGO_SERVER_SELECTION_TIMEOUT` (2 s default) bounds selection only, not a socket read on an already-selected stalled server, so the `DoChan` goroutine can park indefinitely.
- `nitpick` — no JetStream consumers in this service, so the `pkg/jsretry` / `BackOff` / `MaxAckPending` rules are N/A; no bare `Nak()`/`NakWithDelay(0)` exists to flag. Blob serving correctly streams via `c.DataFromReader` with a real `Content-Length` (`handler.go:93`, `emoji_serve.go:79`) rather than buffering — `media-service/minio.go:38-52`

### Recommendations
- `high` — Add `image.DecodeConfig` + a `MAX_DIMENSION` bound to `HandleBotUpload` before `image.Decode`, mirroring `validateEmojiImage`; extract that function so both paths share one guard.
- `high` — Move `setImageCacheHeaders` to *after* a successful `blobs.Get`, and on the error/404 paths emit `Cache-Control: no-store` (keep the deliberate cacheable 404 in `serveDefault`).
- `medium` — Add the missing projection `{minioKey:1, etag:1, contentType:1}` to `mongoStore.Avatar`.
- `medium` — Generalize `eidCache` into a small TTL-LRU+singleflight used for `RoomSite`, `Avatar` and `EmojiDoc`; keep the TTL well under `CACHE_MAX_AGE_SECONDS` so an avatar change still propagates within the browser-cache window.
- `medium` — Either drop `WriteTimeout` and rely on the per-request context (matching the stated intent) or fix the comment; if kept, set it from the largest expected blob ÷ worst-case client bandwidth.
- `medium` — Generate and store a 120 px derivative at upload time and serve it from the GET paths, keeping the original only as a source.
- `low` — Give the detached singleflight fetch its own `context.WithTimeout`, and run `UserByAccount`/`RoomMember` concurrently in `HandleDriveMembers`.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Set cache headers only after a successful blob fetch; emit `no-store` on error paths** | Performance | `handler.go:75` then `:86-89`; same shape at `emoji_serve.go:59` then `:66-76` | `Cache-Control: public, max-age=21600` + `ETag` are written **before** the fetch, so a MinIO 500 or a not-found **becomes a shared-cache-storable error** — a CDN can serve a broken avatar to every user for six hours. Both hot `<img src>` paths have it. |
| 2 | `high` | **Add a `DecodeConfig` dimension pre-check to the bot-avatar upload** | Performance | bare `image.Decode` at `upload.go:66`; **the guard exists one file over** at `emoji_upload.go:61-80` | A small compressed PNG declaring huge dimensions **allocates the full pixel buffer before any bound is applied.** The emoji path has both a header pre-check and a decoded-bounds cap; the avatar path has neither — the two upload handlers were copy-pasted and have since diverged **on exactly the hardening that matters** (see item 6). |
| 3 | `high` | **Raise coverage from 70.0%** | Test coverage | under the 80% floor; `store_mongo.go:27`; `run()` 0% at `main.go:45`, `:48` | Almost the entire deficit is the Mongo/MinIO layers and `run()`'s five startup guards — which are **untested decisions, not pure wiring.** Start with `UserByAccount` and `RoomMember` (`store_mongo.go:81`, `:94`), which have **no integration test at all**: their filters and projections are only ever mocked. |
| 4 | `medium` | **Gate `emoji.delete` on admin, matching the HTTP route** | Architecture | HTTP upload admin-gated at `routes.go:20`; NATS RPC open to any authenticated account at `emoji_nats.go:37-43`, guarded only by a kill-switch | **Authorization is asymmetric across transports for the same resource.** Any authenticated user can delete a custom emoji over NATS that only an admin may upload over HTTP. |
| 5 | `medium` | **Scope CORS, and drop `Access-Control-Allow-Origin: *` from the authenticated routes** | Architecture | hardcoded at `middleware.go:29-42`; applied to the two authenticated PUTs at `routes.go:19-20`; `x-auth-token` in allowed headers | A wildcard origin on credential-bearing routes with the auth header explicitly allow-listed. Browsers block credentialed wildcard requests, which is the only reason this is `medium` — but it is a header configuration that says the opposite of the service's intent. |
| 6 | `medium` | **Factor the two upload handlers into one shared path** | Maintainability | ~60 identical lines: name validate → `MaxBytesReader`+`ReadAll` → decode → `blobs.Put` → upsert → response; divergence at `emoji_upload.go:61-80` vs `upload.go:66-70` | **The duplication is the mechanism of finding 2.** Fixing item 2 by hand leaves the next hardening step to diverge again; factoring makes the pre-check structurally shared. |
| 7 | `medium` | **Make the `(siteId, shortcode)` index a hard requirement** | Architecture | created best-effort with a warning at `main.go:78-80`; `UpsertEmoji`'s duplicate-key retry **assumes it exists** at `store_mongo.go:189-193` | A correctness path depends on an index the service will happily start without. If creation fails, the retry logic silently stops protecting anything. |
| 8 | `medium` | **Document `drive.members` and route it through `pkg/errcode`** | Integration / Arch | `routes.go:15`, `drive.go:54`; bespoke `{success,error,errorType}` envelope at `drive.go:32-48`; logs-then-writes at `:70-71`, `:81-82`, `:92-93` | A client-facing HTTP endpoint **absent from `docs/client-api.md` §7**, with its own four-constant error taxonomy that no client library knows, and a log-and-return on every failure path. |
| 9 | `medium` | **Add a projection to `Avatar`, and cache the hot serve paths** | Performance | `store_mongo.go:109` is the one find with no projection; two sequential round trips per room-avatar GET at `handler.go:125`, `:147`, plus a MinIO `Stat` | `Avatar` fetches the whole document to read `MinioKey`/`ETag` — the one place the service breaks its own otherwise-exemplary projection discipline. And the two hottest `<img src>` paths hit MongoDB uncached on **every** request. |
| 10 | `medium` | **Resolve the `WriteTimeout` contradiction** | Performance | `srv.WriteTimeout = 30s` at `main.go:119`, three lines below a comment at `:107-110` claiming there is "no blanket HTTP timeout … a short deadline would cancel a slow up/download mid-stream" | The code and its own justification directly contradict each other on a media service's central constraint. Whichever is right, one of them is currently lying to the next reader — and if the comment is right, large uploads are being severed. |

**Also worth doing.** Probe the MinIO bucket at startup — `newMinioBlobStore` binds the name blind (`minio.go:34`, `main.go:82-86`), so a missing bucket is a per-request failure rather than a startup one. Fix the two bare `return err` in startup validation (`main.go:53`, `:56`), and stop rejecting a cross-site session with `AdminNotAuthorized` (`middleware_auth.go:47`) — that is the same wire reason emitted for a genuine role failure, so a client cannot tell a federation problem from a permissions one. Guard the client-facing message that interpolates a possibly-empty base URL (`upload.go:51`). Replace the 35 raw `c.Set("media_kind"/"media_outcome", …)` string literals with constants and a closed outcome set — two routes (`HandleBotUpload`, `HandleDriveMembers`) set neither, so the access log emits empty fields for them. De-duplicate the route paths that appear in `routes.go` and again at five construction sites. Exercise the decompression-bomb layer of `validateEmojiImage` (`emoji_upload.go:76`). Correct the stale claim that `custom_emojis` is read by the `pkg/emoji` validator (`pkg/model/custom_emoji.go:6-7`) — it does not read Mongo. And consider serving a downscaled variant: uploads are stored and served at original resolution although every consumer renders 120 px.
