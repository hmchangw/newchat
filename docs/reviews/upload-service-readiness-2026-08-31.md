# upload-service — Production Readiness Review

**Service:** `upload-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.0 / 5

A clean, conventional Gin service with genuinely strong streaming and projection discipline — the multipart body never lands in memory, sniff and decode read only headers, both Mongo queries are precisely projected, and the WHY-comments are unusually good. `handler.go` is at 92%, `middleware.go` 98.9%, `mediatype.go` 98.3%.

**It also carries the fleet's one `critical` security finding.** `drive_host` is taken **verbatim from the client query string** and used as the upstream base URL — with the Drive `api-token` header attached. A client picks the host that receives the service's credential. Two more findings compound the exposure: **`resolveMediaType` returns the client-declared Content-Type whenever it is anything but empty or `application/octet-stream`**, so the byte-sniff, `looksLikeSVG` check and extension logic **never run for a lying client** — contradicting the function's own security comment; and **no request-body cap exists**, so `c.MultipartForm()` spools the entire body to the OS temp dir, for up to 15 minutes, before any size limit is consulted.

Coverage is 76.5%, below the floor, with `run()` at 0/70 — and `run()` holds the DEV_MODE/OIDC required-config guard.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 7 | 21 | 17 | 7 | **53** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-wrapped, correctly-tiered `errcode` usage with unusually good WHY-comments; docked one point for a media-type resolution path that contradicts its own security comment and for collapsing Drive 404s into 503s.

### Findings
- `medium` — `resolveMediaType` returns the client-declared Content-Type whenever it is anything but empty or `application/octet-stream`, so the byte-sniff, `looksLikeSVG` and extension logic never run for a lying client — `upload-service/mediatype.go:473`
  Directly contradicts the handler's own comment at `upload-service/handler.go:268-277` ("the real type comes from the bytes and the name … what stops a blacklisted upload arriving under a generic label"). A POST declaring `Content-Type: image/png` on SVG bytes is recorded as `image/png` and sails past the `FILE_UPLOAD_MEDIA_TYPE_BLACKLIST=image/svg+xml` default. Every SVG-defence case in `mediatype_test.go:242-258` declares `application/octet-stream`; the lying-declared case is untested.

- `medium` — all `GetGroupImage` failures collapse to `errcode.Unavailable` (503), including Drive's 404 — `upload-service/handler.go:358-362`
  `pkg/drive/uploader.go:169` returns a bare `fmt.Errorf("image not found")` with no sentinel, so the caller *cannot* distinguish it via `errors.Is` and the only alternative would be string comparison, which CLAUDE.md forbids. A missing legacy image therefore pages ops as an upstream outage instead of returning 404 to the client. Fix belongs in `pkg/drive` (export `ErrImageNotFound`), then map it here.

- `medium` — `NewHandler` takes 11 positional parameters, four of them adjacent same-typed scalars and two of them the *same interface type* at opposite ends of the list — `upload-service/handler.go:72-73`
  `maxImages, maxAttachments int` and `maxImageSize, maxFileSize int64` are silently swappable, and `dc driveClient` (param 2) vs `legacyDrive driveClient` (param 11) means transposing the primary and legacy Drive backends compiles cleanly and serves the wrong backend. `broadcast-worker`, `message-worker` and `inbox-worker` already use the `options ...handlerOption` pattern for exactly this.

- `low` — `err != http.ErrServerClosed` uses direct comparison rather than `errors.Is` — `upload-service/main.go:228`
  `media-service/main.go:143`, `botplatform-service/main.go:160`, `admin-service/main.go:174` and `user-service/main.go:392` all use `errors.Is`; this service is on the older minority form.

- `low` — two `Close` errors discarded with no comment, against CLAUDE.md "never ignore errors silently — comment if intentionally discarded" — `upload-service/handler.go:177`, `upload-service/store_minio.go:39`
  Both are defensible (read handles on cleanup / error paths), which is exactly why the one-line justification is cheap.

- `low` — `logCtx` re-attaches `request_id` to the context in all five handlers, though `requestIDMiddleware` already owns that ID and already rewrites `c.Request`'s context — `upload-service/handler.go:91` vs `upload-service/middleware.go:58-68`
  CLAUDE.md's Tier-3 note says handlers should get request-id logging "for free from the router middleware". One `errcode.WithLogValues` in `requestIDMiddleware` would remove the per-handler call and the risk of a future handler forgetting it. (The factoring into `logCtx` is still an improvement on the inline duplication in `media-service`/`botplatform-service`.)

- `low` — SAST audit-coverage gap, environmental not a service defect: gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean, but `govulncheck` and the semgrep registry packs could not run (egress 403 per `GLOBAL_PREP.md`). No dependency-CVE assurance for this service's `minio-go`/`resty`/`gin` tree.

- `nitpick` — the `roomId is required` / `fileId is required` guards are unreachable through the registered routes (Gin never matches an empty `:param` segment) and have no test — `upload-service/handler.go:138`, `:216`, `:333`, `:384`.

- `nitpick` — `errIsUploadNotFound` is a one-line wrapper over `errors.Is` used at a single call site — `upload-service/store.go:294`, called at `handler.go:397`.

- `nitpick` — the `upload` read DTO carries `bson` tags only, deviating from CLAUDE.md's "all model structs get both `json` and `bson` tags" — `upload-service/store.go:269`. The inline comment documents the reason (never serialized to clients) and the rule reasonably targets `pkg/model`; noted for completeness, not for action.

### Recommendations
- `medium` — Reverse the precedence in `resolveMediaType`: sniff and extension should decide, with the declared type used only as a tiebreak for inconclusive sniffs. At minimum, run `mimeFilter.allowed` against *both* the declared and the resolved type. Add the missing table case (`declared: "image/png"`, `data: svgBytes`) to `mediatype_test.go` first — it currently passes for the wrong reason.
- `medium` — Export `drive.ErrImageNotFound` in `pkg/drive/uploader.go:169` and branch on it in `downloadFrom` to return `errcode.NotFound`, keeping `Unavailable` for genuine upstream failures.
- `medium` — Convert `NewHandler`'s seven scalar/config parameters to a `handlerOption` functional-option set (or a single `handlerConfig` struct), keeping only `store`, `drive`, `legacyDrive` and `s3` positional. This also removes the two-`driveClient` transposition hazard.
- `low` — Switch `main.go:228` to `errors.Is(err, http.ErrServerClosed)`.
- `low` — Move `errcode.WithLogValues(ctx, "request_id", …)` into `requestIDMiddleware` and delete `logCtx`; handlers then use `c.Request.Context()` directly.
- `low` — Add the required one-line justification comments to the two discarded `Close()` calls.
- `low` — Re-run `make sast-vuln` and the semgrep registry packs from an environment with egress before merge; this audit could not clear the dependency tree.

---

## 3. Architecture — 4 / 5

Clean, conventional Gin service: consumer-owned store interface, constructor DI, correct file layout and shutdown order; the gaps are config fail-fast on the Drive dependency and a few boundary/knob-ownership slips, none structural.

### Findings
- `high` — Neither Drive backend's URL or API token is `required`/`notEmpty`, and nothing validates them at startup: `pkg/drive/config.go:12-13` declares `env:"URL"` / `env:"API_TOKEN"` with no marker, and `upload-service/main.go:103-105` mounts two instances (`DRIVE_`, `LEGACY_DRIVE_`) then builds clients at `main.go:145-146` without a check. A pod with an empty `DRIVE_URL` starts healthy, passes `/healthz`, and fails every upload/download at request time. Contrast `BOTPLATFORM_URL` (`main.go:90`) and `cfg.Pool.Validate()` (`main.go:120`) — the fail-fast pattern exists in this same file and Drive is the hole.
- `medium` — `LoadBaseURLs` fails open: a missing or malformed baseurls file only logs a warning and installs an empty map (`pkg/drive/config.go:26-39`), after which `GetBaseURLFromRoomOrigin` silently falls back to the default base URL (`pkg/drive/uploader.go:67-72`) for every cross-site room. Called unconditionally at `upload-service/main.go:125-126`. Also the only file-based config in the service, against CLAUDE.md §6 "All config from environment variables — no config files".
- `medium` — MinIO knobs are re-declared per service rather than owned by `pkg/minioutil`: `MINIO_DOWNLOAD_TIMEOUT` appears with identical tag+default in `upload-service/main.go:101` and `client-update-service/config.go:24`, and `MINIO_ENDPOINT`/`ACCESS_KEY`/`SECRET_KEY`/`USE_SSL` in those two plus `media-service/config.go:70-74`. CLAUDE.md §6 Configuration requires a shared knob be declared once in the owning package and mounted as a named field with `envPrefix` — exactly what `Pool mongoutil.PoolConfig` (`main.go:59`) and `Drive drive.Config` (`main.go:103`) already do here. `pkg/minioutil` has no `Config` type at all.
- `medium` — The object-storage boundary interface lives outside `store.go` and so is outside mockgen's reach: `objectStore` is declared at `handler.go:46-49`, while `//go:generate mockgen -source=store.go` (`store.go:265`) only covers `Store`. Tests consequently hand-roll `fakeS3` (`handler_test.go:69-75`) and `fakeDrive` (`handler_test.go:37-66`), which drift from the interface silently. `driveClient` (`handler.go:40-44`) has the same problem.
- `low` — `NewHandler` takes ten positional parameters, seven of them bare scalars (`handler.go:72-73`), and the call site is a three-line argument list (`main.go:175-177`). Two adjacent `int`s (`maxImages`, `maxAttachments`) and two adjacent `int64`s (`maxImageSize`, `maxFileSize`) are swappable without a compile error.
- `low` — Middleware ordering puts `corsMiddleware` ahead of `gin.Recovery()` and `requestIDMiddleware` (`main.go:183-189`), so a panic in the CORS path is unrecovered and the o11y server span at `main.go:186` is created before `request_id` exists, leaving spans uncorrelated with the access log's `request_id` (`middleware.go:76-84`).
- `low` — `http.Server` sets `ReadHeaderTimeout`/`ReadTimeout`/`WriteTimeout` but no `IdleTimeout` (`main.go:200-208`), so keep-alive connections are held by the Go default rather than an explicit bound on a service whose other timeouts are 15m.
- `low` — Mongo is wired without a circuit breaker (`main.go:137-139`), unlike peer services that mount `mongoutil.BreakerConfig` (`botplatform-service/config.go:22`, `notification-worker/main.go:62`). Every download does a blocking `MemberSiteID`/`GetUpload` in the request path (`handler.go:395`, `handler.go:435`).
- `nitpick` — A startup failure returns before any teardown runs (`main.go:227-230`), so `obsShutdown` never flushes the traces/logs describing why the process died.

Positives worth recording: `Store` holds exactly the two methods the handlers need and is defined in the consumer (`store.go:282-291`); `NewMongoStore` returns an unexported struct (`store_mongo.go:369`); routes are entirely in `routes.go` with `/healthz` outside the auth group; the MinIO reader correctly ties the timeout context's lifetime to `Close` via `cancelReadCloser` (`store_minio.go:340-350`) rather than leaking or cancelling mid-stream; outbound calls use `restyutil` (`main.go:191`) except the documented streaming-upload exception (`pkg/drive/uploader.go:41-49`); no NATS/JetStream surface, so the `BOOTSTRAP_STREAMS` and consumer-pattern rules do not apply.

### Recommendations
- `high` — Mark `pkg/drive/config.go` `URL` as `required,notEmpty` and `Token` as `required` (it is a secret), or add a `Config.Validate()` called beside `cfg.Pool.Validate()` in `main.go:120`.
- `medium` — Make `LoadBaseURLs` return an error and fail startup on a malformed file; keep the empty-map fallback only for a genuinely absent path.
- `medium` — Add `minioutil.Config` (endpoint, keys, SSL, download timeout) and mount it as a named field in upload-service, client-update-service and media-service, deleting the three duplicate declarations.
- `medium` — Move `objectStore` and `driveClient` into `store.go` so `make generate` produces their mocks, and delete `fakeS3`/`fakeDrive`.
- `low` — Replace `NewHandler`'s scalar tail with a `handlerOptions`/`limits` struct so the call site names each bound.
- `low` — Reorder to `Recovery → requestID → CORS → o11y → accessLog` so panics are caught and spans carry `request_id`.
- `low` — Set `IdleTimeout` on the server and mount `mongoutil.BreakerConfig` with an `UPLOAD_` prefix.

---

## 4. Test coverage — 2 / 5

Test *quality* is genuinely good — handler.go 92%, middleware.go 98.9%, mediatype.go 98.3%, generated mocks, no real infra in unit tests — but the service sits at 76.5%, under the CLAUDE.md Section 4 floor, which caps this dimension at 2.

### Findings
- `high` — Coverage is **76.5% (372/486 statements)**, below the 80% floor; per CLAUDE.md Section 4 this must not merge as-is — `coverage_by_service.txt:upload-service`
  Composition: `main.go` 0/70, `store_mongo.go` 0/15, `store_minio.go` 3/14, `handler.go` 185/201. Excluding `main.go` and the two integration-covered store files the service is 369/387 (95.3%) — the shortfall is concentrated, not diffuse.
- `medium` — `run()` is 0/70 statements (14.4% of the service) and is not pure wiring: it holds the DEV_MODE/OIDC required-config guard and the bucket-name default, both untested decisions — `upload-service/main.go:159-161`, `upload-service/main.go:151-154`
- `medium` — `HandleUploadFile`'s rejection branches are uncovered while the *identical* branches on the images twin are tested: no-user 500, blank-email 500, non-multipart 400, and `fh.Open` failure are all 0% — `upload-service/handler.go:221-224,227-230,238-243,262-265`
  The images equivalents exist at `handler_test.go:183`, `:193`, `:247` — the file endpoint is the one that runs MIME filtering, so its pre-Drive rejection path is the more important of the two.
- `medium` — The stated "no orphaned Drive file" invariant is unasserted: dimensions are read before the Drive upload specifically so a read failure cannot orphan a file, yet that error branch is 0% and no test proves `UploadGroupImages` is *not* called on it — `upload-service/handler.go:283-290`
  Same for the media-type resolver's error return (`handler.go:273-276`). Both are only reachable via a failing `Seek`, and the handler takes the reader straight from `fh.Open()` with no seam (`handler.go:261`), so as written they are structurally untestable — the reason they are 0%, not an oversight.
- `medium` — `preprocessFiles`' third rejection reason, "failed to open file", is 0% — `upload-service/handler.go:464-466`
  The other two per-file rejections are covered (size `handler_test.go:291`, extension `handler_test.go:270`); this one silently drops a file from a partial-success response, so its result-item shape is worth pinning.
- `low` — `matchMediaType`'s exact-equality fallback is unreachable, so its 80% can never rise — `upload-service/mediatype.go:86`
  `parseMediaTypes` routes only `*`, `*/*` and `type/*` into the wildcard slice (`mediatype.go:44-47`), and `matchSet` is the only caller, so `pattern == mime` cannot execute.
- `low` — The SSO `account` fallback to `claims.Name` when `PreferredUsername` is empty is 0% — `upload-service/middleware.go:194-196`
  Every downstream membership check keys on `user.Account`; nothing pins this derivation.
- `low` — `store_mongo.go` (0/15) and `store_minio.Open` (3/14) show 0% only because their tests are behind `//go:build integration` and excluded from the unit profile — `upload-service/integration_test.go:1-115`
  Those tests are well-formed (`TestMain` → `testutil.RunTests`, `testutil.MongoDB`/`testutil.MinIO`, not-found and missing-key paths both asserted). This is ~5.8 points of the headline gap that is not a real gap.
- `low` — `upload_stream_test.go` carries no `//go:build integration` tag but stands up a live loopback HTTP server and pushes 100 MiB through it under `-race`, asserting a 16 MiB peak sampled process-wide every 10 ms — `upload-service/upload_stream_test.go:85-105`, `pkg/testutil/memtest.go:76-89`
  A 10 ms sampler can miss a transient spike, so the guarantee is weaker than it reads, and it dominates the unit suite's runtime.
- `nitpick` — Many tests miss the `Test<Type>_<Method>[_<Scenario>]` convention: `TestUpload_MissingRoomID_400`, `TestDownload_NotMember_403` name neither type nor method — `upload-service/handler_test.go:173`, `:653`
  The same file's `TestHandler_HandleSetCookie_SetsCookieAttributes` (`:1218`) shows the intended form.

### Recommendations
- `high` — Mirror the four images-endpoint rejection tests onto `HandleUploadFile` (no-user, blank-email SSO caller, non-multipart, `fh.Open` failure). Copy-shape from `handler_test.go:183-257`; this alone recovers most of `handler.go`'s 16 uncovered statements.
- `medium` — Give `HandleUploadFile` a reader seam (accept the opened file through a small `func(*multipart.FileHeader) (multipart.File, error)` field on `Handler`, defaulted to `fh.Open`) so a failing-`Seek` reader can be injected. Then assert the real invariant: on a dimensions/resolve failure the response is 500 **and** `fakeDrive.uploadGot.n == 0` — no orphaned Drive file.
- `medium` — Extract `run()`'s two decisions into pure helpers — `requireOIDCConfig(cfg) error` and `bucketName(cfg) string` — and table-test them. Leave the rest of `run()` untested; connection wiring does not repay a test.
- `low` — Add a `preprocessFiles` subtest for the open-failure branch, asserting the item is `{Status: "failure", Error: "failed to open file"}` and is excluded from `fileHeaders`.
- `low` — Delete the unreachable `return pattern == mime` at `mediatype.go:86` rather than writing a test that cannot reach it; add a `middleware_test.go` case with `PreferredUsername: ""` and `Name: "alice"` to pin the account fallback.
- `low` — Move `upload_stream_test.go` behind `//go:build integration` (or a `-short` guard) so `make test` is not carrying a 100 MiB sampled memory assertion under `-race`.
- `nitpick` — Rename the `TestUpload_*` / `TestDownload_*` / `TestS3Download_*` families to `TestHandler_HandleUploadImages_*` etc. to match CLAUDE.md Section 4 naming.
