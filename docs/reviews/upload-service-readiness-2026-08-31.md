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
