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

Notable strengths, verified: no `fmt.Println`/`log.Println` anywhere; no error comparison by string; `errors.Is(err, mongo.ErrNoDocuments)` at all seven miss-expected sites; a proper sentinel + `errors.Is(err, errBlobNotFound)` across the blob boundary (`minio.go:15`, `handler.go:82`); an explicit projection on every single find (`store_mongo.go:39,55,71,84,96,142,155,203`) and no `$lookup`; no `os.Getenv`; shared knobs mounted as named fields (`config.go:68,110`), not re-declared.

### Recommendations
- `medium` — wrap both startup validations: `fmt.Errorf("validate mongo pool config: %w", err)` and `fmt.Errorf("validate nats guard config: %w", err)` at `main.go:53,56`.
- `medium` — add an `AvatarSessionWrongSite` (or similar) `Reason` to `pkg/errcode/codes_avatar.go` and use it at `middleware_auth.go:47`, leaving `AdminNotAuthorized` for the two genuine role checks.
- `medium` — at `upload.go:49`, branch on `base == ""` and emit a message without the dangling URL (e.g. `fmt.Sprintf("bot is owned by cluster %s", siteID)`), mirroring the read path's handling at `handler.go:103`.
- `low` — promote `media_kind`, `media_outcome`, `request_id`, `bot_account` to consts beside `ctxSessionKey`, and set kind/outcome in `HandleBotUpload` and `HandleDriveMembers` so the access log is complete for all seven routes.
- `low` — give the detached load in `cache.go:50` its own bounded deadline (`context.WithTimeout(context.WithoutCancel(ctx), …)`) so the goroutine has a guaranteed exit independent of driver behaviour.
- `low` — add a one-line WHY comment at `drive.go:45` naming the external Drive contract and linking the design spec, so the errcode bypass reads as sanctioned rather than as drift.
- `low` — re-run `govulncheck` and the semgrep registry packs in an environment with egress before treating this service's SAST posture as fully verified.
