# botplatform-service — Production Readiness Review

**Service:** `botplatform-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Genuinely well-written Go with narrow consumer-side interfaces, precise Mongo projections, a shared breaker, an L2 session tier, no goroutine leaks and no N+1 — and comments that say WHY. But it is also the service where the most CLAUDE.md rules are breached at once, and where three unbounded-work paths meet on an unauthenticated endpoint.

**The bot rate limiter runs *after* the auth middleware**, so an invalid token is never rate-limited — and because the session cache is positive-only by design, every bogus-token request is one uncapped MongoDB `FindOne`. Token spraying is an unmetered load generator against the same Mongo the breaker exists to protect. Alongside it, **`/api/v1/login` is unauthenticated, has no rate limiter, and runs a full bcrypt verify** (~50–100 ms CPU) per request, and **the idempotency middleware buffers the entire request body with an uncapped `io.ReadAll`.**

Structurally: three of the handler's five dependencies are **poked in after construction**, so every bot route nil-derefs if a wiring line is dropped; `BcryptCost` is parsed and range-validated but **never used by anything**; and the 15 s room-management budget is **unreachable** — the request deadline is 10 s and cuts it first. Coverage is 56.5%, below the critical line, with both federation forwarders and the cross-site routing decision at zero.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 14 | 18 | 20 | 8 | **61** |

---

## 2. Go code quality — 3 / 5

Genuinely well-written Go — narrow consumer-side interfaces, near-universal error wrapping, and a clean secret-handling record — but six distinct CLAUDE.md Section 3 rules are breached, including string-matching on error text and log-AND-return.

### Findings
- `high` — `bindStrict` classifies a decode failure by **matching error text**: `msg := err.Error()` then `strings.Contains(msg, "unknown field")` — `botplatform-service/bot_handlers.go:210-212`
  Direct breach of "Never compare errors by string." A stdlib message tweak silently reclassifies every unknown-field 400 as a generic `bot_content_invalid`, changing the wire `reason` clients branch on. `encoding/json` gives no typed error here, so the fix is to detect the unknown field before decode (decode to `map[string]json.RawMessage` and diff keys) or to accept the coupling explicitly with a test pinning the literal.
- `medium` — the idempotency middleware reads the request body with **no size cap**: `io.ReadAll(c.Request.Body)` — `botplatform-service/middleware_idempotency.go:60`
  It runs *before* the handler (`routes.go:39-47`), so the deliberate `botRequestBodyMaxBytes` cap in `bot_handlers.go:190-193` never applies on any of the five bot routes. Every request is fully buffered and SHA-256'd first. Wrap in `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` and map the overflow to the same 400.
- `medium` — log-AND-return: `denied` calls `slog.WarnContext` then `errhttp.Write` on the same error — `botplatform-service/handler.go:180-181`
  `errhttp.Write` → `errcode.Classify` already logs once (`pkg/errcode/errhttp/errhttp.go:13-16`), so every failed bot login emits two lines. Move `reason` into `errcode.WithLogValues(ctx, "reason", reason)` and drop the `slog` call.
- `medium` — dependencies injected by post-construction field poke, not by constructor: `h.subs`, `h.forwarder`, `h.dmEnsurer` — `botplatform-service/main.go:106,114,116`
  `newHandler` (`handler.go:46`) returns a handler that is *not usable*; three of five deps are nil until later statements run. Nothing compiles-checks the wiring, and a reordering ships a nil-panic on the first bot request. Widen `newHandler` to take all five.
- `medium` — Tier-1 drift: infra failures are dressed up as `errcode.Internal(..., WithCause(err))` at ~12 sites instead of raw `fmt.Errorf` — `botplatform-service/middleware.go:70,128,136,151`, `middleware_idempotency.go:55,62,74`, `bot_forwarder.go:84,141,153`, `dm_ensurer.go:63,72`, `bot_handlers.go:48`
  CLAUDE.md: "a raw wrapped error collapses to `internal` at the boundary; do NOT dress it up as an errcode." The same file knows the rule — `bot_handlers.go:185` and `:83` use raw `fmt.Errorf` correctly — so the codebase is internally inconsistent.
- `medium` — bare `err` returned from a function that did more than delegate — `botplatform-service/store_mongo.go:99`
  `DeleteSessionsBeyondCap` loses "which store operation" on the way up. (`InsertSession`:81 / `FindSessionByHash`:85 are pure pass-throughs; defensible, but the same rule reads on them.)
- `low` — sentinel compared with `!=` rather than `errors.Is`: `if err := dec.Decode(&struct{}{}); err != io.EOF` — `botplatform-service/bot_handlers.go:219`
- `low` — exported identifiers in `package main` that nothing can import: `BotplatformStore` (`store.go:14`), `BotSub` (`subscription_store.go:21`), `HandleLogin`/`HandleValidate`/`HandleHealth` (`handler.go:55,89,193`)
- `low` — dead config: `DevMode` is declared and never read anywhere in the service — `botplatform-service/config.go:57`
- `low` — SAST audit-coverage gap (environmental, per GLOBAL_PREP): gosec + repo-owned semgrep are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress). No dependency-CVE signal for this service.
- `nitpick` — `context.Context` is the second parameter, not the first: `func (h *handler) denied(c *gin.Context, ctx context.Context, ...)` — `botplatform-service/handler.go:178`
- `nitpick` — misleading variable name: `var unknownErr *json.SyntaxError` holds a *syntax* error, and the unknown-field case is handled ten lines below — `botplatform-service/bot_handlers.go:205`
- `nitpick` — typo in a doc comment: "Reuses model.model.ErrSubscriptionNotFound" — `botplatform-service/subscription_store.go:17`

**Credential handling — verified clean.** Every `slog` call site in the service was read (`handler.go:57,154,159,180`, `main.go:29,72,77,87,93,95,138,147,150`, `middleware.go:27`, `middleware_idempotency.go:91`). No password, `x-auth-token`, raw session token, or request body is logged; the login path logs only `account` + `userId` (`handler.go:159`), the access log logs `bot_account` (`middleware.go:36`), and the idempotency warning logs a SHA-256 `opID`, not the body (`middleware_idempotency.go:91-92`). No `fmt.Println`/`log.Println` anywhere; no token is ever placed in an `errcode` cause.

### Recommendations
- `high` — Replace the `strings.Contains(err.Error(), "unknown field")` classification in `bot_handlers.go:211` with a structural check, and add a test asserting the `bot_unknown_field` reason so a stdlib message change fails CI instead of silently degrading.
- `medium` — Apply `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` in `middleware_idempotency.go:60` so the documented cap holds at the first reader, and keep `bindStrict`'s cap as defence-in-depth.
- `medium` — Delete the `slog.WarnContext` in `handler.go:180`; carry `reason` via `errcode.WithLogValues` so `Classify` emits the single line.
- `medium` — Change `newHandler` to accept `subs`, `forwarder`, and `dmEnsurer`, removing the three field assignments in `main.go:106,114,116`.
- `medium` — Convert the ~12 `errcode.Internal(..., WithCause(err))` infra sites to raw `fmt.Errorf("<what this function was doing>: %w", err)`, matching the correct usage already present at `bot_handlers.go:83,185`.
- `low` — Wrap `store_mongo.go:99` as `fmt.Errorf("delete sessions beyond cap: %w", err)`; switch `bot_handlers.go:219` to `!errors.Is(err, io.EOF)`.
- `low` — Unexport `BotplatformStore`→`botplatformStore`, `BotSub`→`botSub`, and the three `Handle*` methods; drop the unused `DevMode` field.
