# message-gatekeeper — Production Readiness Review

**Service:** `message-gatekeeper` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The first hop on every user message, and the hot path is genuinely well-engineered: precomputed metric attribute sets, L1+L2 caches with singleflight, precise Mongo projections, correct semaphore consumer pattern, sonic + `Pretouch`, clean `jsretry` discipline. Excluding `main.go` the package is ~91% covered. Three things stand out. **The consumer binds MESSAGES with no `FilterSubjects`, so every verb under `msg.>` is processed as a create** — a client publishing `msg.edit` today is validated as a send and republished to the canonical `.created` subject. **The parent-resolution path has no overall deadline** — a reply that quotes a different parent can hold a worker slot for ~6 s. And the derived client-API view **contradicts the canonical doc** on bot-DM fan-out.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 8 | 20 | 15 | 9 | **52** |

---

## 2. Go code quality — 4 / 5

Disciplined error tiering, typed `errcode` usage and zero string-matching on errors; marred by four ctx-less `slog` calls on the per-request path, two "log AND return" violations, and two unwrapped error returns.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **Log-and-return double-logs the large-room rejection**: `slog.Info("send blocked")` fires, then the same error reaches `errcode.Classify`, which logs `"request failed"` at Info for a `Forbidden`. **Two Info lines per blocked send** | `handler.go:425`, returned `:432`, marshalled `:211`; `pkg/errcode/classify.go:40`, `:49` |
| medium | Same double-log on the invalid-subject path | `handler.go:160`, marshal at `:164` |
| medium | **Four per-request logs use the ctx-less `slog` variants**, dropping the request/trace correlation the surrounding code just built — `ctx` is enriched with `request_id`/`account` at `:145` and `room_id` at `:173`. Two of them land in Loki with **no `request_id` at all** — precisely the two lines an operator would want to join to a failing send. Sibling calls in the same file use the `*Context` forms, so this is inconsistency, not house style | `handler.go:160`, `:298`, `:425`, `:472` |
| low | **Unwrapped error escapes `processMessage`**: `return sonic.Marshal(msg)` hands the caller a bare sonic error, which is then classified as an *infra* failure and **NAKed for redelivery — even though a marshal failure is permanent.** The immediately preceding marshal wraps correctly. This is the only such tail-return in any service handler repo-wide | `handler.go:519` vs `:507` |
| low | Bare `return nil, err` from the cache constructor — the message survives only because the caller happens to supply one | `metacache.go:21` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Unchecked type assertion inside a JetStream worker goroutine — safe today, but a panic here takes down a `MaxWorkers` goroutine; `interface{}` instead of `any` on a Go 1.25 module; an empty `default:` clause; `accountFromSubject` computed twice on the rejection path | `subcache.go:111`, `:81`; `handler.go:251`, `:147`, `:164` |

**Verified clean:** no `fmt.Println`/`log.Println`; no `errors.Is`-by-string; no `WithCause` chaining an `*errcode.Error`; **no token/password/body logging** — the flow breadcrumbs carry sizes and coarse tags only, and `:180` **deliberately declines `WithCause(parseErr)` to keep the offending substring out of the log**; `WithReason` used only where the frontend must branch; infra failures returned as raw `fmt.Errorf`; `errCanonicalPublish` is a sentinel matched by `errors.Is`, not text. **The documented sonic exception at `fetcher_history.go:53` is correctly scoped** — the projection omits `Reactions` and is correspondingly excluded from `pretouch.go`.

### Recommendations
- `medium` — Delete the two `slog` calls that precede a returned error; move any field worth keeping into `errcode.WithLogValues(ctx, …)` so `Classify` emits them on its single line.
- `medium` — Convert `handler.go:298` and `:472` to the `*Context` variants so the enriched `request_id`/`room_id`/trace ride the line.
- `low` — Wrap the tail marshal as a typed `errcode.Internal(…, WithCause(err))` so a permanently unmarshalable message **Acks instead of burning `MaxDeliver` NAKs**; wrap `metacache.go:21`.
- `nitpick` — Make the singleflight assertion comma-ok with a defensive fallback; switch `interface{}` → `any`.

