# portal-service — Production Readiness Review

**Service:** `portal-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

A small Gin service with a genuinely well-engineered read path — a lock-free `atomic.Pointer` directory snapshot with immutable maps, precise projections on both queries, and the fleet's one `$lookup` that carries the CLAUDE.md-required justification comment with a correlated `$expr` on an indexed field. Comment discipline is exemplary; the atomic-swap rationale is explained where a reader needs it.

The service has **no NATS surface at all**, so its integration risk is entirely the client-facing HTTP contract — and `docs/client-api.md` has drifted from the handler in four places, including a **documented `site_unknown` reason the code never emits** and an `upstream_unavailable` attributed to an endpoint that makes no outbound call. Coverage is 58.6%, under the critical line, and the uncovered branch in `resolve` is exactly the dev-fallback bug: a fallback site absent from the registry returns **200 with an empty `baseUrl`** — a syntactically valid response the client cannot act on. Rounding it out, the botplatform forwarder and the unauthenticated login endpoint both lack the connection-pool and load-shedding tuning their siblings already apply.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 2 | 10 | 14 | 2 | **29** |

---

## 2. Go code quality — 4 / 5

Idiomatic, precisely commented, correct errcode tiering and slog discipline; one real reason-code gap and a handful of low-severity drifts from CLAUDE.md.

### Findings
- `medium` — a defined domain reason is never emitted: both site-registry failures return a raw `fmt.Errorf`, which `Classify` collapses to `internal` with an **empty** reason — `portal-service/handler.go:193`, `portal-service/handler.go:299`. `errcode.BotplatformSiteUnknown` ("site_unknown") already exists at `pkg/errcode/codes_botplatform.go:30` and `docs/client-api.md:414` promises it to the frontend.
- `low` — log-and-return: `slog.WarnContext` immediately before `errhttp.Write`, which runs `Classify` and logs again — `portal-service/handler.go:284`, `:291`, `:319`. CLAUDE.md §3 says never log AND return. It is a repo-wide login-audit convention (`admin-service/login.go:145`, `botplatform-service/handler.go:180`), but CLAUDE.md wins; if the audit line is wanted, it should carry a distinct `login_outcome` key and the errcode path should stay quiet.
- `low` — error compared with `!=` instead of `errors.Is` — `portal-service/main.go:191`. Siblings already use `errors.Is` (`media-service/main.go:143`, `admin-service/main.go:174`).
- `low` — a secret carries an `envDefault` — `MONGO_PASSWORD` at `portal-service/main.go:59`. CLAUDE.md: "never default secrets or connection strings".
- `nitpick` — stuttering names `PortalHandler` / `NewPortalHandler` / `PortalHandlerOption` in `package main` — `portal-service/handler.go:104`, `:121`, `:139`. Every other service uses `Handler`/`NewHandler` (e.g. `broadcast-worker/handler.go:79`).
- `low` — audit-coverage gap, not a defect: gosec + repo-owned semgrep clean repo-wide; `govulncheck` and semgrep registry packs unavailable (blocked egress).

### Recommendations
- `medium` — attach `errcode.WithReason(errcode.BotplatformSiteUnknown)` via `errcode.Internal(...)` at `handler.go:193`/`:299`, or delete the promise from `docs/client-api.md:414`; assert the reason in `TestHandleLogin_500_SiteUnknown` (`handler_test.go:652`), which currently checks only the status.
- `low` — drop the pre-`Write` `slog.Warn` calls (`handler.go:284`, `:291`, `:319`) and let `errhttp.Write` be the single log site.
- `low` — `errors.Is(err, http.ErrServerClosed)` at `main.go:191`; make `MONGO_PASSWORD` tag-free or `required` per deployment.
- `nitpick` — rename to `Handler`/`NewHandler`/`handlerOption` for repo consistency.

---
