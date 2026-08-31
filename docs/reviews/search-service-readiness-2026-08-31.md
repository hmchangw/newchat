# search-service — Production Readiness Review

**Service:** `search-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

Small files, clean layering, excellent WHY-comments, and **exemplary client-API documentation** — all five RPCs registered via `pkg/subject` builders, documented in the canonical doc *and* both derived views with no field drift. The problems are about **what ships as production while still being a prototype**, and about **unbounded client-driven cost**. `search.apps` threads an `account` through three layers to a `_ = account // referenced in the access-guard $lookup once implemented`; `search.users` is a live registered route whose upstream URL, request body and auth scheme are all unresolved TODOs. And the **messages index read pattern is a hardcoded literal** while the writer's index name is fully operator-configured — a prefix mismatch returns zero hits, silently.

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
| Count | 0 | 12 | 20 | 12 | 7 | **51** |

---

## 2. Go code quality — 4 / 5

Idiomatic, disciplined Go — error wrapping is consistently contextual, `errcode` tiering is textbook, and comments explain *why* — but request-scoped logs drop `context` and two prototype code paths are wired into production.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **Six request-scoped warnings use the non-context `slog.Warn`**, so they carry no trace ID and none of the `WithLogValues` enrichment — every call site already holds a `ctx` (or the `*natsrouter.Context`, which implements it). The service's own `logEmptyResult` gets this right, so **trace-correlated logging is broken only on the enrichment/cache-degradation paths — exactly the ones you need to correlate** | `enrich.go:44`, `:91`, `:100`, `:173`; `handler.go:260`, `:290` vs `:196` |
| medium | **`search.apps` accepts `account`, threads it through the store interface and pipeline builder, then discards it**: `_ = account // referenced in the access-guard $lookup once implemented`. The parameter is dead weight in a live signature, and the TODO block documents that the user-scope guard is absent. **Dead parameters that look like access control are the worst kind of dead code** | `query_apps.go:49`, `:38-48`; `store.go:60-65`; `store_mongo.go:140` |
| medium | **`httpUsersClient` is a placeholder with a guessed wire contract, wired into `main.go` unconditionally.** Three `TODO(searchUsers-thirdparty)` markers admit the URL path, request body and response shape are all invented — and `USERS_API_URL` is `required`, so **the service cannot boot without pointing at an endpoint nobody has specified** | `users_client.go:21-23`, `:40-43`, `:59-60`; `main.go:216`, `:58` |
| low | All five handlers re-set `request_id` that the router middleware already attached — `natsrouter.RequestID()`'s docstring says "handlers don't need to re-pass it" | `handler.go:94`, `:146`, `:217`, `:303`, `:339` |
| low | Nine identifiers exported from `package main` where nothing can consume them | `store.go:13-90`; `query_messages.go:18`; `main.go:32-108` |
| low | `MongoStore` is named for its **technology, not its domain**, breaking the `<Domain>Store` convention — the handler ends up holding `store` (ES) and `mongo` (Mongo) side by side, **neither of which names what it stores** | `store.go:59`; `handler.go:48-49` |
| low | Internal projection structs carry no struct tags at all, while ES hit structs carry `json` only. These are internal DTOs, so the rule arguably shouldn't bite — but the drift deserves a deliberate decision | `store.go:38-56`, `:29-33` |
| low | The Valkey error is flattened into a format string, **destroying `errors.Is`/`As` on it**. Deliberate and commented, but no caller can ever match `valkeyutil` sentinels through the chain | `handler.go:272` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Two distinct validation failures emit the identical message, making them indistinguishable in logs; a no-op loop-variable copy under Go 1.25; raw caller-supplied input echoed unbounded into a client-facing error | `main.go:137`, `:141`; `enrich.go:165`; `query_rooms.go:90` |

### Recommendations
- `medium` — Convert the six `slog.Warn` calls in `enrich.go` and `handler.go` to `WarnContext(ctx, …)`; the ctx is already in scope at every site. Consider a lint rule banning non-`Context` `slog` verbs outside `main.go`.
- `medium` — **Either implement the `subscriptions` access guard in `buildSearchAppsPipeline` or drop `account` from the store method and the pipeline builder until it is real.** A discarded access-control parameter reads as enforcement that isn't there.
- `medium` — Gate `USERS_API_URL` behind a feature flag (or make `search.users` return `errcode.Unavailable`) until the HR contract is settled, rather than shipping a guessed `POST /search` as required config.
- `low` — Delete the five redundant `"request_id"` arguments; unexport the interfaces, DTOs and config structs (`mockgen -source` handles unexported types fine).
- `low` — Rename `MongoStore` to a domain name (`CatalogStore`/`EnrichmentStore` — it serves apps/users/subscriptions lookups) and `SearchStore` to `IndexStore`, so the handler's two dependencies are distinguishable by purpose.

