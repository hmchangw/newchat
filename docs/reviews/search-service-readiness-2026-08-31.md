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

---

## 3. Architecture — 4 / 5

Boundaries, consumer-defined interfaces, subject builders, no-stream-creation and shutdown order are all correct; the deductions are a duplicated request-timeout knob that makes the documented operator dial ineffective, post-construction DI, and a hardcoded message index.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`SearchConfig.RequestTimeout` re-declares a knob `pkg/natsrouter` already owns** as `GuardConfig.RequestTimeout` — both default to 10 s and **both deadlines are applied**, so the effective bound is the min. `deploy/docker-compose.yml` exposes only `SEARCH_REQUEST_TIMEOUT`, so **raising it to 30 s still gets cut at 10 s by the guard, silently.** §6: a knob is declared once, in the package that owns the thing it configures | `main.go:71` vs `pkg/natsrouter/guard.go:24`; `main.go:249`; `handler.go:80`; `deploy/docker-compose.yml:28` |
| medium | **The message index is a hardcoded literal** (`{"messages-*", "*:messages-*"}`) while the writer's index name is fully operator-configured (`MSG_INDEX_PREFIX,required`). The service applies a strict read/write contract to the *other three* indices — required, notEmpty, `StripVersion`-checked, read pattern derived from config — but **the primary search target escapes it.** A prefix that doesn't literally start with `messages-` **returns empty hits with no error**, and a v1→v2 reindex is double-searched | `query_messages.go:18` vs `search-sync-worker/main.go:60`; `main.go:88-96`, `:141-155` |
| medium | `handler.room` is injected **by field assignment after construction**, not through `newHandler`. The consequence leaks into business logic as nil-guards: `if h.room == nil` and three `if h.mongo != nil` branches. **A test-only convenience has become a production nil-check on the enrichment path** | `main.go:243`; `handler.go:56`; `enrich.go:152`, `:41`, `:79`, `:87` |
| medium | `ESConfig` and `SearchConfig` are **both mounted on `envPrefix:"SEARCH_"`**. The code *documents* the shadowing hazard rather than removing it ("any new field added to either must be checked against the other"), and the naming already misleads — `SEARCH_URL` is the Elasticsearch URL, while `HealthAddr` sits in the request-shape struct | `main.go:82-86`, `:75-80`, `:66-72` |
| low | Middleware is hand-rolled in the **wrong order** (`RequestID → Recovery → Logging`) whereas `natsrouter.Default` establishes `Recovery(), RequestID(), Logging()`. `natsrouter.DefaultGuarded` exists to do exactly what this reassembles by hand, and **its doc comment says it exists "so a service can't apply only half the overload protection"** | `main.go:245-249`; `pkg/natsrouter/router.go:142`; `pkg/natsrouter/guard.go:67-71` |
| low | The restricted-rooms Valkey key builder is **duplicated outside the service**, along with a hand-copied TTL — two independent literals for one Valkey keyspace | `store_valkey.go:23`; `tools/seed-sample-data/sidestores.go:14-18` |
| low | The health listener uses `http.NewServeMux`, while §6 says "never `net/http` mux directly". This is the sole `pkg/health.Register` caller in the repo, so it is a one-off; a health-only port arguably doesn't warrant Gin, but `CLAUDE.md` as written carves out no exception | `main.go:262` |
| nitpick | `MessageIndexPattern` is an exported, **mutable** package-level `var` in `package main` with no external consumer; `appMetrics` is a process-wide mutable global that tests swap and restore | `query_messages.go:18`; `metrics.go:107` |

### Recommendations
- `high` — **Delete `SearchConfig.RequestTimeout` and `handler.withRequestTimeout`**; let `natsrouter.GuardConfig` own the per-request deadline, and update the compose file to `REQUEST_TIMEOUT`. If a search-specific ceiling is genuinely wanted, mount `Guard natsrouter.GuardConfig` with `envPrefix:"SEARCH_"` so there is exactly one dial.
- `medium` — Add `MSG_INDEX_PREFIX` (`required,notEmpty`) to `Config`, run it through `searchindex.StripVersion` like the spotlight indices, and derive both the local and CCS read patterns from it.
- `medium` — Move `room RoomInfoClient` into the `newHandler` signature and drop the nil guards in `enrich.go`; tests pass explicit no-op fakes.
- `medium` — Split the ES connection knobs onto their own prefix (`SEARCH_ES_`) so the two config structs cannot shadow each other.
- `low` — Replace the hand-rolled middleware block with `natsrouter.DefaultGuarded(...)` to inherit the correct order; export the restricted-rooms key builder and TTL from one place.

