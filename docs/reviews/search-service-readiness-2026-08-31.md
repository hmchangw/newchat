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

---

## 4. Test coverage — 2 / 5

Coverage is **66.9% (674 statements)**, below the §4 80% floor, so the dimension is floored at 2. The suite's *quality* is well above that — the access-filter/HSS and CCS assertions are as good as one would expect anywhere — but the handler entry-guards, the request deadline and the degraded-CCS paths are genuinely untested. The number also **understates reality**: `store_mongo.go`, `users_client.go` and `room_client.go` are covered only by `//go:build integration` tests excluded from the profile.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 66.9%, below the mandatory 80% floor | `coverage_by_service.txt:26` |
| high | **The missing-`account` rejection is uncovered in all five handlers** — `ctxWithAccount` always supplies one. This is the **auth-adjacent entry guard**: a router/subject-pattern change that stopped binding `{account}` would return results for an empty principal with no test failing | `handler.go:91`, `:143`, `:214`, `:300`, `:336` |
| high | **`withRequestTimeout`'s actual deadline branch is never executed** — `newTestHandler` leaves `RequestTimeout` zero and no integration fixture sets it. `REQUEST_TIMEOUT` is the only bound on a slow ES/CCS call, and nothing proves it is applied or propagated to `store.Search` | `handler.go:84`; `handler_test.go:88-96` |
| medium | `loadRestricted`'s "cache miss **and** ES prefetch fails" branch (healthy cache, ES down) is uncovered; only the cache-also-errored sibling is tested, and the two produce different wrapped chains | `handler.go:274` vs `:272` |
| medium | **No CCS *degraded* test**, and it is untestable as written: `rawResponse` decodes `_shards.total` but **not `failed`/`skipped`**, so **a partial CCS result is returned as complete with an undercounted `Total`** | `integration_ccs_test.go:282`, `:373`; `response.go:21-23` |
| medium | Response-parse failure branches uncovered in three handlers. The parsers are unit-tested for malformed input directly, but no test drives a malformed body *through* a handler, so the wraps and the metrics `statusLabel` they produce are unverified | `handler.go:131`, `:180`, `:245` |
| medium | `Register` is 0% — no unit test asserts the five `subject.Search*Pattern` bindings; only 4 of 5 are indirectly exercised by integration fixtures | `handler.go:72` |
| medium | `newHandler` is 55.6%: **all four zero-value fallbacks are uncovered** because `newTestHandler` always passes explicit values, so a broken default ships silently | `handler.go:57-68` |
| medium | **`mock_store_test.go` (291 generated lines) is unused** — every test uses hand-rolled fakes. §4 mandates `go.uber.org/mock`; the file is dead weight `make generate` keeps regenerating | `handler_test.go:25-84` |
| low | `enrichMessages`' apps-lookup **error** fallback is uncovered — the existing test covers an empty result, not a failure, so the degrade-without-appInfo path is unproven | `enrich.go:99-101` |
| low | `config_test.go:71` calls `os.Unsetenv` with no prior `t.Setenv`, so **no cleanup is registered and the var stays unset for the rest of the process** — an order-dependence hazard §4 forbids | `config_test.go:71` |
| nitpick | ~130 handler/query tests are near-identical single-scenario funcs; `config_test.go:48` shows the house style done right | `handler_test.go:177-948` |

**Positive, and genuinely exemplary:** the integration harness uses `RunTestsWithPrewarm`, containers from `pkg/testutil`, `FlushValkey` registered in cleanup, and the **sanctioned inline CCS ES pair stores its ref with `t.Cleanup(Terminate)` plus network removal** — exactly what `CLAUDE.md` asks of the inline-container exception.

### Recommendations
- `high` — Add a table-driven `Test<Handler>_MissingAccountParam` over all five handlers using `natsrouter.NewContext(nil)`, asserting the `errcode` category.
- `high` — Set `RequestTimeout` in `newTestHandler` and add a test whose fake `store.Search` blocks until `ctx.Done()`, asserting a `DeadlineExceeded`-derived error and that `cancel()` fires.
- `medium` — **Decode `_shards.failed`/`skipped` in `rawResponse`, surface it**, then add a CCS test that stops the remote container mid-suite and asserts the degraded result is flagged rather than reported complete.
- `medium` — Cover `loadRestricted` cache-miss + ES-error and the three handler-level parse-error branches via fakes returning malformed JSON.
- `medium` — Either migrate `handler_test.go` to the generated mocks or delete `mock_store_test.go` and its directive — **do not keep both**.
- `low` — Table-drive the repeated pagination/empty-query cases; add `newHandler` default-fallback and `enrich` apps-error cases; guard the `config_test.go` env manipulation with `t.Setenv`.

---

## 5. Maintainability — 3 / 5

Small files, excellent WHY-oriented comments and clean layering are undercut by a five-way duplicated handler preamble, two endpoints still shipping as TODO-marked prototypes, and a self-documented config-prefix landmine.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`search.apps` threads `account` through three layers to a discard**: `_ = account // referenced in the access-guard $lookup once implemented`. The signature and doc comment read as an access-guarded pipeline; the body is `$match/$skip/$limit` only. **A dead parameter that looks load-bearing is the worst kind of maintenance trap** | `query_apps.go:49`; `store.go:442`; `handler.go:317` |
| high | **`httpUsersClient` is a placeholder whose request body, URL path and auth scheme are all unresolved TODOs — yet `search.users` is a live registered route.** The "stable contract" claimed in-file is the return type only; everything on the wire is guesswork | `users_client.go:21`, `:40`, `:59`; `handler.go:376` |
| medium | **Five handlers repeat an identical ~12-line preamble** (`Params.Require("account")` → `WithLogValues` → `normalizePagination` → trim + empty-query check → `withRequestTimeout`). Adding a sixth searchable collection means copy-pasting it a sixth time; a fix to the account/validation path has to be applied five times | `handler.go:87-104`, `:139-160`, `:210-230`, `:296-315`, `:332-357` |
| medium | `parseRooms` and `parseOrgs` are structural clones over an already-generic `rawResponse[T]`, differing only in the `_source` type and the projection function | `response.go:108-124` vs `:153-169` |
| medium | **`ESConfig` and `SearchConfig` are both mounted at `envPrefix:"SEARCH_"`.** The hazard is *documented rather than removed*, and the naming already misleads: `SEARCH_URL` is the Elasticsearch URL while `HealthAddr` sits in the request-shape struct. **A comment cannot prevent the collision; only distinct prefixes can** | `main.go:83`, `:86`, `:75-80`, `:66-72` |
| medium | Zero-result diagnostics are **asymmetric**: `logEmptyResult` fires for rooms and orgs but not messages — and `parseMessagesResponse` does not even return the shard count, so the `messages-*` wildcard read has the same allow-no-indices blind spot the helper exists to catch | `handler.go:184`, `:249` vs `:130`; `response.go:71` |
| low | `enrichMessages` is 138 lines running six sequential phases in one function | `enrich.go:18-151` |
| low | `main()` is 177 lines containing **14 repetitions** of `slog.Error(...); os.Exit(1)` | `main.go:124-300` |
| nitpick | `MessageIndexPattern` is an exported **mutable** package-level `var` in `package main`, so nothing can consume it and any test can mutate it globally | `query_messages.go:18` |

### Recommendations
- `high` — **Decide per endpoint**: either finish `buildSearchAppsPipeline`'s subscription access guard, or delete the `account` parameter from the store method and the builder so the signature stops advertising scoping that does not exist. Same call for `search.users`: pin the third-party contract or unregister the route until it exists.
- `medium` — Extract the handler preamble into one helper (e.g. `begin(c, size, offset, query) (ctx, account, cancel, error)`) and have all five handlers call it; the roomType/empty-query specifics stay in the handler body. **This is the single highest-leverage refactor here.**
- `medium` — Collapse `parseRooms`/`parseOrgs` into `parseHits[S, R any](raw, conv)`; keep `parseMessagesResponse` as a thin wrapper that also returns shards, then call `logEmptyResult` from `searchMessages` for parity.
- `medium` — **Split the `SEARCH_` prefix now, while the field sets happen not to collide**: move `ESConfig` to `SEARCH_ES_` and `HealthAddr` to a top-level `HEALTH_ADDR`.
- `low` — Break `enrichMessages` into `collectEnrichKeys`, `resolveDirectory`, `buildRooms` and a final attach loop; add a `fatal(msg, args...)` helper and a `mustParseConfig()` to compress the 14 exit blocks.
- `nitpick` — Make `MessageIndexPattern` unexported and return it from a function (or pass it via `handlerConfig`) so all three index sources are configured in one place.

---

## 6. Integration — 4 / 5

**Client-API documentation and `pkg/subject` usage are exemplary** — all five RPCs registered via builders and documented in the canonical doc plus both derived views with **no field drift** — but the messages-index read contract with `search-sync-worker` is hardcoded rather than configured, and three index `_source` shapes are re-declared locally.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The messages read pattern is a hardcoded literal** while the writer's index name is fully env-driven (`MSG_INDEX_PREFIX`, e.g. `messages-site-a-v1-2026-04`). search-service has **no `MSG_INDEX_PREFIX` counterpart at all** — its compose configures only the other three indices. The writer's validation enforces only a `-v<N>` suffix, so `MSG_INDEX_PREFIX=chatmsg-site-a-v1` is accepted and **`search.messages` then returns zero hits silently** (`allow_no_indices` + `ignore_unavailable`). This is precisely the drift the adjacent comment says the other three envs exist to prevent | `query_messages.go:18` vs `search-sync-worker/main.go:60`; `deploy/docker-compose.yml:33-38`; `main.go:88-91` |
| medium | **The `-v<N>` index version is validated but has no read-side effect** — reads use the version-stripped base wildcard, so a v1→v2 reindex is read from **both versions at once**. There is no alias anywhere in `pkg/searchindex` or the migrator, so during a cutover the same spotlight `_id` exists in v1 and v2 and `search.rooms` **returns the room twice**; `search.messages` double-counts `total` | `main.go:146-157`; `query_messages.go:18` |
| medium | `messageSearchHit` re-declares the messages-index `_source` contract locally, **against the explicit instruction on the canonical type** ("this is the one place the wire/mapping contract for the messages index is defined; do not redefine it anywhere else"). Same pattern for `roomSearchHit` | `response.go:33-49` vs `pkg/searchindex/messagedoc.go:12-15`; `response.go:54-59` |
| medium | `UserRoomDoc`'s sync comment **points at a symbol that no longer exists** — the referenced type has moved and been renamed. The local copy also silently omits `roomTimestamps`, so a reader following the comment cannot find the contract | `store.go:27-34` vs `pkg/searchindex/userroomdoc.go:68` |
| low | `main.go` reimplements the wildcard helper that exists to prevent exactly this drift — `fmt.Sprintf("%s-*", …)` duplicates `searchindex.IndexPattern`, whose doc comment says "template and mapping push share it to avoid drift" | `main.go:151`, `:158`; `pkg/searchindex/version.go:33` |
| low | Cross-site room enrichment has **no guard on an empty `siteId`** — an empty site yields `chat.server.request.room..info.batch` (empty token), a request that can never be served. Degrades correctly but wastes a 5 s round trip per response | `enrich.go:31`, `:56`, `:164` |
| nitpick | `Search Users` is the only search RPC whose error table omits the "See [Error envelope]" pointer and both "Triggered events" subsections that every sibling carries | `docs/client-api.md:4544-4553` |

### Verified clean
All five handlers registered via `subject.Search*Pattern` builders, **no raw `fmt.Sprintf` subjects**; all five subjects documented in `docs/client-api.md` and **both derived views** (`events.md` correctly has no entry); request/response field tables match `pkg/model/search.go` **exactly**, including the prototype `$lookup` caveat; the service publishes no NATS events and owns no JetStream consumer or stream, so the `Timestamp`/OUTBOX-partition/`bootstrapStreams` rules do not apply; `RoomsInfoBatchRequest{SkipKeys:true}` matches room-service's expectation.

### Recommendations
- `high` — Add `MSG_INDEX_PREFIX` to `Config` (unprefixed, `required,notEmpty`, same `StripVersion` validation as the other three), derive the local pattern via `searchindex.IndexPattern` and the CCS pattern as `"*:" + that`, and set it in the compose file beside the existing three.
- `medium` — **Pick a read-side version policy and encode it**: either publish a read alias per index family from `search-sync-worker`/`es-index-migrator` and read the alias, or read the exact configured `-v<N>` name. **The current base-wildcard makes the version suffix decorative.**
- `medium` — Replace `messageSearchHit` and `roomSearchHit` with `searchindex.MessageDoc` / `SpotlightDoc` (adding a `SpotlightOrgDoc` for parity), keeping the `to*` functions as the only projection layer.
- `medium` — Delete the local `UserRoomDoc` and read `searchindex.UserRoomUpsertDoc`, removing the stale cross-service comment.
- `low` — Skip the `RoomsInfoBatch` fan-out for an empty site key; swap the two `fmt.Sprintf` calls for `searchindex.IndexPattern`; add the missing error-envelope/triggered-events stanzas to the Search Users doc section.

---

## 7. Performance — 3 / 5

Solid hot-path fundamentals (batched enrichment with **no N+1**, precomputed metric attribute sets, bounded fan-out, Valkey L2, `_source` excludes, `secondaryPreferred` reads), undercut by **unbounded client-driven query cost**, an unprojected Mongo aggregation, and no HTTP connection-pool sizing behind a 256-way concurrency guard.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The apps aggregation has no `$project`**, so `search.apps` decodes whole `apps` documents into `[]model.App` on every request. §MongoDB ("always project precisely") is violated outright; the TODO block above it also plans `$lookup`s that `CLAUDE.md` forbids without a justification comment | `query_apps.go:51-55`; `store_mongo.go:141-149` |
| high | **`offset` is validated non-negative but never capped**, while `size` is. A client can send `offset: 5000000`; ES pays deep-pagination cost across `messages-*` **and every remote cluster** before `index.max_result_window` rejects it. `MAX_DOC_COUNTS` bounds only half the `from+size` product | `handler.go:374-384` |
| high | **`req.RoomIDs` goes straight into an inline `terms` clause with no length bound.** A 50k-element `roomIds` array becomes a 50k-term clause; a large `restricted` map likewise expands to **two bool clauses per restricted room** with no cap — risking `max_clause_count` rejection and a multi-MB request body per search | `query_messages.go:112-134`, `:99-107` |
| high | **No idle-connection pool sizing on either outbound HTTP client while `MAX_CONCURRENCY` defaults to 256.** The ES client inherits `http.DefaultTransport.Clone()` (`MaxIdleConnsPerHost = 2`), and the HR client is built without `WithMaxIdleConns`. **Beyond 2 in-flight calls per host every request pays a fresh TCP+TLS handshake.** `media-service` and `upload-service` already set `WithMaxIdleConns(32)` — this service is the outlier | `main.go:212-215`; `pkg/searchengine/factory.go:33-58`; `pkg/natsrouter/guard.go:21` |
| medium | `GetUserRoomDoc` fetches the **entire** user-room doc including the `rooms[]` array on every cache miss — while `store.go:19-22` **explicitly states that array is never needed locally** (terms-lookup resolves it server-side). For a heavy user this decodes and discards thousands of room IDs per miss | `store_es.go:37-52`; `pkg/searchengine/adapter.go:393-397` |
| medium | `track_total_hits: true` on all three ES queries, but **only `searchMessages` returns `Total`** — `parseRooms`/`parseOrgs` discard it. For rooms and orgs this buys an exact cross-shard count nobody reads | `query_messages.go:57`; `query_rooms.go:43`; `query_orgs.go:18`; `response.go:110-121`, `:160-171` |
| medium | Enrichment issues its independent lookups **serially** on the user-visible path: subscriptions → users → apps → room RPCs. `UsersByAccounts` and `AppsByAssistantNames` have **no data dependency on each other**, so up to three round trips of latency are added in sequence when two would do | `enrich.go:36-44`, `:87-106` |
| low | `loadRestricted` has **no single-flight**: on TTL expiry (5 m) or a Valkey blip, concurrent requests for the same hot account each fire their own ES `GET`. The empty-map caching comment shows miss-storms were already anticipated | `handler.go:250-292` |
| low | The apps `$match` is an **unanchored `$options:"i"` regex** over `name` — it cannot take index bounds, so the `{name:1}` index at best converts a collection scan into a full index scan; and `$skip` before `$limit` with **no `$sort`** makes page ordering unstable | `query_apps.go:37-40`, `:53` |
| nitpick | Every ES body is rebuilt as nested `map[string]any` and re-marshalled per request; dwarfed by the ES round trip | `query_messages.go:41-84` |

**No goroutine leaks**: `fetchRoomNames` bounds fan-out and joins, the health server exits via `Shutdown`, and no `time.Sleep` appears in production code. No JetStream consumers, so the `jsretry`/`BackOff` rules do not apply.

### Recommendations
- `high` — Add a terminal `$project` matching `model.App`'s bson tags to the apps pipeline; drop the `$lookup` plan from the TODO or attach the required justification comment.
- `high` — Cap `offset` in `normalizePagination` (bounded well under `index.max_result_window`) and reject it with `errcode.BadRequest` rather than letting ES do it.
- `high` — Bound `len(req.RoomIDs)` (reject above ~200) and cap the restricted-room clause expansion, falling back to a terms-lookup-only clause past the ceiling.
- `high` — **Size the connection pools**: `restyutil.WithMaxIdleConns(32)` on the HR client, and thread a `MaxIdleConnsPerHost` knob through `searchengine.Config` for the ES transport, derived from `MAX_CONCURRENCY`.
- `medium` — Pass `_source_includes=restrictedRooms` on the user-room `GetDoc` and drop `Rooms` from the local doc type; set `track_total_hits` to a bounded integer for rooms and orgs.
- `medium` — Run `UsersByAccounts` and `AppsByAssistantNames` concurrently, and start `fetchRoomNames` as soon as the subscription partition is known.

