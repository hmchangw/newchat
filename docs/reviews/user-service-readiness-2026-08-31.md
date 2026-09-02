# user-service — Production Readiness Review

**Service:** `user-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The best-behaved service in the fleet on code discipline: **zero Section-3 violations found** across ~18.8k lines, no token/password/body ever logged, `errors.Is` throughout, every `fmt.Errorf` wrapping with context. The sub-package decomposition is coherent and the `subscription.list` hot path is genuinely well engineered (TTL'd sort-key LRU, page-sized batch refetches, bounded per-site fan-out, precise projections). Three things hold it back. The **cross-site fan-out is synchronous, sequential and best-effort inside the request handler** — a down peer permanently loses that site's replica of settings that a remote `notification-worker` reads to decide whether to push. The **per-site fan-out pattern is hand-copied five times and has already drifted** — one copy lost its semaphore. And **coverage is 53.2%**, with all six client-facing chatlist RPCs handler-untested.

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
| Count | 1 | 6 | 20 | 13 | 7 | **47** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules are clean repo-wide. `govulncheck` and the semgrep registry packs could not run (egress blocks `vuln.go.dev` / `semgrep.dev`); dependency-CVE coverage is unverified.

---

## 2. Go code quality — 4 / 5

Exemplary error-wrapping, logging and `errcode` discipline across ~18.8k lines with **zero Section-3 violations found**; only minor idiom and startup-robustness nits keep it off 5.

### Verified clean — and this is the finding that matters

Zero `fmt.Println`/`log.Println`. All 58 `slog` sites use scalar structured k/v with `request_id` and `WarnContext`. **No token, refresh token, password or request body is ever logged** — the two SSO failure paths deliberately route the verification error through `WithCause` (server-log only) and never the token bytes (`service/sso.go:41-47`, `:82`; `middleware.go:93-98`). No `err.Error()` string comparison — `errors.Is` throughout, including the `pkgoidc.ErrTokenExpired` and `ErrShuttingDown`/`context.DeadlineExceeded` branches. Every `fmt.Errorf` wraps with a this-function description and `%w`. No silently discarded errors, no `time.Sleep`, no `os.Getenv`, no `map[string]interface{}` payloads, no `panic`. `errcode` tiering respected: Tier-1 constructors with `WithReason` only on branchable cases, `errcode.Parse` confined to the four remote-envelope RPC clients, no `WithCause` wrapping another `*errcode.Error`, no log-and-return double-logging. The `#nosec G101` on `ssoTokenHeader` is a correctly-placed, justified false positive.

| Sev | Finding | Evidence |
|-----|---------|----------|
| low | All five `EnsureIndexes` failures are `slog.Warn` + continue, so a pod can serve the whole aggregation surface against an unindexed collection with no fail-fast and no readiness signal — index absence degrades silently to collscans exactly under the load that caused the failure | `main.go:177-190` |
| low | `cappedUnion(ids []string, trigger string, cap int)` shadows the predeclared `cap` builtin | `service/badge.go:67` |
| low | DTOs in `models/` carry only `json` tags where §3 requires both. Materially harmless today — none is bson-decoded, and `models.ActiveSubscription` does carry both, so the rule is honoured exactly where decoding happens — but a future direct `Decode` into one of these would silently zero the fields | `models/prioritycontacts.go:19-22`, `models/app.go:39-41`, `models/status.go:17-21` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Thread-unread fan-out spawns one unbounded goroutine per site, bypassing the `MAX_SITE_FANOUT` semaphore that `fanOutChunks` enforces for the same class of RPC | `service/threadunread.go:58-61`, `:150-160` |
| nitpick | `main.go` hand-copies `service`'s unexported `badgeCache` interface because it cannot name it, with a 4-line comment explaining the workaround | `main.go:62-71` |
| nitpick | `fmt.Errorf` with no format verbs where `errors.New` is the idiom | `config/config.go:262`, `:265` |

### Recommendations
- `low` — Make `EnsureIndexes` failures fatal, or gate readiness on them so an unindexed pod never takes traffic.
- `low` — Rename `cappedUnion`'s `cap` parameter and enable the `predeclared` linter so this class cannot recur.
- `low` — Add `bson` tags to the remaining `models/` DTOs, or mark the package wire-only in a comment.
- `low` — Re-run `make sast-vuln` and the registry packs from a network-permitted runner before shipping.
- `nitpick` — Route the two `threadunread.go` fan-outs through `fanOutChunks` so all per-site RPC concurrency obeys one knob; export `service.BadgeCache` and delete `main.go`'s structural copy.

---

## 3. Architecture — 4 / 5

Boundaries are unusually disciplined — every interface is consumer-defined, compile-time assertions guard drift, subjects come from `pkg/subject`, shutdown order is correct — but a re-declared shared Mongo pool knob, a duplicated unexported interface, and a synchronous non-durable cross-site fan-out keep it off 5.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | The HTTP Mongo client **re-declares `mongoutil.PoolConfig`'s knobs as three hand-written fields** instead of mounting the shared type with `envPrefix`. The justifying comment is *factually wrong*: it claims the tags "are fixed to the `MONGO_` prefix and cannot be reused", but `pkg/mongoutil/poolconfig.go:11-13` states the tags carry the full prefix precisely so a prefixed field works, and `CLAUDE.md` gives `Breaker mongoutil.BreakerConfig` + `envPrefix:"HISTORY_"` as the sanctioned form. **The concrete cost is a dropped knob**: `ServerSelectionTimeout` is only applied via `WithPool`, so the HTTP pool silently keeps the driver's 30 s default — the exact hang `poolconfig.go:33-40` exists to prevent, on the transport that serves the client-facing sidebar | `config/config.go:47-55`, `:196-198`; `main.go:137-144` |
| medium | Cross-site fan-out is a **synchronous, sequential, best-effort** JetStream publish inside the request handler: one blocking `PublishMsg` PubAck per destination, errors logged and swallowed. N remote sites add N × cross-gateway RTT to every mutation under a 15 s `HANDLER_TIMEOUT`, and a down peer permanently loses that site's replica. `CLAUDE.md` sanctions user-service as a direct publisher, so this is a documented gap rather than a rule break — but **the settings replica drives remote `notification-worker` push decisions**, so a lost event means a remote site keeps pushing muted notifications until the user next touches settings. This is the case OUTBOX was introduced for | `service/status.go:106-123`; `service/settings.go:143-160`; `service/chatlist.go:202-220`; `publisher/publisher.go:26-31` |
| medium | `badgeCache` is declared unexported in `service` yet is a constructor parameter of the exported `service.New`, forcing `main` to keep a hand-maintained structural copy whose own comment admits the workaround. Nothing fails the build if the two drift | `service/service.go:91-104`, `:159`; `main.go:63-70` |
| medium | `service.New` takes 14 positional dependencies plus variadic options, and `main` calls it **twice** with the argument list duplicated — the second call silently omits `WithPageBudget`. No compiler help on ordering between same-typed params (`pub, clientPub EventPublisher`) | `main.go:210-211`, `:217-222` |
| medium | `ALL_SITE_IDS` defaults to empty with **no validation and no startup log**, and `Load` never checks it or that `SiteID` is a member. An empty value makes every federation loop a silent no-op: federation is off with zero signal, while `SITE_ID` is correctly `notEmpty` | `config/config.go:70`, `:210-280` |
| low | No `bootstrap.go` / `Bootstrap` field despite publishing to JetStream. Behaviourally **correct** — it only ever publishes to remote INBOX lanes, which `inbox-worker` owns and this service must never create — but the gap deserves an explicit comment | `main.go:126` |
| low | Cohesion drift: `service/enrich_test.go` (846 lines) has no `enrich.go`; the fan-out/enrichment engine it tests lives inside `service/subscriptions.go:225-544` | — |
| nitpick | `startHTTPServer` (~60 lines of listener wiring) sits in `main.go` while `httpserver.go` holds only the 12-line `newHTTPServer` | `main.go:328-404` |

### Recommendations
- `high` — Replace `HTTPConfig`'s three pool fields and `validateMongoPool` with `Pool mongoutil.PoolConfig \`envPrefix:"HTTP_"\``; env names are unchanged, `Validate()` replaces the local copy, and the HTTP client picks up the 2 s `ServerSelectionTimeout` it currently lacks.
- `medium` — Route the settings and chatlist inbox events through the local OUTBOX (`outbox.Publish`), adding each type to exactly one `pkg/outbox` partition set; keep status direct if the LWW argument is judged sufficient. At minimum, fan out concurrently under the existing `MaxSiteFanout` semaphore instead of serially.
- `medium` — Export `service.BadgeCache` and delete the structural copy; collapse `service.New`'s dependency list into a named `service.Deps` struct and build the two instances by overriding one `Deps`.
- `medium` — Validate in `config.Load` that `SiteID` appears in `AllSiteIDs` when non-empty, and log the resolved peer list at startup so a federation-disabled deploy is visible.
- `low` — Extract `service/subscriptions.go`'s enrichment engine into `service/enrich.go`; add a comment at `main.go:126` recording why no `bootstrapStreams` exists.

---

## 4. Test coverage — 2 / 5

Statement-weighted coverage is **53.2% (1223/2300)** — below the §4 60% critical line. Two structural distortions are worth stating: `service/mocks/mock_repository.go` contributes **306 uncovered statements (13.3% of the denominator)** as generated non-`_test` code, and `mongorepo/` (421 stmts) is deliberately integration-only per §4. Excluding both, real unit coverage is **~72.7%** — still under 80%.

| Sev | Finding | Evidence |
|-----|---------|----------|
| critical | 53.2%, below the §4 60% line | `coverage_by_service.txt` |
| high | **All six client-facing chatlist RPCs are handler-untested** — `chatlist_test.go` exercises only pure helpers. `GetChatlist`, `Create/Delete/RenameChatlistSection`, `ReorderChatlistSections`, `SetChatlistSectionSortMode` are registered on `chat.user.{account}.…` yet no test drives one end-to-end: the `user not found` → `errcode.NotFound`, the store-error wrap, and the last-write-wins read-modify-write in `mutateChatlist` are all unexercised | `service/chatlist.go:27`, `:45`, `:74`, `:91`, `:116`, `:131` (45/153 stmts) |
| high | The cross-site chatlist replication fan-out is entirely uncovered. Its sibling `publishSettingsInbox` is at 81.8%; the chatlist lane's per-dest `InboxEvent` construction, `dest == s.siteID` skip and marshal-failure logging have never run in a test | `service/chatlist.go:200` (0%) |
| high | Both error paths of `GET /api/v1/subscriptions/count` have zero hits. The sibling `ListSubscriptions` has both branches covered, so this is an **asymmetry, not a pattern**; `integration_test.go` never hits the route either | `handler.go:80-83`, `:86-89` |
| medium | The local-site thread-list degrade branch is uncovered — the `history.GetThreadList` failure → `results[i].failed = true` path that decides whether a partial thread list is returned or a page silently loses a site. Its cross-site twin is at 79.2% | `service/threads.go:207-210` |
| medium | `historyclient.GetThreadList` is 0% while `RoomsGet` in the same file is 83.3%; the test file is untagged/unit, so this is a testable encode/decode path that was simply skipped | `historyclient/client.go:33` |
| medium | `RegisterHandlers` is 0% (29 stmts) — nothing asserts the 29 subject patterns match the client contract; a typo'd `subject.*Pattern` or dropped registration would ship green | `service/service.go:232` |
| low | `oidcValidator` is 0%. The runtime auth path itself is fine — `middleware.go` is **100%** covered including `WithCause`-on-invalid-token, expired-token and ambiguous-credential branches | `oidc.go:15` |
| low | `publishSubscriptionUpdate`'s publish-failure branch uncovered | `service/apps.go:116-118` |
| nitpick | `roomclient` (0/50), `presenceclient` (0/13), `publisher` (0/12) are integration-only — permitted, but none has a unit test for request marshalling | — |

**Load-bearing positives:** every integration file carries `//go:build integration` and a `TestMain` calling `testutil.RunTests(m)`; containers come exclusively from `pkg/testutil` with zero inline `GenericContainer`; no `time.Sleep`, no shared mutable state, no order dependence; mocks generated and confirmed non-stale.

### Recommendations
- `critical` — Exclude `service/mocks/` from the coverage denominator (build-tag or `-coverpkg` filter). It alone accounts for 13.3% of the shortfall and no amount of real testing will move it.
- `high` — Add table-driven handler tests for all six chatlist RPCs against `mocks.UserRepository`: nil-user → `NotFound`, `GetUserChatlist` error → wrapped infra error, duplicate-name rejection, non-permutation reorder.
- `high` — Cover `publishChatlistInbox` with a 3-site `allSiteIDs` and a stubbed `EventPublisher`, asserting one event per remote dest, self-site skipped, and a shared `Timestamp`.
- `high` — Extend the existing malformed-query / service-error tests to the `count` endpoint.
- `medium` — Stub a `history.GetThreadList` error so the degrade branch runs, asserting the page returns degraded rather than erroring; add a `historyclient.GetThreadList` unit test mirroring `RoomsGet`.
- `low` — Table-test `oidcValidator`'s unconfigured-issuer return, and assert `RegisterHandlers` registers the expected subject set against a fake router.

---

## 5. Maintainability — 3 / 5

The sub-package decomposition is coherent and the comment discipline exemplary (WHY not WHAT; zero TODOs, zero dead code), but the cross-site fan-out pattern has been hand-copied five times **with drift**, and two 850-line files plus a 14-positional-argument constructor make adding one dependency or one federated endpoint a multi-site edit.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | The per-site fan-out/degrade pattern is implemented **five separate times**, all doing "spawn per site, log `… site degraded`, mark failed, `wg.Wait()`" — and the copies **have already drifted**: `threadunread.go:58` is the only one with **no `s.fanout()` semaphore**, so it spawns one goroutine per owning site unbounded, silently ignoring `MAX_SITE_FANOUT`. A bug fixed in one copy will not reach the other four; one already missed the bound | `service/subscriptions.go:491`, `:812`; `service/threads.go:219`; `service/threadunread.go:58`, `:145` |
| medium | `service.New` takes 14 positional dependencies plus `*config.Config` and is called twice; the two calls **already diverge** — only the NATS instance gets `WithPageBudget`, with no comment saying the HTTP instance intentionally skips trimming | `service/service.go:180`; `main.go:220`, `:228` |
| medium | `service/subscriptions.go` (874 lines) carries five unrelated concerns: handlers, page clamping, NATS-payload split-retry, preview truncation, room-info mapping. **The tests have already split along seams the production code lacks** — `enrich_test.go` (846 lines) and `threadfit_test.go` (182) have no `enrich.go`/`threadfit.go` counterpart | `service/subscriptions.go:56`, `:107`, `:364-431`, `:433-475`, `:590-644` |
| medium | Three near-identical degrading wrappers over `GetAppsByAssistants` and two over `GetHRInfoByAccounts`, differing only in the log string; the distinct-name collectors are likewise duplicated | `service/subscriptions.go:162`, `:176`, `:191`; `service/threads.go:279`, `:293`, `:308`; `service/prioritycontacts.go:93` |
| medium | `mongorepo/subscriptions.go` (850 lines) is a repository **and** a query planner: ~250 lines of in-Go sorting/paging/cache policy share a file with the raw pipeline builders | `mongorepo/subscriptions.go:386`, `:413`, `:443`, `:477`, `:492` |
| medium | `CLAUDE.md` requires an inline `// $lookup justification:` on any `$lookup` you touch. `apps.go:92` and `threadsubscriptions.go:48` have one; **the four in `subscriptions.go` do not**, despite that file being heavily reworked | `mongorepo/subscriptions.go:96`, `:151`, `:673`, `:723` |
| low | `service.badgeCache` is unexported, forcing a structural copy in `main.go`. It is the **only** dependency absent from the compile-time assertion block at `main.go:44`, so a method added to one copy fails at the `service.New` call site rather than at the interface | `service/service.go:91`; `main.go:65` |
| nitpick | Four one-type client packages sit alongside the sanctioned set plus seven root `package main` files — the widest package surface of any service here | — |

### Recommendations
- `high` — Add `service/fanout.go` with one generic `forEachSite[T](ctx, sites, fanout, call) ([]T, []string)` returning results plus `UnavailableSites`; migrate all five call sites. **This alone fixes the unbounded goroutine spawn** at `threadunread.go:58`.
- `medium` — Replace `service.New`'s 14 positional parameters with a `service.Deps` struct; the two construction sites then differ only by the fields they intentionally override.
- `medium` — Split `service/subscriptions.go` into `subscriptions.go` (handlers), `enrich.go` (the file `enrich_test.go` already assumes) and `payloadsplit.go`.
- `medium` — Collapse the app/HR/priority-contact lookup wrappers into two helpers taking a `label string` for the degradation log, and one shared `distinctNamesByRoomType` collector.
- `medium` — Move the list-planning functions out of `mongorepo/subscriptions.go` into `mongorepo/listplan.go`; add the four missing `$lookup` justification comments.
- `low` — Export `service.BadgeCache`, delete the mirror interface, and add it to the assertion block.

---

## 6. Integration — 3 / 5

Subject construction, INBOX lane naming, `idgen` formats and `docs/client-api.md` coverage are all correct, but the client-fanout subject builders are inconsistent about account encoding, the cross-site fanout blocks the request path, and the chatlist fanout pair is entirely untested.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | `subject.SettingsUpdate` / `ChatlistUpdate` **do not call `EncodeAccount`**, unlike the sibling `SubscriptionUpdate`. `natsrouter` hands handlers the *decoded* dotted account, and both publish with it verbatim. For a `.bot` account (`weather.site-a.bot`) the subject spans four extra tokens and **never matches the recipient's JWT-scoped `chat.user.weather_site-a_bot.>`**. `docs/client-api.md:5692` documents this encoding requirement for `subscription.update` only — the invariant exists, it is just not applied here | `pkg/subject/subject.go:260` (encodes) vs `:266`, `:273`; `service/settings.go:124`; `service/chatlist.go:192` |
| medium | Chatlist's two fanouts have **zero** test coverage — no expectation on `subject.ChatlistUpdate` or `subject.InboxExternal` exists anywhere. Status, settings and apps all assert their subjects; chatlist is the outlier, so a subject or event-type regression on the **newest** federation path is invisible | `service/chatlist.go:190-224`; `service/chatlist_test.go` |
| medium | Cross-site INBOX publishing is synchronous and sequential **inside the request path** — each handler loops `ALL_SITE_IDS` doing a blocking `PublishMsg` (waits on PubAck) before returning. Every `status.set`/`settings.set` pays N × supercluster RTT, and a partitioned peer can consume `HANDLER_TIMEOUT` and fail an RPC whose local write already succeeded | `service/status.go:107-121`; `service/settings.go:136-158`; `service/chatlist.go:203-224` |
| medium | No durable retry for user-config federation: publish failures are logged and dropped. `CLAUDE.md` sanctions user-service as a direct publisher, so not a rule violation — but **settings are what each remote `notification-worker` reads locally to decide whether to push**, so one lost publish leaves a remote site pushing against stale mute preferences indefinitely. Status is genuinely LWW-benign; settings and chatlist are not | `service/settings.go:154-157`; `service/chatlist.go:218-221` |
| low | `SettingsUpdateEvent` and `ChatlistUpdateEvent` carry `Timestamp int64` with **no `bson` tag**. The load-bearing half is correct: every publish site sets it via `time.Now().UTC().UnixMilli()`, and settings/chatlist correctly derive **one** timestamp shared by the stored HWM and both fanouts | `pkg/model/usersettings.go:47-50`; `pkg/model/chatlist.go:68-71` |
| low | The six chatlist-section RPCs are documented only as a compact shape table rather than the mandated per-RPC field table with explicit types and a JSON example; every other user-service RPC follows the mandated style | `docs/client-api.md:5206-5211` |

### Verified clean
All 28 client subjects and `BadgeCountBatch` built via `pkg/subject` — no raw `fmt.Sprintf` anywhere. Every registered `chat.user.…` subject appears in both `docs/client-api.md` and `docs/client-api/request-reply.md`, and both fanout events appear in `docs/client-api/events.md` — **no derived-view drift**. `BadgeCountBatchPattern` is `chat.server.request.…` and correctly absent from client docs. INBOX lane is `chat.inbox.{destSiteID}.external.{eventType}` with self-site skipped. No `bootstrap.go` — correct, since `inbox-worker` owns INBOX and this service publishes to remote lanes only. IDs via `idgen.GenerateID()` 17-char base62 for `sso_tokens._id` and `idgen.GenerateUUIDv7()` for section ids. No JetStream consumer, so no `Nak`/`BackOff` surface.

### Recommendations
- `medium` — Route `SettingsUpdate` and `ChatlistUpdate` through `EncodeAccount`, matching `SubscriptionUpdate`; add a `pkg/subject` table case for a `.bot` account across all three builders so they cannot diverge again.
- `medium` — Add chatlist fanout test expectations asserting both subjects, the self-site skip and a non-zero shared timestamp, mirroring `status_test.go:74-160`.
- `medium` — Move the `ALL_SITE_IDS` fanout off the reply path: fan out concurrently under the existing `MAX_SITE_FANOUT` semaphore and reply as soon as the local write commits.
- `medium` — For settings and chatlist specifically, publish through the local OUTBOX instead of direct-to-remote-INBOX, so a gateway failure is durably retried rather than logged and dropped.
- `low` — Add `bson` tags to the two event structs; update `docs/client-api.md` §Chatlist Sections to the mandated field-table + JSON-example style in the same PR.

---

## 7. Performance — 4 / 5

Genuinely strong hot-path engineering on `subscription.list` — a TTL'd sort-key LRU, page-sized batch refetches instead of a join-and-sort, bounded per-site fan-out, precise projections — undercut by an unbounded serial per-account recompute in `badge.count.batch`, a few unprojected reads, and an unbounded phase-one scan.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `BadgeCountBatch` recomputes `unreadRooms` **serially, once per account, with no cap on `req.Accounts`**. Each miss is a Mongo aggregate (`$lookup` per active sub) plus cross-site `GetRoomsMeta` RPCs. `GetChannels` caps its account list via `MAX_ACCOUNT_NAMES`; this one does not — and it is the notification fan-in path. A cold cache on a large room walks accounts one at a time until the 15 s `HANDLER_TIMEOUT` runs out and the remainder silently degrade to absent badges | `service/badge.go:41`; cf. `service/subscriptions.go:664` |
| medium | `ListApps`' `$lookup` sub-pipeline has **no `$project` and no `$limit`** — only `$size > 0` is consumed, yet every matching botDM subscription document is materialized for every app on the page. The outer aggregation also has no terminal projection | `mongorepo/apps.go:95-106`, `:107` |
| medium | Phase-one subscription read is unbounded — no `$limit`, no cap. Every list request materializes one `subLite` per subscription the account holds and sorts them in Go, regardless of `page.Limit` | `mongorepo/subscriptions.go:299`, `:313-314` |
| medium | Unprojected reads on client-facing paths, against "always project precisely" | `mongorepo/subscriptions.go:838`; `mongorepo/apps.go:85`, `:140` |
| medium | The four `$lookup` sites in `subscriptions.go` carry no `// $lookup justification:` comment (the convention is live here — `apps.go:92` does it). The `hrUser` join at `:723` is additionally a `localField`/`foreignField` lookup with **no sub-pipeline projection**, pulling whole `users` documents to read three fields | `mongorepo/subscriptions.go:96`, `:151`, `:673`, `:723-731` |
| low | HTTP-path Mongo pool knobs re-declared instead of mounting `mongoutil.PoolConfig` — two pools against one cluster whose defaults can drift independently (see also Chapter 3) | `config/config.go:45-54` vs `:146` |
| low | Thread-unread fan-out spawns one goroutine per site with **no semaphore**, while every other fan-out bounds itself with `s.fanout()` | `service/threadunread.go:58-77` |
| nitpick | No L2 (`pkg/userstore`) tier for HR/user display lookups; `GetHRInfoByAccounts` hits Mongo on every list page. Mitigated by secondary routing and by being a single batched `$in` | `mongorepo/users.go:81` |

**Checked and clean:** no `time.Sleep` synchronization; no leaked goroutines (`fanOutChunks` acquires the semaphore *before* spawning); no JetStream consumers so no `Nak()`/`BackOff` exposure; `mongo.ErrNoDocuments` handled at every expected-miss site; indexes created at startup; `encoding/json` correct here (not a designated sonic worker).

### Recommendations
- `high` — Cap `req.Accounts` in `BadgeCountBatch` (reuse `MAX_ACCOUNT_NAMES`) and run the miss-path `unreadRooms` calls through the existing bounded `fanOutChunks` helper rather than serially.
- `medium` — Add `{"$project":{"_id":1}}` and `{"$limit":1}` to the `ListApps` co-subscription `$lookup`, plus a terminal `$project` naming only the `AppListItem` fields.
- `medium` — Bound the phase-one `Find` with `SetLimit` derived from `MAX_SUBSCRIPTION_LIMIT` plus headroom, so per-request cost tracks the page.
- `medium` — Add explicit projections to `GetAppSubscription`, `GetApp`, `GetAppsByAssistants`, and convert the `hrUser` join to a `let`/`pipeline` form projecting three fields.
- `medium` — Add the four `// $lookup justification:` comments.
- `low` — Mount `mongoutil.PoolConfig` with `envPrefix:"HTTP_"` and delete the hand-rolled fields; bound the `threadunread.go` goroutines with `s.fanout()`.

---

## 8. Prioritized action list

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Cap `req.Accounts` in `BadgeCountBatch` and run miss-path recomputes through the bounded fan-out helper | Performance | `service/badge.go:41` | Unbounded serial work on the **notification fan-in path**; a cold cache silently degrades to absent badges when the handler timeout expires. The cap already exists on a sibling RPC. |
| 2 | `high` | Mount `Pool mongoutil.PoolConfig` with `envPrefix:"HTTP_"` on `HTTPConfig`, deleting the three hand-rolled fields | Architecture / Perf | `config/config.go:47-55` | Not a style nit: the hand-rolled copy **drops `ServerSelectionTimeout`**, so the client-facing sidebar transport keeps the driver's 30 s default — the exact hang the shared type exists to prevent. The justifying comment is factually wrong. |
| 3 | `high` | Add one generic `forEachSite[T]` in `service/fanout.go` and migrate all five copies | Maintainability | `service/threadunread.go:58` | Five hand-copies have **already drifted** — one lost its semaphore and spawns unbounded goroutines. Fixing the abstraction fixes the live bug and prevents the next one. |
| 4 | `high` | Add handler tests for the six chatlist RPCs and both chatlist fanouts | Test coverage / Integration | `service/chatlist.go:27-131`, `:200` | Six **client-facing** RPCs and the newest federation lane have no end-to-end test at all; a subject or event-type regression ships green. |
| 5 | `medium` | Route `SettingsUpdate`/`ChatlistUpdate` through `EncodeAccount` | Integration | `pkg/subject/subject.go:266`, `:273` | A `.bot` account's settings/chatlist events are published on a subject its own JWT scope **cannot match** — silent non-delivery for bot accounts. |
| 6 | `medium` | Publish settings and chatlist through the local OUTBOX instead of direct-to-remote-INBOX | Integration / Architecture | `service/settings.go:143-160` | Settings drive each remote `notification-worker`'s push decision. A dropped publish leaves a peer pushing against stale mute preferences with no heal path. |
| 7 | `critical` | Exclude `service/mocks/` from the coverage denominator, then close the real gaps | Test coverage | `service/service.go:19` | 13.3% of the shortfall is generated code no testing can ever move; excluding it makes the remaining 72.7% → 80% gap actionable rather than demoralizing. |
| 8 | `medium` | Validate `SiteID ∈ AllSiteIDs` in `config.Load` and log the resolved peer list at startup | Architecture | `config/config.go:70` | An empty `ALL_SITE_IDS` turns **every** federation loop into a silent no-op — federation off with zero signal. |
| 9 | `medium` | Bound the phase-one subscription `Find`; project `ListApps` and the three unprojected reads | Performance | `mongorepo/subscriptions.go:299` | Per-request memory and sort cost currently scale with an account's **total** subscriptions rather than page size. |
| 10 | `medium` | Replace `service.New`'s 14 positional args with a `Deps` struct | Maintainability | `service/service.go:180` | The two call sites have already diverged silently on `WithPageBudget`, and same-typed neighbours (`pub, clientPub`) transpose without a compile error. |

### Verdict

**Ship-capable.** This is the strongest service in the fleet on code discipline — the secret-handling and error-wrapping review found nothing to fix, which is rare at 18.8k lines. The work here is about *durability and bounds*: item 6 is the one that loses user-visible state across sites, items 1–2 are capacity and latency cliffs on live paths, and items 3–4 are what keep the next change from introducing the fifth copy of a bug.
