# Production Readiness Review — `user-service`

| | |
|---|---|
| **Service** | `user-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/user-service-production-readiness-zlyo7o` |
| **Commit** | `e7e94c9` |
| **Overall score** | **3.3 / 5** |
| **Method** | Six independent expert agents, each reading `CLAUDE.md` + the full service + its `pkg/` dependencies |

## TL;DR

`user-service` is a mature, well-engineered Go service whose *code* is among the best in the
repo — `make lint` is clean across the whole repo, `gosec` reports zero findings, every NATS
subject goes through `pkg/subject` builders, every store interface is consumer-defined and
role-scoped, and all 28 client-facing RPCs are documented in `docs/client-api.md` with matching
schemas and `reason` wire values. What holds it back from a shipping grade is not defect density
but three structural pressures. First, **measured unit coverage is 52.6%**, far below the repo's
80% floor — six client-facing `chatlist.*` RPCs and the entire `roomclient`/`presenceclient`/
`publisher` trio have no unit tests at all (see the material Docker caveat in Chapter 4).
Second, the service layer has consolidated into a **god object**: one `UserService` struct with
66 methods across 9 unrelated domains, built by a 15-parameter positional constructor that is
called twice with a silent divergence. Third — and flagged **independently by three of the six
experts** — the cross-site federation path for status/settings/chatlist does **serial, blocking,
lossy JetStream publishes inside the user's request handler**, the exact failure mode the OUTBOX
lane was introduced to eliminate. None of these is a reason to stop a deploy today; all three are
reasons this service should not absorb its next feature before they are addressed.

## Dimension scores

| # | Dimension | Score |
|---|---|---|
| 1 | Go code quality | 4.5 / 5 |
| 2 | Architecture | 4.0 / 5 |
| 3 | Test coverage | 1.0 / 5 |
| 4 | Maintainability | 3.0 / 5 |
| 5 | Integration | 4.0 / 5 |
| 6 | Performance | 3.5 / 5 |
| | **Average** | **3.3 / 5** |

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 8 |
| `medium` | 20 |
| `low` | 15 |
| `nitpick` | 11 |
| **Total** | **55** |

The single `critical` is the coverage floor breach. Of the 8 `high` findings, 3 are performance
hot paths (badge recompute, badge `$lookup` over-fetch, unbounded `subscription.list` scan),
2 are maintainability structure (god object, 15-param constructor), 2 are test gaps (untested
chatlist RPCs, untested `RegisterHandlers`), and 1 is a cross-service config divergence in the
badge cache.

## Convergent findings

Three experts independently reached the same conclusion from different angles, which raises
confidence well above any single agent's judgment:

- **Synchronous cross-site federation** (`service/status.go:101`, `service/settings.go:138`,
  `service/chatlist.go:202`) was flagged by Architecture, Integration, *and* Performance — as a
  durability gap, a correlation gap, and a latency gap respectively.
- **`GetThreadUnreadSummary`'s unbounded fan-out** (`service/threadunread.go:59`) was flagged by
  Code quality, Maintainability, *and* Performance.
- **Missing Mongo projections** on `GetAppsByAssistants` (`mongorepo/apps.go:140`) were flagged
  by Code quality *and* Performance.

---

# Chapter 2 — Go Code Quality

**Score: 4.5 / 5**

Exceptional Go by large-scale-microservice standards: consumer-defined interfaces, constructor
DI, wrapped errors at every layer, disciplined `errcode` tiering, slog-only structured logging
with zero secret leakage, and comments that explain *why* rather than *what*. The findings below
are small deviations from the binding spec, not defects.

## Tooling results

| Gate | Result |
|---|---|
| `make lint` (golangci-lint v2.11.4) | **0 issues**, whole repo |
| `gosec` (medium+, confidence medium) | **PASS — 0 findings** |
| `govulncheck` | **Could not run** — sandbox proxy returns 403 CONNECT for `vuln.go.dev` |
| `semgrep` (repo-local rules) | **0 findings** — 10 rules, 253 files |
| `semgrep` (registry rulesets `p/golang`, `p/security-audit`) | **Could not run** — `semgrep.dev` proxy-blocked |

**No medium+ SAST finding touches `user-service`.** Two caveats must be carried forward: the
`govulncheck` dependency-vulnerability scan and semgrep's registry rulesets are network-blocked
in this environment and were **not** assessed. Both are blocking CI gates per CLAUDE.md §5 and
must be re-run in CI before shipping — this audit cannot substitute for them.

## Findings

**`medium` — Three Mongo reads ship no projection**, against CLAUDE.md §6 ("every
find/aggregation MUST specify an explicit projection"):
- `mongorepo/subscriptions.go:815` — `GetAppSubscription` decodes a whole `model.Subscription`
  to read two fields.
- `mongorepo/apps.go:140` — `GetAppsByAssistants` pulls whole app docs on the sidebar hot path,
  called once per list / thread / priority-contact request.
- `mongorepo/apps.go:85` — `GetApp`.

Every other read in the package is precisely projected, so this reads as oversight rather than
intent. (Independently corroborated by the performance expert — see Chapter 7.)

**`low` — Bare `return err`** at `main.go:308` (the `httpSrv.Shutdown` result inside the shutdown
step), against CLAUDE.md §3 "Never return bare `err`". Note the bare returns at
`middleware.go:115`, `service/prioritycontacts.go:122` and `service/settings.go:59` are
**correct** — they forward an already-typed `*errcode.Error`, which wrapping would collapse to
`internal`.

**`low` — Unbounded fan-out in one handler.** `service/threadunread.go:59-79`
(`GetThreadUnreadSummary`) spawns one goroutine per site with no semaphore, while every sibling
fan-out bounds by `s.fanout()`: `ClearAllThreadUnread` (`threadunread.go:148`),
`enrichCrossSiteThreads` (`threads.go:224`), `fanOutChunks` (`subscriptions.go:496`),
`unreadRooms` (`subscriptions.go:806`). Bounded in practice by `ALL_SITE_IDS`, but it ignores
`MAX_SITE_FANOUT` and is the one path an operator cannot throttle.

**`low` — Double-logged degradation.** `roomsGetSplitting` logs `"split branch degraded"` per
failed half (`subscriptions.go:395`) while the enclosing `enrichLastMessage` callback logs
`"last-message enrichment degraded"` for the same chunk failure (`subscriptions.go:555`) — two
WARN lines per incident, inflating the alerting signal.

**`low` — `WithPageBudget` is wired but consumed by exactly one handler**: `service/threads.go:120`
(`thread.list`). `subscription.list` — the largest reply, up to `MAX_SUBSCRIPTION_LIMIT=1000` rows
with previews — is never fit-trimmed, so `PAGE_TRIMMING_ENABLED` and main.go's
`"page trimming DISABLED"` warning (`main.go:225`) read as global while covering one RPC.
Behaviour is safe (`errnats` converts overflow to `response_too_large` and clients retry over
HTTP, per the comment at `subscriptions.go:372-376`); the issue is the misleading knob.

**`nitpick`** — `cappedUnion(ids []string, trigger string, cap int)` at `service/badge.go:67`
shadows the builtin `cap`.

**`nitpick`** — Legacy `sort.Slice` / `sort.SliceStable` at `service/threads.go:102` and
`mongorepo/subscriptions.go:398`, and a hand-rolled `chunkStrings` at
`service/threadunread.go:179`, where the codebase already uses the modern idiom (`slices.Chunk`
at `mongorepo/subscriptions.go:323`).

**`nitpick`** — `main.go:229-239` claims the HTTP service instance runs "over the HTTP-only Mongo
pool… everything else is shared and stateless", but `threadSubRepo` and `ssoTokenRepo` are handed
in still bound to the **NATS** pool. Harmless today (the HTTP route only reaches
`ListSubscriptionsFor`) but the comment will mislead the next editor.

**`nitpick`** — `service/status.go:44` `GetProfileByName` duplicates `GetStatusByName` (`:19`)
verbatim; the "may diverge later" rationale is stated, but delegating would keep them in step
until it does.

## Recommendations

1. **`medium`** — Add explicit projections to `GetAppSubscription`, `GetAppsByAssistants` and
   `GetApp`. `GetAppsByAssistants` is the highest-value one (hot path, three call sites).
2. **`high`** — Re-run `govulncheck` and the semgrep registry rulesets in CI, where the network
   is available; they are blocking gates and were unassessable here.
3. **`low`** — Wrap `main.go:308` as `fmt.Errorf("shutdown http server: %w", err)`.
4. **`low`** — Bound `GetThreadUnreadSummary`'s per-site goroutines with
   `sem := make(chan struct{}, s.fanout())`, matching `ClearAllThreadUnread` twenty lines below.
5. **`low`** — Drop one of the two degradation logs on the last-message split path; keep the leaf
   `roomsGetSplitting` one, which carries `chunk_size`.
6. **`low`** — Either apply `pagefit.Fit` to `subscription.list`, or narrow the config comment and
   startup warning to name `thread.list` explicitly.
7. **`nitpick`** — Rename `cap` → `limit` in `cappedUnion`; migrate the two `sort.*` calls to
   `slices.SortFunc` and `chunkStrings` to `slices.Chunk`; correct the `httpSvc` comment in
   `main.go` to say which repos are *not* on the HTTP pool.

---

# Chapter 3 — Architecture

**Score: 4.0 / 5**

Solid, disciplined architecture with above-house-average shutdown and DI hygiene. The deductions
are cross-service config coherence and a lossy federation path — not layering.

## Verified clean

- **Subjects** — zero `fmt.Sprintf` on subjects in non-test code; every subject comes from
  `pkg/subject` (`service/service.go:233-261`, `roomclient/client.go:44,67,89,109`,
  `presenceclient/client.go:36`, `historyclient/client.go:38,64`).
- **Consumer-defined interfaces** — all ten in `service/service.go:20-124`; the HTTP handler's own
  slice in `store.go:13`. Compile-time assertions at `main.go:45-59`.
- **No JetStream consumer exists** (no `.Consume(` / `.Messages()` anywhere), so the
  consumer-pattern rules are N/A — `user-service` is request/reply + publisher only.
- **Config discipline** — a single `env.ParseAs` at `config/config.go:179`, no `os.Getenv`, 20+
  cross-field validations.
- **Shutdown** (`main.go:298-330`) — readiness flip → bounded HTTP drain + `cancelInFlight` →
  `router.Shutdown` → `natsutil.Drain` → Mongo → Valkey → health → o11y. Correct ordering, and the
  self-SIGTERM on listener death (`main.go:398-404`) is better than the peer service.

## Findings

**`high` — Badge-cache writer contract diverges across services.** `main.go:213` builds the cache
with `WithMarkerTTL(10m)` and `BADGE_COUNT_CAP`; the other two writers of the same Valkey keyspace
build it with neither — `room-service/main.go:300` and `inbox-worker/main.go:862` both call
`badgecache.New(valkeyClient, cfg.BadgeCacheTTL, badgecache.DefaultMaxCount)`, so their marker
inherits `ttl` (24h, `pkg/badgecache/badgecache.go:106`). `config/config.go:124-128` states the
marker's lifetime *is* the maximum badge staleness; a peer's `Seed` therefore marks the set fresh
for 24h, not 10m. That silently invalidates the premise of `BADGE_COUNT_CACHE_FIRST`
(`config/config.go:133-137`). `BADGE_COUNT_CAP` agrees only by coincidence of defaults (both 10)
and diverges the moment ops tunes it.

**`medium` — Cross-site federation is direct, lossy, and in the request path.**
`service/status.go:117`, `service/settings.go:154`, `service/chatlist.go:218` loop `allSiteIDs`
and blocking-`PublishMsg` into each remote INBOX, logging failures as WARN. CLAUDE.md does list
`user-service` as a sanctioned direct publisher, but `room-service`'s request/reply federation was
moved behind OUTBOX for exactly this loss mode. A down gateway drops a settings/chatlist replica
until the user's *next* mutation, and N serial PubAcks run inside the 15s `HandlerTimeout`
(`main.go:257`) on the caller's critical path. *(Independently flagged by Integration and
Performance — see Chapters 6 and 7.)*

**`medium` — A duplicated service instance duplicates unowned state.** `main.go:226` and
`main.go:234` build two `UserService`s; each `NewSubscriptionRepo` allocates its own LRU
(`mongorepo/subscriptions.go:67` → `mongorepo/sortkeycache.go:35`), so
`SUBS_SORTKEY_CACHE_SIZE=100000` costs 2×100k entries, and both register the same
`cachemetrics.For("sub_sortkey","l1")` series, blending two hit rates. `httpSvc` also silently
omits `WithPageBudget` and reuses `threadSubRepo`/`ssoTokenRepo` bound to the *NATS* pool —
undermining the isolation `main.go:229-233` claims if the HTTP surface grows.

**`medium` — Overload knobs bypass the shared house type.** `pkg/natsrouter/guard.go` exists
(`GuardConfig`, `DefaultGuarded`) precisely so a service cannot wire half the protection;
`main.go:243-257` hand-assembles it against bespoke `MAX_CONCURRENCY` / `HANDLER_TIMEOUT` fields
(`config/config.go:72,92`). The fleet now has two operator-facing names for one knob
(`REQUEST_TIMEOUT` elsewhere vs `HANDLER_TIMEOUT` here, plus `HTTP_HANDLER_TIMEOUT`).

**`medium` — The domain layer depends on the process config struct.** `service/service.go:13,180`:
`New(..., cfg *config.Config, ...)` — 14 positional params, of which `pub, clientPub
EventPublisher` are adjacent and identically typed. Transposing them compiles and silently sends
federation events over unpersisted core NATS and client events over JetStream. The peer
(`room-service/handler.go:90`) passes discrete values and imports no config package.

**`low` — No `bootstrap.go` / `Bootstrap bootstrapConfig`.** Correct in effect (INBOX is
`inbox-worker`'s, and a remote stream cannot be verified across the gateway), but there is no
startup assertion of any kind; contrast `room-service/bootstrap.go:56`, which fail-fasts on a
missing stream. A mis-provisioned INBOX surfaces only as runtime WARNs.

**`low` — The hybrid layout blurs the sanctioned exception.** The sub-package layout (`config/`,
`models/`, `mongorepo/`, `service/`, `service/mocks/`) coexists with a flat root transport layer.
Defensible for a second (HTTP) transport, but `store.go:13` holds a service-layer interface, not a
store, and `main.go:66-85` re-declares `badgeCache` plus a `noopBadgeCache` purely because
`service.badgeCache` is unexported. `startHTTPServer` (`main.go:347`) belongs in `httpserver.go`.

## Recommendations

1. **`high`** — Add `BADGE_MARKER_TTL` (and `BADGE_COUNT_CAP`) to `room-service` and
   `inbox-worker` and pass `badgecache.WithMarkerTTL`; keep `BADGE_COUNT_CACHE_FIRST=false` until
   then.
2. **`medium`** — Route `user_status_updated` / `user_settings_updated` / `user_chatlist_updated`
   through OUTBOX via `outbox.Publish` (concurrent partition), or at minimum move the fan-out off
   the request path.
3. **`medium`** — Split the sort-key cache (and any shared repo) out of the second `service.New`,
   or share one `SubscriptionRepo` cache between instances; give `httpSvc` the same
   `WithPageBudget` treatment or document the asymmetry in code.
4. **`medium`** — Replace the hand-rolled router options with `natsrouter.GuardConfig` +
   `DefaultGuarded`, aliasing `HANDLER_TIMEOUT` → `REQUEST_TIMEOUT` for one deploy cycle.
5. **`medium`** — Convert `service.New` to an options struct, removing the `*config.Config` import
   from `service/` and the adjacent same-typed publisher hazard.
6. **`low`** — Add `bootstrap.go` with the standard `Bootstrap bootstrapConfig` that verifies the
   *local* `INBOX-{siteID}` (or explicitly documents why none is needed) so misprovisioning fails
   at startup.
7. **`low`** — Rename `store.go` → `service_iface.go`, export `service.BadgeCache` to delete the
   `main.go` duplicate, and move `startHTTPServer` into `httpserver.go`.

---

# Chapter 4 — Test Coverage

**Score: 1.0 / 5**

> **Score is mechanically floored** by CLAUDE.md §4: measured total coverage is 52.6%, below the
> 60% threshold that mandates a `critical` finding and a score of 1. **On test *quality* alone the
> `service` and root packages would rate 4/5.** Read the caveat below before acting on the number.

## `make test SERVICE=user-service` — PASS

```
ok  .../user-service          1.105s     ok  .../user-service/mongorepo  1.055s
ok  .../user-service/config   1.073s     ?   .../presenceclient  [no test files]
ok  .../historyclient         1.169s     ?   .../publisher       [no test files]
ok  .../models                1.027s     ?   .../roomclient      [no test files]
ok  .../user-service/service  1.159s
```

`make generate SERVICE=user-service` produced **no diff** — mocks are current, not stale. The
working tree was left clean.

## Numeric coverage

| Package | Covered / total stmts | % |
|---|---|---|
| `user-service` (root) | 60/229 | **26.2%** |
| `user-service/config` | 67/70 | 95.7% |
| `user-service/historyclient` | 11/25 | 44.0% |
| `user-service/models` | — | no statements |
| `user-service/mongorepo` | 68/417 | **16.3%** |
| `user-service/presenceclient` | 0/13 | **0.0%** |
| `user-service/publisher` | 0/8 | **0.0%** |
| `user-service/roomclient` | 0/50 | **0.0%** |
| `user-service/service` | 994/1165 | 85.3% |
| `user-service/service/mocks` (generated) | 0/306 | 0.0% |
| **TOTAL** | **1200/2283** | **52.6%** |
| TOTAL excluding generated mocks | 1200/1977 | 60.7% |

### Material caveat — read this before acting

**Docker was unavailable in the audit environment, so `-tags integration` could not run.**
`mongorepo` (88 integration tests), `roomclient`, `presenceclient`, `publisher` and the root
`integration_test.go` are covered *only* under that tag. `go vet -tags=integration
./user-service/...` compiles clean. **The integration-inclusive figure is very likely ≥80%.**

The genuinely actionable finding is therefore less "this service is undertested" than **"nobody
measures it"**: the repo has no coverage gate wired for `user-service` (`tools/coveragecheck` is
used only by `tools/loadgen`), and the reported number counts 306 statements of generated mocks.
Both distortions should be fixed before the 52.6% is treated as a quality verdict.

## Findings

**`critical`** — Coverage below repo minimum 80%, currently **52.6%**. Generated mocks
(`service/mocks/mock_repository.go`, 306 stmts) are counted, no `-exclude` is applied, and no gate
exists.

**`high` — Six client-facing chatlist RPCs are entirely untested.** `service/chatlist.go:27`
`GetChatlist`, `:45` `CreateChatlistSection`, `:74` `DeleteChatlistSection`, `:91`
`RenameChatlistSection`, `:116` `ReorderChatlistSections`, `:131` `SetChatlistSectionSortMode`,
plus `:153` `mutateChatlist` and the `:190`/`:200` publish paths — all 0.0%.
`service/chatlist_test.go` tests only pure helpers (`validateSectionName`, `isPermutation`). All
six are registered at `service/service.go:242-247` as `chat.user.*` handlers, so CLAUDE.md §4
("every handler method must have tests for valid input, invalid input, store errors, and edge
cases") is unmet. **This gap is real regardless of the Docker caveat** — these are unit-testable
against existing mocks.

**`high` — `service/service.go:232` `RegisterHandlers` at 0.0%.** ~40 subject registrations; a
dropped or mistyped subject ships silently. Cheap to assert against a fake router.

**`medium`** — `historyclient/client.go:33` `GetThreadList` is 0.0% while its sibling `RoomsGet`
(`:59`) is at 83.3%, well tested with an embedded in-process NATS server
(`historyclient/client_test.go:25` `startTestNATS`). The pattern exists and is simply unused for
the other method.

**`medium`** — `roomclient` / `presenceclient` / `publisher` have zero *unit* tests
(`roomclient/client.go:27-100`, `presenceclient/client.go:28,31`, `publisher/core.go:20,24`,
`publisher/publisher.go:21,26`). They rely wholly on Docker-gated integration tests, so a normal
`make test` run validates none of their marshalling or error mapping. The `startTestNATS`
embedded-server pattern would cover these without Docker.

**`medium`** — `oidc.go:15` `oidcValidator` at 0.0%. The unconfigured branch (returns
`nil, nil, nil`) and the `TLSSkipVerify` warning are security-relevant and testable with no
network.

**`low`** — `service/chatlist_test.go` is the only non-generated test file not using testify
(49/54 do), and uses no `t.Run` subtests and no table-driven cases despite testing 5+ input
variations per function.

**`low`** — TDD Red phase is unverifiable: history is squashed PR merges. Every commit does ship
tests alongside implementation (`2d7bf69` 2 impl/1 test, `834e837` 15/11, `e793090` 22/11) — good
discipline, but not evidence of test-first.

**`nitpick`** — Naming drifts from `Test<Type>_<Method>_<Scenario>` (e.g. `service/status_test.go:58`
`TestGetProfileByName_StoreError` omits the type).

**`nitpick`** — `newSvc(t)` returns 7 positional values (`service/status_test.go:59`); every call
site carries `_, _, _, _` noise. A struct return would survive dependency additions.

**`nitpick`** — No `t.Parallel()` anywhere in the service; suite runs are serial.

## What is genuinely good

Integration infrastructure is **fully compliant**: `TestMain` + `testutil.RunTests` in all five
integration packages, zero inline `testcontainers.GenericContainer`, per-test
`testutil.MongoDB(t, prefix)` isolation, compile-time interface assertions at
`mongorepo/setup_test.go:18`. No `time.Sleep`, no shared mutable state, no order dependence;
`config/config_test.go:15` `unsetEnv` correctly restores prior env. Error-path depth in `service`
is strong (`status_test.go:58,79,86,93` store/publish failures; `threads_test.go:217,255,310`
assert graceful degradation).

## Recommendations

1. **`critical`** — Wire a `coveragecheck` gate for `user-service` in CI mirroring
   `coverage-loadgen-*`, at `-min 80` with `-exclude service/mocks`, measured **with
   `-tags integration`** so the number reflects what actually ships. Until this exists the true
   figure is unknown to everyone, not just this audit.
2. **`high`** — Write table-driven tests for the six `service/chatlist.go` RPCs against
   `service/mocks`: happy path, invalid name, duplicate name, unknown section ID, non-permutation
   reorder, and repository error → `errcode.CodeInternal`.
3. **`high`** — Test `RegisterHandlers` (`service/service.go:232`) by registering into a
   `natsrouter` and asserting the exact set of subjects, so a lost registration fails the build.
4. **`medium`** — Extract `startTestNATS` (currently `historyclient/client_test.go:25`) into a
   shared test helper and use it to unit-test `roomclient`, `presenceclient`, `publisher` and
   `historyclient.GetThreadList` without Docker.
5. **`medium`** — Add two cheap unit tests for `oidc.go:15`: unconfigured issuer returns
   `(nil, nil, nil)`, and a bad issuer URL returns a wrapped `init oidc validator` error.
6. **`low`** — Rewrite `service/chatlist_test.go` onto testify + `t.Run` subtests with named
   cases, matching the other 49 files.
7. **`low`** — Convert `newSvc(t)` to return a struct of dependencies to stop the positional-blank
   churn.

---

# Chapter 5 — Maintainability

**Score: 3.0 / 5**

Unusually well-commented code with **zero dead code and zero TODO/FIXME debt** — but the service
layer has consolidated into a god object, and the same concurrency and lookup skeletons are now
written three to five times each. The service is refactor-*ready*, not refactor-*urgent*.

## Findings

**`high` — `UserService` is a god object spanning 9 unrelated domains.**
`service/service.go:127-169` — 28 struct fields; **66 methods** (31 exported) across 12 files;
**29 RPCs** registered in one `RegisterHandlers` (`service.go:232-262`); **12 dependency
interfaces** in one file. It began as subscriptions + status and now carries apps, threads,
thread-unread, chatlist, priority contacts, settings, SSO and badge. Nothing forces cohesion —
`SSOSet` and `ListSubscriptions` share a struct only by accident.

**`high` — 15-parameter positional constructor, called twice with a silent divergence.**
`service/service.go:180` takes 15 positional args, 13 of them interfaces. `main.go:226` and
`main.go:234` both call it; the HTTP instance omits `service.WithPageBudget(pageBudget)` with no
comment explaining why (`main.go:229-239` explains only the Mongo pool). Adding a 14th dependency
means editing a signature and two 6-line call sites, and any future option must be re-decided at
both — a divergence the compiler cannot catch.

**`medium` — The bounded site fan-out skeleton is hand-rolled 5 times.**
`subscriptions.go:495` (the generic `fanOutChunks`), `subscriptions.go:805`, `threads.go:223`,
`threadunread.go:147` and `threadunread.go:58`. All are `wg + sem + failed[i] + ctx.Err()`
recheck. Worse, `threadunread.go:58` (`GetThreadUnreadSummary`) has **no semaphore at all** — it
ignores `s.fanout()` and spawns one goroutine per site unbounded, an inconsistency that survives
only because nobody can diff five copies. *(Independently flagged by Code quality and
Performance.)*

**`medium` — Degrade-to-nil lookup helpers triplicated.** `GetAppsByAssistants` is wrapped
identically at `subscriptions.go:162` (`lookupApps`), `threads.go:293` (`lookupThreadApps`) and
`prioritycontacts.go:93` (`lookupPriorityContactApps`) — same guard, same warn, same nil return,
only the log string differs. `GetHRInfoByAccounts` is doubled at `subscriptions.go:176` and
`threads.go:279`. Likewise `distinctListNames` (`subscriptions.go:186`) vs `distinctDMAndBotNames`
(`threads.go:308`).

**`medium` — Cross-site INBOX fanout loop copy-pasted 3×.** `status.go:101-124`,
`settings.go:138-158` and `chatlist.go:202-222` are ~20 identical lines each (skip-self, build
`model.InboxEvent`, marshal, publish, warn). Only the event type and payload vary. A fourth event
type means a fourth copy.

**`medium` — `service/subscriptions.go` (867 lines) mixes six concerns.** Transport handlers, page
normalization, room enrichment, chunk planning, a recursive payload-splitting retry
(`roomsGetSplitting:364`), rune truncation, a generic fan-out primitive, and the badge unread
computation (`unreadRooms:771`, 97 lines, 5 nesting levels) all live in one file. `unreadRooms` is
also the only fan-out that *doesn't* use `fanOutChunks`, sitting 275 lines above it.

**`medium` — No complexity guard in CI.** `.golangci.yml` enables no `gocyclo` / `cyclop` /
`funlen` / `gocognit`. Nothing stops `main()` (242 lines, `main.go:86`) or `config.Load()`
(98 lines, ~35 branches, `config/config.go:178`) from growing further.

**`low` — Persistence type leaks into the service interface.** `mongoutil.OffsetPageRequest` /
`OffsetPageHasMore` appear in `service.go:21,22,52` and `subscriptions.go:107` — a Mongo-named
package in a consumer-defined domain interface. Harmless today, awkward if a repo is ever not
Mongo.

**`low` — `$lookup` without the mandated justification comment.** CLAUDE.md requires
`// $lookup justification: …` when touching a `$lookup`. `mongorepo/apps.go:92` and
`threadsubscriptions.go:48` comply; `mongorepo/subscriptions.go:98` (`roomsEnrichStages`), `:145`
(`roomMatchStages`) and `:715` (`GetDMSubscription`) do not.

**`low` — Near-identical 12-field room projections duplicated.** `mongorepo/subscriptions.go:100-112`
and `:154-166` list the same fields; drift between them silently breaks one path.

**`nitpick`** — `chunkStrings` (`threadunread.go:178`) reimplements `slices.Chunk`, already used
at `subscriptions.go:334`.

**`nitpick` — The hybrid layout is undocumented.** HTTP transport lives in flat root files
(`handler.go`, `routes.go`, `store.go`, `middleware.go`); NATS handlers live in `service/`.
CLAUDE.md's exception says sub-packages are used *instead of* flat files. A newcomer will look for
the chatlist handler in `handler.go`.

## Ease of adding a new user-scoped RPC

Traced `chatlist.section.setSortMode` end-to-end: **8 files** — `pkg/subject/subject.go`,
`pkg/errcode/codes_user.go`, `models/chatlist.go`, `service/chatlist.go`, `service/service.go`
(register + possibly interface), `mongorepo/*.go`, `service/mocks/` (via `make generate`), plus
`docs/client-api.md` and its two derived views. The path is **obvious and mechanical** — this is
the codebase's biggest strength. Cost appears only when the RPC needs cross-site fan-out or a
degrading lookup, where the newcomer must pick among five near-identical prior arts.

## Store interfaces — done right

**Not fat.** Instead of one `UserStore` there are 8 role-scoped, consumer-defined interfaces
(`SubscriptionRepository` 8 methods, `UserRepository` 12, `AppRepository` 4,
`ThreadSubscriptionRepository` 1, `RoomClient` 5, `HistoryClient` 2, `PresenceClient` 1,
`SSOTokenRepository` 2), each with a doc comment, plus compile-time assertions at
`main.go:46-59`. `UserRepository` at 12 methods is the only one drifting toward fat.

## Recommendations

1. **`high`** — Convert `service.New` to a `Deps` struct (or functional options); delete the
   duplicated 6-line call site at `main.go:234` in favour of a `withHTTPPool(deps)` derivation so
   options cannot silently diverge.
2. **`high`** — Split `UserService` along its existing file seams into 3–4 structs sharing a small
   embedded `base` (siteID, allSiteIDs, pub, clientPub, fanout): `subscriptionService`,
   `threadService`, `profileService` (status/settings/chatlist/contacts), `ssoService`.
   `RegisterHandlers` becomes a per-struct method.
3. **`medium`** — Extract one `fanOutSites[T](ctx, sites, maxFanout, call)` primitive and delete
   the four hand-rolled copies; this also fixes the unbounded goroutines at `threadunread.go:58`.
4. **`medium`** — Extract `lookupOrDegrade[K,V](ctx, keys, fn, what)` and
   `publishInboxFanout(ctx, evType, payload, now)`; removes ~120 duplicated lines across 6 files.
5. **`medium`** — Split `service/subscriptions.go`: move `fanOutChunks`/`planChunks`/`chunkJob` to
   `fanout.go`, `roomsGetSplitting`/`truncatePreviews` to `preview.go`, and `unreadRooms` to
   `badge.go` (its only two callers are `CountSubscriptions` and `BadgeCountBatch`).
6. **`medium`** — Enable `gocyclo` (threshold 15) and `funlen` (80 lines) in `.golangci.yml`, with
   `main()` and `config.Load()` explicitly excluded, so the current five >80-line functions become
   the ceiling rather than the trend.
7. **`low`** — Add `// $lookup justification:` to the three sites in `mongorepo/subscriptions.go`,
   and factor the duplicated room projection into one `roomProjectionFields()` used by both
   pipeline builders.
