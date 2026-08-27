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
