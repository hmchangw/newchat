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
