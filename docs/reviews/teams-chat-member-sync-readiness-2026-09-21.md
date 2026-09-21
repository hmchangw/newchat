# teams-chat-member-sync — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `b12a503` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-chat-member-sync` is the strongest of the seven Teams CronJobs: 3.1 overall, with 4s in architecture, maintainability and performance. Its core logic (`syncer.go`) is at 98.6% coverage, the store has real testcontainer tests for every method including the optimistic-write path, files are small, there are no TODOs anywhere, and the hot path is clean — explicit projections, no `$lookup`, one batched `$in` per chat rather than a per-member N+1, and a lock that is never held across I/O.

The one architectural defect is that its optimistic-concurrency scheme can stall silently and forever. The conditional write is guarded by `{_id, updatedAt: seenUpdatedAt}` (`store_mongo.go:53`), but `updatedAt` is a whole-document write stamp that `teams-chat-sync` unconditionally `$set`s on *every* upsert of a non-1:1 chat. So an overlapping run that merely re-touches an active chat invalidates the token even though membership never changed. The write is skipped, counted as `Superseded`, logged at Warn — and deliberately excluded from `Failed`, so the run exits 0. A busy large group chat can therefore loop indefinitely without ever reaching `needCreateRoom=true`: no room is created, and no CronJob ever goes red. Two things make this worse rather than self-limiting: the token is read through a `SecondaryPreferred` client (`main.go:100`) while the compare runs on the primary, so replication lag manufactures additional spurious losses — each one paid for with a full Graph round trip against a throttled per-tenant budget — and the service wires no `pkg/obs.Init`, so `chatsSuperseded` exists only as a log field with no metric to alert on. The fix is to guard on a token that tracks actual membership change (Graph's `lastUpdatedDateTime`) and to fail the run when `Superseded > 0 && Succeeded == 0`.

The second theme is fleet-wide rather than local, and this service is one of five sharing it. The whole Microsoft Graph credential/proxy/TLS config block — tags, defaults and a 20-line comment — is copy-pasted per service instead of being declared once in `pkg/msgraph` and mounted as a named field, which CLAUDE.md §Configuration explicitly forbids. The predicted drift has already happened: `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults `true` here, in `teams-chat-sync` and in `teams-user-sync`, but `false` in `teams-hr-sync` and `user-presence-service/sync`. Because the same `http.Client` carries the OAuth POST to the public `login.microsoftonline.com`, a CronJob that simply omits the variable sends `GRAPH_CLIENT_SECRET` over an unverified connection. This service's own compose file ships `false`, so the insecure value only ever reaches production. gosec cannot see any of it — the `#nosec G402` sits in `pkg/msgraph`.

Two smaller correctness gaps: a graceful SIGTERM is reported as a mass failure, because neither the dispatch loop nor the worker loop selects on `ctx.Done()` — every remaining chat drains through a cancelled context, emits an Error line and increments `Failed`, so a routine eviction is indistinguishable from an outage (this same gap appears in `teams-chat-sync`). And a Graph member with no `userId` — a guest or anonymous `aadUserConversationMember` — is written through unfiltered as `{ID:"", Account:"", DisplayName:""}`, which `teams-room-creation` then copies verbatim into the room-create event.

Coverage reads 60.3%, but the number understates the service: the whole store layer is covered by properly-structured integration tests that the pipeline never runs, since no service pipeline in this repo passes `-tags integration`. Discounting `main.go`'s wiring, the tested surface is around 98%. The gate still fails, but the remedy is a merged profile plus a handful of unit gaps, not new tests.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 7 high, 17 medium, 14 low, 7 nitpick (45 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

