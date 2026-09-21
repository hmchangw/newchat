# teams-room-verify — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `724d2ae` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.5, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-verify` is the audit stage of the Teams room pipeline: it reads chats flagged `needVerify`, asks each site's `teams-room-inspector` what the room actually looks like, and clears the flag when the two agree. It is well-made — 4s in code quality, architecture, maintainability and performance, zero TODOs, no file over 260 lines, a consumer-defined two-method store, and 100% statement coverage on `runner.go`, `client.go` and `config.go` with real error-path cases. It lands at 3.5, the highest score of the seven Teams CronJobs. The problem is what it verifies.

**The convergence check compares cardinality, not membership.** `runner.go:156` tests `SubscriptionCount != accountsPresent(c.Members)`, and the wire type cannot do better — `model.TeamsRoomVerifyResult` carries an `int` and no account set. So when `teams-chat-member-sync` swaps member A for member B, the count is unchanged; if `room-worker` has not yet applied the new roster the room still holds A's subscription and none for B, and the audit reports `ok`. A swap is the most common roster change there is. This is also why the verify lane cannot catch the fleet's worst Teams finding: against an empty published roster, 0 subscriptions versus 0 accounts verifies as converged. The fix is to carry `SubscribedAccounts []string` and diff the sets.

Second, the two pipeline stages are not mutually exclusive. `ListChatsNeedingVerify` filters on `needVerify: true` alone and does not even project `needCreateRoom`, so a chat still awaiting (re-)creation gets audited. The `updatedAt` compare-and-set does not close the window, because `teams-room-creation`'s `MarkRoomsCreated` sets `needCreateRoom:false, needVerify:true` *without* bumping `updatedAt` — so a token read before that write still matches. Verify can therefore clear the flag on a room it knows is about to be reconciled. Adding `needCreateRoom: {$ne: true}` to the filter and bumping `updatedAt` on the other side closes both halves.

Third, the flagged set is structurally non-draining. `MarkVerified` is the only writer that ever clears `needVerify`, and every failure path — site missing from `TEAMS_VERIFY_SITE_URLS`, call error, misrouted reply, omitted result, genuine mismatch — leaves the chat flagged with only a WARN. There is no attempt counter, no aging, no dead-letter. Migrated rooms are explicitly live and nothing forbids a native join or leave on one, so any such room mismatches forever. The scan that reloads them is an unbounded full materialisation with no limit, pulling every flagged chat's complete `members` array on every run, and `run` also discards the CAS `ModifiedCount`, so `chats_ok` counts chats *attempted* rather than cleared — in exactly the scenario the CAS exists for, the summary reports a clean run.

Two smaller but compounding gaps: the only HTTP hop in the whole pipeline propagates neither `traceparent` nor `X-Request-ID` (the job never calls `obs.Init`, so the propagator is a no-op, and no run id is minted), which means a mismatch WARN here can never be joined to the inspector log line that produced it; and one dead site starves every healthy one, because batches are grouped per site in input order and fed through a single global semaphore with a 30-second per-call timeout and no circuit-break.

Coverage measures 78.9% — the closest of the family to the floor, and the entire shortfall is `main.go`'s wiring plus a store whose integration tests exist, comply fully with CLAUDE.md §4, and run in no pipeline. Unit plus integration reaches ~86.5% before a single new test is written. The reviewer scored this dimension 3 and explicitly deferred to the rulebook; I applied the `<80%` cap mechanically so the service stays comparable with the other 34.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 5 high, 14 medium, 19 low, 7 nitpick (45 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

