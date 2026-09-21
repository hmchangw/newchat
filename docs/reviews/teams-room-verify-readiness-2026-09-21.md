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


## 2. Code quality — score 4

### Evidence

- [medium] SIGTERM does not actually stop the pass — `teams-room-verify/runner.go:78-86` — the batch loop spawns a goroutine for every planned batch with no `ctx.Done()` check, so after cancellation each remaining batch still runs `r.verify` (which fails on the dead ctx) and emits a warn + `failedBatches` stat. This contradicts main.go:68-70's comment that the job "aborts between operations"; on a large backlog a pod deletion produces one warn line per remaining batch and inflates the per-site failure stats.
- [medium] No run-level correlation ID, and none propagated to the inspector — `teams-room-verify/main.go:30-36`, `teams-room-verify/client.go:26-30` — CLAUDE.md §3 "Request Logging & Tracing" requires an ID minted at the entry point, carried in `context.Context` and present on every log line. The sibling jobs do exactly that (`teams-hr-sync/main.go:97-99` mints via `idgen.GenerateRequestID()` + `natsutil.WithRequestID`; `teams-user-sync/main.go:76`), and `portal-service/handler.go:314` forwards `natsutil.RequestIDHeader` on outbound Resty calls. Here every `slog.*Context(ctx, …)` carries nothing, and restyutil's own `request_id` field (`pkg/restyutil/restyutil.go:97`) is always empty — a `teams room verification mismatch` line cannot be joined to the inspector's log for the same call.
- [low] Error message wrapped with the callee's own text — `teams-room-verify/runner.go:67` repeats `store_mongo.go:38` verbatim, yielding `run: list chats needing verify: list chats needing verify: <mongo err>`. CLAUDE.md wants the wrap to describe what *this* function was doing.
- [low] Bare `return err` twice — `teams-room-verify/main.go:61` and `:65` return `validateConfig` / `parseSiteURLs` errors unwrapped. The underlying messages are self-describing, so impact is cosmetic, but it is the literal "never return bare `err`" rule.
- [low] The HTTP path is hardcoded independently at both ends — `teams-room-verify/client.go:13` and `teams-room-inspector/routes.go:9` — while the rest of the contract (request/response types and `TeamsRoomVerifyMaxChatIDs`) is shared in `pkg/model/teams.go:104-146`. A path drift makes every batch 404, which `verifyBatch` records as a failed batch and leaves chats flagged forever — the exact "reads like a healthy run" failure mode the shared cap constant was introduced to prevent.
- [low] Level inversion + double log on every tolerated failure — `pkg/restyutil/restyutil.go:80` logs the transport failure at ERROR, then `teams-room-verify/runner.go:111` logs the same event at WARN. The job deliberately treats an inspector outage as benign (chats re-verify next run), but it still emits an ERROR line that will trip level-based alerting.
- [low] `//nolint:gocritic // rangeValCopy` with a weak rationale — `teams-room-verify/runner.go:240-245` — `gocritic`'s performance tag is genuinely enabled (`.golangci.yml:16-19`) and `model.TeamsChat` is a fat struct copied twice per chat here; the justification ("index-range would be less idiomatic") is contradicted by the same file using `for i := range b.chats` at `runner.go:106` and `:135`.
- [nitpick] `parseSiteURLs` validates only non-emptiness — `teams-room-verify/config.go:43-47` — no `url.Parse`/scheme check, so a missing `http://` or a trailing slash (yielding `//internal/...`) is only discovered as per-batch runtime failures rather than at startup, unlike the numeric knobs which fail fast.
- [nitpick] Redundant loop-variable parameter — `teams-room-verify/runner.go:81` — `go func(b batch)` predates Go 1.22 per-iteration loop vars; the repo is on Go 1.25.

### Recommendations

- [medium] Break the batch loop on cancellation — `runner.go:78-86` — add `if ctx.Err() != nil { break }` (or a `select` on `ctx.Done()` before acquiring the semaphore) and log once that the pass was cut short. Makes the behaviour match main.go's comment and keeps shutdown logs and stats honest.
- [medium] Mint a run ID and propagate it — `main.go:55-71`, `client.go:22-30` — `requestID := idgen.GenerateRequestID()`, `ctx = natsutil.WithRequestID(ctx, requestID)`, and `SetHeader(natsutil.RequestIDHeader, …)` on the Resty request, mirroring `teams-hr-sync/main.go:97-99` and `portal-service/handler.go:314`. Gives one joinable id across the job's logs and the inspector's.
- [low] Move `verifyPath` to `pkg/model/teams.go` beside `TeamsRoomVerifyMaxChatIDs` and have `teams-room-inspector/routes.go:9` register from it — removes a silent-drift failure mode between the two services.
- [low] Re-wrap `runner.go:67` as e.g. `fmt.Errorf("verification pass: %w", err)` and wrap `main.go:61`/`:65` with `fmt.Errorf("validate config: %w", err)` / `fmt.Errorf("site registry: %w", err)`.
- [low] Downgrade or silence the duplicate transport log — either drop the warn at `runner.go:111` or add a restyutil option to suppress `logError` for callers that handle the error themselves — so an inspector outage does not raise ERROR for a condition the job treats as retryable.
- [nitpick] Validate each registry entry with `url.Parse` + scheme check in `config.go:43-47`, so a malformed inspector URL fails at startup like the numeric knobs do.
