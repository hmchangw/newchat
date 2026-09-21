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


## 2. Code quality — score 3

### Evidence

- [high] TLS verification is disabled by default, and the same transport carries the OAuth client-secret POST — `teams-chat-member-sync/main.go:44` — `GRAPH_TLS_INSECURE_SKIP_VERIFY` is `envDefault:"true"`, so a deployment that simply omits the var fails *open*. `pkg/msgraph/msgraph.go:272-278` installs `InsecureSkipVerify: true` on the single `http.Client` that `accessToken` (`pkg/msgraph/msgraph.go:492-513`) also uses to POST `client_secret` (`:503`) to the **public** `login.microsoftonline.com` token URL (`:285-288`). The service's own compose ships `false` (`deploy/docker-compose.yml:15`), so the insecure value only ever reaches production.
- [high] A knob shared by five services is re-declared per service with divergent defaults — `teams-chat-member-sync/main.go:44` (`true`) vs `teams-hr-sync/config.go:28` (`false`), `user-presence-service/sync/main.go:44` (`false`), `teams-user-sync/config.go:20` (`true`), `teams-chat-sync/main.go:51` (`true`). CLAUDE.md §Configuration requires such a knob be declared once in the owning package (`pkg/msgraph`) and mounted as a named field — "never re-declare the env tag and `envDefault` in a service". `GraphProxyURL/Username/Password` (`main.go:49,57,58`) have the same problem; `Pool mongoutil.PoolConfig` (`main.go:33`) shows the correct shape right above them.
- [medium] A persistently lagging secondary makes the job report success while syncing nothing — `syncer.go:144-147` + `store_mongo.go:35-37,52-60` — `ListChatsToSync` reads `updatedAt` through a `SecondaryPreferred` client (`pkg/mongoutil/mongo.go:282-283`), then the write is conditional on that value still matching on the primary. Under sustained replication lag every chat returns `errSuperseded`, which is WARN-only and deliberately excluded from `Failed`, so `run` exits 0 with `chatsSucceeded=0` forever. The only signal is a log field; the service wires no metrics (see below), so nothing alerts.
- [medium] A graceful SIGTERM is logged as a mass failure — `syncer.go:154-157` — the dispatch loop sends on `jobs` with no `select` on `ctx.Done()`, so after `signal.NotifyContext` (`main.go:97`) fires, every remaining chat still drains through `syncChat`, fails with `context.Canceled`, and emits one `slog.Error("teams chat member sync: chat failed")` each (`syncer.go:149`) plus `"%d of %d chats failed"` and exit 1. A routine pod eviction is indistinguishable from a Graph/Mongo outage.
- [medium] A Graph member with no `userId` is written through unfiltered — `syncer.go:89-92,99-107` — guest/anonymous `aadUserConversationMember` entries unmarshal to `ChatMemberDetail{UserID: ""}` (`pkg/msgraph/members.go:14-17`); `buildMembers` pushes `""` into the `UsersByIDs` `$in` and then stores `TeamsChatMember{ID:"", Account:"", DisplayName:""}`, which `teams-room-creation/runner.go:129-133` copies verbatim into the room-create event. No test covers an empty id (`syncer_test.go:132` covers only an *unknown* one).
- [low] No `pkg/obs.Init`, no log level control — `main.go:62` — the job installs a bare `slog.NewJSONHandler(os.Stdout, nil)`, so there are no traces or metrics and no `LOG_LEVEL`, against CLAUDE.md §1 ("each service wires it once via `pkg/obs.Init`"). Sibling `teams-hr-sync/main.go` does wire it; the rest of the teams-* CronJob family does not.
- [low] `syncer.run` deadlocks instead of failing fast on a non-positive worker count — `syncer.go:135` + `:154-156` — with `MaxWorkers <= 0` no goroutine drains the unbuffered `jobs` channel and the send blocks forever. Only `validateConfig` (`main.go:74`) prevents it; `newSyncer` (`syncer.go:31`) accepts the value unchecked.
- [nitpick] Bare `return err` and `fmt.Errorf` with no format verbs — `main.go:91` and `main.go:75` — CLAUDE.md §Error Handling forbids the bare return (the callee's "invalid config: " prefix is the only reason it reads acceptably); `:75` should be `errors.New`. Neither `go vet` nor the repo golangci config catches either.

### Recommendations

- [high] Flip `GRAPH_TLS_INSECURE_SKIP_VERIFY` to `envDefault:"false"` — `main.go:44` — restores fail-closed behaviour and stops `GRAPH_CLIENT_SECRET` (and `GRAPH_PROXY_PASSWORD` on a Basic-auth proxy) riding an unverified connection to Azure AD. Update `main_test.go:29` with it.
- [high] Move the Graph TLS and proxy knobs into a `pkg/msgraph` config struct mounted as a named field, deleting the per-service `env`/`envDefault` tags in all five services — `main.go:44,49,57,58` — resolves the CLAUDE.md §Configuration violation and makes the default unfalsifiable.
- [medium] If an on-prem TLS-intercepting proxy is genuinely required, ship its CA via a `GRAPH_CA_BUNDLE` root pool instead of `InsecureSkipVerify` — keeps verification on for the token endpoint, which is never on-prem.
- [medium] Fail (or at least surface) a run where `Superseded > 0 && Succeeded == 0` — `syncer.go:160-168` — turns silent no-progress under replica lag into a CronJob failure; alternatively re-read the chat's `updatedAt` from the write client before the conditional update.
- [medium] `select { case jobs <- chat: case <-ctx.Done(): }` in the dispatch loop, and branch on `errors.Is(err, context.Canceled)` in the worker to log at Info and skip the `Failed` counter — `syncer.go:141-157` — so a graceful stop reads as a graceful stop.
- [medium] Skip (and count) members with an empty `UserID` in `buildMembers` — `syncer.go:89-92` — with a table case in `syncer_test.go`; stops empty-identity members reaching `teams_chat` and the room-create event.
- [low] Validate `MaxWorkers > 0` in `newSyncer` — `syncer.go:31` — turns a silent hang into an immediate error independent of `validateConfig`.

## 3. Architecture — score 4

### Evidence

- [high] The optimistic-concurrency token is a whole-document write stamp, and losing the CAS is reported as success — `teams-chat-member-sync/store_mongo.go:53` — the write is guarded by `{_id, updatedAt: seenUpdatedAt}`, but `teams-chat-sync` stamps `updatedAt: now` into an *unconditional* `$set` on every upsert of a non-oneOnOne chat (`teams-chat-sync/store_mongo.go:151`, `teams-chat-sync/syncer.go:123`), so any overlapping run that re-touches an active chat invalidates the token even when membership did not change. The write is then skipped (`store_mongo.go:59`), counted as `Superseded`, logged at Warn, and the run still exits 0 (`syncer.go:144-147`, `syncer.go:165`). A busy large group chat can therefore loop forever without ever reaching `needCreateRoom=true` — no room is created and no CronJob ever goes red.
- [medium] No `pkg/obs.Init`; the job is unobservable beyond stdout — `teams-chat-member-sync/main.go:62` — it installs a bare `slog.NewJSONHandler`. 34 services in the tree wire `pkg/obs.Init`; none of the teams-* CronJobs do. `obs.Init` returns a deferrable shutdown func (`pkg/obs/obs.go:178`), so a run-to-completion job can use it. Today there are no traces on the Graph calls and no metrics behind the run counters at `syncer.go:160-163` — which is exactly what would surface the stall above.
- [medium] Dispatch loop and workers never observe context cancellation — `teams-chat-member-sync/syncer.go:154-157` — `signal.NotifyContext` cancels on SIGTERM (`main.go:97`), but the loop keeps feeding every remaining chat into `jobs`. Each `syncChat` then fails on the cancelled ctx at `graph.ListChatMembers` (`syncer.go:176`), increments `Failed` and emits an Error log per chat (`syncer.go:148-150`), so `run` returns "N of M chats failed" and the pod's graceful eviction is recorded as a Job failure.
- [medium] `ListChatsToSync` loads the whole pending set unbounded — `teams-chat-member-sync/store_mongo.go:36` — no limit, sort or paging (the sibling `teams-room-creation/store_mongo.go:34` at least sorts `_id` for deterministic batches). Memory is O(pending chats), and a large backlog directly widens the read→write window that finding 1 depends on.
- [low] `syncer.users` is a dead dependency — `teams-chat-member-sync/syncer.go:26`, assigned at `syncer.go:32` — the only read of a `TeamsUserStore` is `c.users.UsersByIDs` inside the cache (`syncer.go:70`). The field is never referenced.
- [low] The CAS token is read through the secondary-preferred client — `teams-chat-member-sync/main.go:100` + `store_mongo.go:26` — `ListChatsToSync` is served by `readChats`, so replication lag yields a stale `updatedAt` and extra supersessions. Correctness holds (the compare executes on the primary), but every spurious loss costs a full Graph round trip against the tenant throttle budget, and finding 1 makes the loss invisible.
- [nitpick] No `pkg/shutdown.Wait` — `teams-chat-member-sync/main.go:97` — CLAUDE.md asks for it in every `main.go`, but `signal.NotifyContext` is the correct primitive for a run-to-completion job and matches every teams-* CronJob. The rulebook simply lacks the carve-out.

### Recommendations

- [high] Replace the CAS guard with one that only fails on a real membership change — `store_mongo.go:53` — filter on `{_id, needMemberSync: true, lastUpdatedDateTime: seen}` (the Graph-side change token) instead of the write stamp, and/or track a per-chat supersession count so a chat that loses the CAS repeatedly fails the run. Removes the silent-stall path.
- [medium] Wire `pkg/obs.Init` in `run()` with its shutdown deferred — `main.go:62` — and export the run counters (`syncer.go:160-163`) so `chatsSuperseded`/`chatsFailed` are alertable instead of log-only.
- [medium] Make the fan-out cancellation-aware — `syncer.go:154-157` — `select` on `ctx.Done()` in the dispatch loop and in the worker receive, and treat `context.Canceled` as a clean early stop (log once, exit 0) rather than per-chat failure.
- [medium] Bound the scan — `store_mongo.go:36` — add a configurable limit plus a stable `_id` sort, matching `teams-room-creation`; a backlog then drains over several runs with a short CAS window each.
- [low] Delete the unused `syncer.users` field — `syncer.go:26,32` — pass `TeamsUserStore` only to `newUserRefCache`.
- [low] Serve `ListChatsToSync` from the primary — `main.go:112` — or document the lag/waste trade-off; keep `UsersByIDs` on the secondary, where staleness is harmless.

## 4. Test coverage — score 2

### Evidence

- [high] coverage below repo minimum 80%, currently 60.3% — `teams-chat-member-sync/main.go:61` — `go test -race -covermode=atomic` reports 60.3% of statements (79/131). Per-file from the provided profile: `syncer.go` 73/74 (98.6%), `main.go` 5/33 (15.2%), `store_mongo.go` 1/24 (4.2%). CLAUDE.md §4 sets 80% as a MUST-NOT-merge floor.
- [medium] the 60.3% number understates real coverage: the whole store layer is exercised only under the `integration` tag, which the profile does not include — `teams-chat-member-sync/integration_test.go:30` — `ListChatsToSync`, `SetMembersSynced` (incl. the `errSuperseded` optimistic-write path), `UsersByIDs` and `newMongoStore` all have real testcontainer tests (`:30`, `:56`, `:86`, `:114`). Discounting `main.go`'s wiring, the tested surface is ~98%. The gate still fails, but the remedy is a tagged/merged profile plus a few unit gaps, not a rewrite.
- [medium] no test covers SIGTERM/context cancellation, and the dispatch loop has no `ctx.Done()` arm — `teams-chat-member-sync/syncer.go:154` — `for _, chat := range chats { jobs <- chat }` dispatches every remaining chat after `ctx` is cancelled; each then fails in `graph.ListChatMembers`, increments `sum.Failed`, and `run` returns `"%d of %d chats failed"` (`:165`), so a routine pod eviction is recorded as a CronJob failure. `grep -n "ctx.Done\|WithCancel"` over the package returns nothing — the behaviour is neither implemented nor asserted.
- [medium] empty member list from Graph is an untested boundary — `teams-chat-member-sync/syncer.go:175` — no test returns `[]msgraph.ChatMemberDetail{}` from `ListChatMembers`. `syncChat` would write `members: []` and flip `needCreateRoom=true` (`store_mongo.go:65`), handing the room-creation stage a 0-member chat. CLAUDE.md §4 requires empty-collection edge cases explicitly.
- [medium] `main.go`'s `run` is 0% and untestable as written — `teams-chat-member-sync/main.go:85` — config parse, two `mongoutil.Connect*` calls, `msgraph.NewChatMembersClient` and `newSyncer` are one 53-line function, so 28 of its statements can never be unit-covered. `validateConfig` (`:73`) was correctly split out and is at 100%; the client/store construction was not.
- [low] the only uncovered statement in `syncer.go` is `syncChat`'s `buildMembers` error branch — `teams-chat-member-sync/syncer.go:181` — the wrap is tested directly (`syncer_test.go:121`) but never through `run`, so nothing asserts that a `teams_user` lookup failure marks the chat failed rather than superseded.
- [low] the run-summary counters are never asserted — `teams-chat-member-sync/syncer.go:160` — `chatsSucceeded`/`chatsFailed`/`chatsSuperseded`/`membersWritten` are the job's only observability, and `log_test.go:72` asserts only the per-chat `"members set"` record. A miscounted `MembersWritten.Add` (`:187`) would pass the suite.
- [nitpick] three variations of one function are three separate tests instead of a table — `teams-chat-member-sync/syncer_test.go:98,121,132` — `TestBuildMembers_*` share one shape; CLAUDE.md §4 prefers table-driven with `t.Run`. Only `TestValidateConfig` (`main_test.go:59`) is table-driven.

### Recommendations

- [high] Raise measured coverage over 80% — merge the integration profile into the gate (`go test -tags=integration -coverprofile`) or exclude `main.go`'s wiring, then close the unit gaps below. — Without this the service is un-mergeable under CLAUDE.md §4 despite genuinely good tests.
- [medium] Add a cancellation test: cancel `ctx` mid-run and assert dispatch stops — `syncer.go:154` — pair it with a `select { case jobs <- chat: case <-ctx.Done(): }` arm so graceful shutdown no longer reports a CronJob failure.
- [medium] Add `TestSyncChat_EmptyMemberList` asserting the intended behaviour for a 0-member Graph response — `syncer.go:175` — decide and pin down whether the chat advances to room creation or stays flagged; today it silently advances.
- [medium] Extract the dependency wiring out of `run` into a `newApp(cfg)`-style constructor — `main.go:85` — makes ~28 statements reachable and removes the largest single block of dead coverage.
- [low] Extend `TestRun_GraphFailureKeepsFlagAndFailsRun` (`worker_test.go:53`) to also assert the summary counters and add a `buildMembers`-fails-inside-`run` case — covers `syncer.go:181` and the counters at `syncer.go:160` in one pass.
- [nitpick] Fold `TestBuildMembers_*` into one table with `t.Run` subtests — `syncer_test.go:98` — same coverage, one place to add the next case.
