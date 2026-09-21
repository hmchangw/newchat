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
