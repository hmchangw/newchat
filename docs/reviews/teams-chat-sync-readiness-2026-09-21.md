# teams-chat-sync — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `c4b461d` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.5, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-chat-sync` is one of the better-built services in the fleet on pure craftsmanship — `syncer.go` is at 100% statement coverage with genuine error-path tests (a Graph failure holds the watermark, an upsert failure holds it, a `SetFrom` failure fails only that user), files are small, wraps are honest, seams are injectable, and there are zero TODOs. It lands at 3.2 because two decisions outside that core are load-bearing and wrong.

The first is a silent data-loss path that two reviewers found independently from opposite directions. `buildChat` decides a roster is complete when a non-1:1 chat comes back from Graph with fewer than 25 inline members (`syncer.go:315`), and then `$set`s `members` plus `needCreateRoom: true` as authoritative. But `msgraph.Chat` decodes only `members` — never `members@odata.nextLink` — so a *truncated* `$expand=members` response is indistinguishable from a complete one, and a group chat that comes back with **zero** members takes the same "complete" branch. `teams-room-creation` republishes that roster and `room-worker/teamsroomcreate.go:153-177` reconciles subscriptions to it, calling `DeleteSubscriptionsByAccounts` for everyone absent — so one degraded Graph response mass-unsubscribes a room. The threshold's own comment says it MUST stay at or below Graph's inline cap, but nothing derives, asserts or cross-checks that number, and `syncer_test.go:129` actually pins "empty group finalized inline" as the intended behaviour. The fix is small: decode the `nextLink`, and defer to `teams-chat-member-sync` whenever the roster is empty or possibly truncated.

The second is the TLS default. `GRAPH_TLS_INSECURE_SKIP_VERIFY` is `envDefault:"true"` (`main.go:51`), so a Kubernetes CronJob that simply omits the variable runs with verification off — and the same `http.Client` carries the OAuth token request to the public `login.microsoftonline.com`, so `GRAPH_CLIENT_SECRET` traverses an unverified connection. The service's own compose file ships `false`, the accepted design plan specified `false`, and `teams-hr-sync` and `user-presence-service/sync` default `false` while this service, `teams-user-sync` and `teams-chat-member-sync` default `true`. That divergence is the direct consequence of a CLAUDE.md §Configuration violation: the whole Graph config block — tags, defaults and comments — is copy-pasted into six services instead of being declared once in `pkg/msgraph` and mounted as a named field, the way `Pool mongoutil.PoolConfig` already is in the same struct. gosec sees none of it, because the `#nosec G402` lives in `pkg/msgraph`.

Beyond those: there is no run deadline anywhere, explicitly delegated to a Kubernetes CronJob manifest that does not exist in `deploy/`, and no `context.WithTimeout` on any Graph or Mongo call — so a hung primary parks all workers indefinitely. The dispatcher never selects on `ctx.Done()`, so a routine SIGTERM eviction drains every remaining user through a cancelled context and reports itself as a mass outage. And the per-user Graph window is unbounded with an all-or-nothing watermark, so a backfill that can't complete in four throttled attempts refetches the identical six-month window on every subsequent run, forever.

Coverage is 67.6%, but the more useful finding is that the service's pipeline runs `go test` with no `-tags=integration` — so `EnsureIndexes`, the whole Mongo layer, and the named "re-sync must not clobber member-sync's `needCreateRoom`" regression guard are verified by tests CI never executes. Counting them would put the service at 80.1%, just over the floor.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 11 high, 15 medium, 15 low, 7 nitpick (48 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] TLS verification is disabled by default, and the same transport carries the OAuth client-secret POST — `teams-chat-sync/main.go:51` — `GRAPH_TLS_INSECURE_SKIP_VERIFY` is `envDefault:"true"`, so production (which simply omits the var) fails *open*. `pkg/msgraph/msgraph.go:272-278` installs `InsecureSkipVerify: true` on the single `http.Client` that `pkg/msgraph/msgraph.go:506-513` also uses for the token request to the **public** `login.microsoftonline.com` endpoint (`msgraph.go:285-288`), so `GRAPH_CLIENT_SECRET` (and `GRAPH_PROXY_PASSWORD` on a Basic-auth proxy) traverse an unverified connection.
- [high] That insecure default contradicts both the service's design plan and its own compose file — `teams-chat-sync/deploy/docker-compose.yml:18` — compose ships `false`, and the plan specified `envDefault:"false"`, "opt-in" (`docs/superpowers/plans/2026-07-14-teams-chat-sync.md:1801`). It reads as unreviewed drift, not a decision.
- [high] A knob shared by five services is re-declared per service with divergent defaults — `teams-chat-sync/main.go:51` vs `teams-hr-sync/config.go:28` (`false`), `user-presence-service/sync/main.go:44` (`false`), `teams-user-sync/config.go:20` (`true`), `teams-chat-member-sync/main.go:44` (`true`). CLAUDE.md §Configuration MUSTs that such a knob be declared once in the owning package (`pkg/msgraph`) and mounted as a named field — "never re-declare the env tag and `envDefault` in a service".
- [medium] A graceful SIGTERM is logged as a mass failure — `teams-chat-sync/syncer.go:161-176` — the dispatch loop sends on `jobs` without selecting on `ctx.Done()`, so on cancellation every remaining user drains through `syncUser`, fails with `context.Canceled`, and emits one `slog.Error("teams chat sync: user failed")` each plus an aggregate `"%d of %d users failed"` and exit 1. A routine eviction is indistinguishable from a Graph/Mongo outage.
- [low] A defensive branch that production can never reach carries most of the worker tests — `teams-chat-sync/syncer.go:218-221` — the empty-`SiteID` skip is unreachable because `SYNC_DEFAULT_SITE_ID` is `required,notEmpty` (`main.go:39`), as its own comment concedes; yet `worker_test.go:32` builds every syncer with an empty `DefaultSiteID`, and `worker_test.go:202`/`:218` assert on the skip. The branch silently drops chats.
- [low] `syncer.run` deadlocks instead of failing fast on a non-positive worker count — `teams-chat-sync/syncer.go:156` + `:175` — no goroutine drains the unbuffered `jobs` channel, so the send blocks forever. Only `validateConfig` (`main.go:82`) prevents it; `newSyncer` (`syncer.go:50`) accepts the value unchecked.
- [nitpick] `fmt.Errorf` with no format verbs where `errors.New` belongs — `teams-chat-sync/main.go:83` and `:86`. Neither `go vet` nor the repo's golangci config catches it.
- [nitpick] Five `//nolint:gocritic // hugeParam` suppressions in ~400 lines — `main.go:80`, `store_mongo.go:123`, `syncer.go:103`, `:194`, `:206`. The four non-startup ones sit on the per-user/per-chat path; pointers would remove both copy and suppression.

### Recommendations

- [high] Flip `GRAPH_TLS_INSECURE_SKIP_VERIFY` to `envDefault:"false"` — `main.go:51` — restores fail-closed behaviour, matches the design plan and compose, and stops the client secret riding an unverified connection to Azure AD.
- [high] Move the knob into `pkg/msgraph` as a mounted config struct (alongside `ProxyURL`/`ProxyUsername`/`ProxyPassword`, which have the same problem) and delete the per-service `env`/`envDefault` tags in all five services — resolves the CLAUDE.md §Configuration violation and makes the default unfalsifiable.
- [medium] Split the peer TLS decision from the Graph host: if an on-prem TLS-intercepting proxy is genuinely required, ship its CA via a `GRAPH_CA_BUNDLE` root pool rather than `InsecureSkipVerify` — keeps verification on for the token endpoint, which is never on-prem.
- [medium] `select { case jobs <- u: case <-ctx.Done(): }` in the dispatch loop, and branch on `errors.Is(err, context.Canceled)` in the worker to log at `Info` and skip the `Failed` counter — `syncer.go:161-176` — so a graceful stop reads as a graceful stop.
- [low] Validate `MaxWorkers > 0` in `newSyncer` (or buffer `jobs`) — `syncer.go:50` — turns a silent hang into an immediate error.
- [low] Drop the unreachable empty-`SiteID` skip and give `newTestSyncer` a non-empty `DefaultSiteID` — `syncer.go:218`, `worker_test.go:32` — so the tests pin the production configuration instead of an impossible one.
