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

