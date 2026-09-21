# teams-user-sync — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `a857cdc` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-user-sync` walks the Microsoft Graph directory and materialises `teams_user`, the collection every other Teams job joins against. It is the best-built of the seven Teams CronJobs on craftsmanship — 4s in code quality, architecture, maintainability and performance — with a consumer-owned three-method store interface, separated read/write Mongo lanes, explicit projections everywhere, `Pool mongoutil.PoolConfig` correctly mounted rather than re-declared, and `handler.go` at 58/58 statements with genuine table-driven tests for every error path. It scores 3.3 because of one behavioural gap and the fleet-wide Graph-config problem.

The behavioural gap is that **reconciliation is insert-only**. `syncPage` skips every id already present in `teams_user` *before* the HR join runs (`handler.go:71`), so `siteId`, `engName`, `mail` and `displayName` are written exactly once and never refreshed. Nothing else in the repo writes those fields. A user synced before their `hr_employee` row exists keeps `siteId: ""` permanently — and `teams-chat-sync` drops empty-`siteId` members from its per-chat site vote, so those chats silently fall back to `DefaultSiteID` and get materialised at the wrong site. Site transfers never propagate either, and there is no backfill path anywhere in the repo. Meanwhile `teams-hr-sync` keeps mutating `hr_employee` and `hr-sync-worker` deletes rows from it, so the two collections diverge monotonically.

Two related integration hazards sit on the same join. `splitUPN` — the function that derives the `account` key tying `teams_user`, `hr_employee` and `users` together — is duplicated byte-for-byte in this service and `teams-hr-sync/transform`, so a one-sided edit (handling `#EXT#` guest UPNs, say) breaks the join with no compile-time or test signal. And `teams_user.account` can collide where every peer collection treats `account` as globally unique: the account is the UPN local part for every tenant user across all domains, but the upsert keys only on `_id`, so two AAD objects sharing a local part across domains produce two rows with one account — and `search-sync-worker` builds an `account → teamsUserID` map that then silently drops one of them. The service also diffs its own write target through a `SecondaryPreferred` client, a read-your-own-writes against a lagging replica; `teams-chat-sync` pins the same collection to the primary for exactly this reason.

The Graph config problem is shared with four siblings and is security-relevant: `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults to `true` here (and in `teams-chat-sync` and `teams-chat-member-sync`) but `false` in `teams-hr-sync` and `user-presence-service/sync`, because `pkg/msgraph.Config` carries no env tags and six services each re-declare the same operator-facing names. CLAUDE.md §Configuration forbids exactly this. `config_test.go:30` even locks the insecure default in as intended behaviour.

On throughput one finding stands out, and it lives in `pkg/msgraph` rather than here: `fetchUsersPage` calls `httpClient.Do` raw with no 429/503 handling, while every other paged walk in the same package routes through `getThrottled` with `Retry-After` backoff and a tenant-wide gate. A full-directory walk is Graph's most throttle-prone call — 400+ pages for a large tenant — and a single 429 mid-walk aborts the entire run, which under `concurrencyPolicy: Forbid` means new users simply never land. It is close to a one-line fix.

Coverage is 53.4%, floored at 1 by the dimension rule, but the shape is favourable: the business logic is at 100% and the deficit is `main.go` wiring plus a store layer covered only by integration tests that the pipeline never runs — no service pipeline in this repo passes `-tags integration`. One consequence worth fixing regardless: both integration tests construct `newMongoStore(db, db)`, so transposing the read and write lanes would ship with a green suite.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 7 high, 12 medium, 16 low, 9 nitpick (45 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

