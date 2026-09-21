# teams-hr-sync — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `f6baad6` (base `main`)  
**Overall score:** 2.5 / 5 (baseline 2026-09-01: 2.8, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-hr-sync` walks Microsoft Graph groups, diffs the result against `hr_employee`, and emits the delta — either as NATS events for `hr-sync-worker` (stream mode) or by writing Mongo directly (direct mode, the migration path). Its structure is genuinely good: consumer-defined interfaces, constructor DI, an injected `publishFunc`, subjects built exclusively from `pkg/subject` with no raw `fmt.Sprintf` anywhere, and a clean `emitter` seam. It scores 2.5 because the two write paths disagree with each other, the destructive path has no guard rail, and the riskiest code has no tests at all.

The most consequential finding is that **nothing bounds the quit batch**. Every stored row absent from the Graph walk becomes a quit, and the consumer runs it as an unfiltered `DeleteMany` by account. A group that returns HTTP 200 with an empty `value` — a revoked app permission, an accidentally emptied group, a typo in `SYNC_GROUPS` — is not an error in `collect.go:36`, so one successful-but-wrong Graph response wipes a whole site's HR directory, which `portal-service` then left-joins against. There is no max-quit ratio and no min-member floor anywhere in the pipeline. Compounding it, `ListTeamsEmployees` reads the entire collection with `bson.M{}` although its name, the README and an integration test all claim it is scoped to `source:"teams"` rows — `model.IEmployee` has no `source` field at all, so the "legacy rows survive" assertion is vacuous and any future non-Teams HR feed into the same collection gets diffed and deleted.

Second, the publish order breaks the service's own stated self-healing contract. `employees.upsert` is awaited and acked before `users.upsert` is attempted (`publisher.go:38-53`), but `hr_employee` — updated by the first message — *is* the diff baseline. If the second publish fails, the next run sees no delta and the user identity rows are never written. That is permanent, silent loss, and both `README.md:10` and `main.go:37` claim the opposite. Reversing the two lines fixes it: the baseline-advancing publish must go last. Related, neither upsert event carries a `Timestamp`, against CLAUDE.md's rule for every `pkg/model` event struct — `search-sync-worker` already pays for that absence with a stream-sequence last-write-wins workaround.

Third, direct mode and `hr-sync-worker` are two hand-written implementations of one documented contract and have already diverged on every detail: match key (`employeeId` vs `account`), `_id` rule (`= employeeId` vs a minted UUIDv7), and which fields are skipped on empty. `users.account` carries a unique index, so a direct-mode backfill against a database holding a native user without an `employeeId` matches nothing, inserts a duplicate account, and fails mid-run — after the employees were already written. That filter is also unindexed: no service creates `{employeeId: 1}` on `users` and this one defines no `EnsureIndexes`, so a 20k-employee migration is O(N·M) collection scans. The README points at a `pkg/hrstore` that would have prevented all of this; it does not exist, and the README now conceals the duplication rather than documenting it.

Coverage is 57.1%, and the gap sits exactly on the dangerous code: all three `WriteStore` methods are at 0% with no test of any kind, unit or integration — including the hand-built `$set` whose entire reason for existing is the comment "must never touch roles/services/password on the live auth store". A future edit to a full-document replace would pass every test in the repo. Both data-corruption guards in `UpsertUserIdentities` are likewise unreached.

On throughput, the whole upsert set ships as one JetStream message with no size check or chunking, so a directory in the low tens of thousands crosses `max_payload` — and it bites on the very first run, when the diff baseline is empty and every employee is an upsert. Nothing self-heals, because nothing was published; the next run rebuilds the identical oversized batch. Every other batching publisher in the repo caps itself.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 2 |
| Performance | 3 |

**Findings by severity:** 1 critical, 15 high, 17 medium, 13 low, 8 nitpick (54 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

