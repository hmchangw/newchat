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


## 2. Code quality — score 3

### Evidence

- [high] A partial publish silently and permanently drops user identities — `teams-hr-sync/publisher.go:38-53` — `publishSync` publishes `employees.upsert` then `users.upsert`. The diff baseline is `hr_employee` only (`store.go:17`) and `hr-sync-worker` creates `users` rows *solely* from `users.upsert` (`hr-sync-worker/handler.go:34-43`), so if the first lands and the second fails, the next run sees no delta for those accounts and never re-emits them. The "a lost publish self-heals" claim (`main.go:37-39`) does not hold here.
- [high] Direct mode writes `users` with a different identity key than the stream consumer — `teams-hr-sync/write_store.go:73-85` vs `hr-sync-worker/store.go:84-96` — this service filters on `{"employeeId": …}` and forces `$setOnInsert _id = employeeId`; the worker filters on `{"account": …}` and mints a UUIDv7 `_id`. Two implementations of one documented contract. A `direct` backfill against a DB where an account already exists without an `employeeId` matches nothing and **inserts a second `users` doc for the same account** on the live auth store.
- [medium] `ListTeamsEmployees` is not scoped to Teams-sourced rows — `teams-hr-sync/store_mongo.go:56-58` — the filter is `bson.M{}` (whole collection) and `model.IEmployee` has no `Source` field (`pkg/model/teams_employee.go:29-42`), so the method name and the README's `source:"teams"` claim are both untrue. Any `hr_employee` row from a non-Teams producer is diffed as a departure and published as a quit → `DeleteMany` downstream. The test assertion "the legacy row never does" (`integration_test.go:163`) is vacuous — no legacy row is ever inserted.
- [medium] Mode-conditional required config is only half enforced — `teams-hr-sync/config.go:55,62` — `MONGO_READ_URI` and `NATS_URL` are `required,notEmpty` unconditionally, yet `runDirectMode` (`main.go:169-181`) touches neither. `DIRECT_WRITE_URI` got a hand-rolled conditional check (`main.go:55-59`); the symmetric case did not, so a migration run must invent dummy Mongo/NATS URLs.
- [low] The request ID reaches only two log lines — `teams-hr-sync/main.go:100-125` — `logger := slog.With("requestId", …)` is local to `run`; `ctx` carries the ID (`main.go:99`) but the default JSON handler (`main.go:27`) is not context-aware, so the drain error (`main.go:153`) and `write_store.go:69` emit without it. CLAUDE.md requires it on *all* log lines. The key also disagrees with `pkg/jsretry`'s `request_id`.
- [low] Silent-skip warn carries no identifying field — `teams-hr-sync/write_store.go:69` — `"skip user identity upsert: empty employeeId"` names no account, so an operator cannot tell which rows were dropped from a backfill. `account` is not a secret and belongs here.
- [low] README has drifted from the code — `teams-hr-sync/README.md:15,48-51,53-54,61-64` — it documents a `pkg/hrstore` package that does not exist (`write_store.go:20-21` says so), `model.ChangeTypeNewHire/Update` (actual: `IChangeTypeNewHire`), a `transform.SourceTeams` tag that exists nowhere, and an example typed `*model.Org`/`model.Employee` that will not compile against `transform/transform.go:41`.
- [nitpick] Dead + undocumented config — `deploy/docker-compose.yml:29` sets `ORG_TYPE=group`, read by no Go file; conversely `GRAPH_PROXY_URL/USERNAME/PASSWORD` (`config.go:34-43`) are absent from the README table.
- [nitpick] `stats.Overridden` is counted before the dedup check — `collect.go:45-52` — an account present in two groups increments it twice for one emitted row. Also `fmt.Errorf` with no verbs at `config.go:100` (use `errors.New`), and a stray "ponytail:" token in the doc comment at `transform/transform.go:34`.

### Recommendations

- [high] Make the employee/user pair atomic — `publisher.go:38-53` — either publish `users.upsert` first (a re-published user upsert is idempotent; a re-published employee is what closes the diff), or have `hr-sync-worker` derive identities from the `employees.upsert` batch so one message carries both. Removes a silent, unrecoverable identity gap.
- [high] Reconcile `UpsertUserIdentities` across the two writers — `write_store.go:73-85` — key on `account` and mint the `_id` the same way `hr-sync-worker/store.go:84-96` does (or extract the shared logic), and add a test asserting the two produce identical documents. Prevents duplicate `users` rows on the auth store.
- [medium] Filter the diff baseline to rows this producer owns — `store_mongo.go:57` — add a `source`-style discriminator to `model.IEmployee`, write it on upsert, and filter both the read and `QuitTeamsEmployees`. Then make `integration_test.go:163` actually insert a foreign row so the assertion has teeth.
- [medium] Validate `MONGO_READ_URI`/`NATS_URL` per mode — `main.go:49-59` — require them only when `HRSyncMode == modeStream`, mirroring the existing `DIRECT_WRITE_URI` check, and fix the README's required column.
- [low] Install a context-aware handler at `main.go:27` that lifts `natsutil.RequestIDFromContext` onto every record, then drop the ad-hoc `slog.With` — one correlation ID across the whole run, including `pkg/` logs.
- [low] Rewrite the README against the current code (`pkg/hrstore`, `SourceTeams`, `IChangeType*`, the example's signatures) and delete `ORG_TYPE` from `deploy/docker-compose.yml:29`.
