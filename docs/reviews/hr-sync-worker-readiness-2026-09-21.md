# hr-sync-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `60ba808` (base `main`)  
**Overall score:** 2.5 / 5 (baseline 2026-09-01: 2.8, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`hr-sync-worker` is a small, tidy daily consumer — four files, clean WHY-comments, correct `jsretry`/`jobguard` settle path, opt-in stream bootstrap, no hardcoded `cc.BackOff`, shutdown in the documented order. Every reviewer said the Go craftsmanship is fine. It scores 2.5 because the contract around it is wrong in ways the code cannot detect, and because almost none of the risky code is tested.

The critical finding is a head-of-line stall that one ordinary HR event can trigger. The upsert keys `hr_employee` by `_id = employeeId` (`store.go:58`), but the only index on that collection is a **unique** index on `account`, created by the read-only `portal-service` (`portal-service/store_mongo.go:30`). The producer, `teams-hr-sync`, diffs by account, so a person whose Graph id changes is emitted as an upsert, never a quit — the insert collides on `account` and returns a `BulkWriteException`. Nothing in the service calls `mongo.IsDuplicateKeyError`, so `handler.go:32` wraps it raw, `jsretry.Settle` classifies it transient and NAKs, and with `MaxDeliver=-1` plus `MaxAckPending=1` that one batch redelivers forever at the 10-minute backoff tail while every later HR message for the site queues behind it. There is no DLQ. Index ownership is inverted the same way: the writer declares no `EnsureIndexes` at all, so the constraint it must satisfy is owned — and, via `EnsureIndexWithRepair`'s drop path, can be silently rewritten — by a service that never writes the collection.

Second, this service and `teams-hr-sync` are two implementations of one documented `Store` contract, copy-pasted and already diverged. `UpsertUserIdentities` here filters on `account` and mints `idgen.GenerateUUIDv7()`; the twin filters on `employeeId` and sets `_id = employeeId` explicitly, with a comment explaining that search-sync depends on the id being derivable from the Teams id. Both write the same `users` collection, whose `account` is unique — so which `_id` a user ends up with depends on which path inserted first, and the derivability the twin documents is lost for every user arriving over the feed. `teams-hr-sync/README.md:48` points at a shared `pkg/hrstore` that does not exist; the de-duplication was written up and never done, and the doc now conceals the duplication.

Third, the README is not a description of the code. It claims employees are replaced by `{account, source}` and that quits are scoped `source: "teams"` so "legacy-source rows survive" — but `model.IEmployee` has no `source` field, the upsert keys on `employeeId`, and `DeleteMany` filters on `account` alone (and discards the batch's decoded `SiteID`). This file calls itself the reference contract for anyone replacing the persister. Subject routing is the same class of hazard: `strings.HasSuffix` against literals that duplicate `pkg/subject`, with a `default` branch that Ack-drops permanently — so a rename in the builders still compiles, and the entire HR feed is discarded with a WARN.

Coverage is 21.1%, the lowest audited this cycle. `store.go` — the dup-key, `omitempty`-merge and blank-`_id` code above — is at 0% in the unit profile, `bootstrap.go` is 0% despite a `streamManager` seam whose comment says it exists for tests, and the integration test re-implements the consume path by hand instead of calling `startSiteConsumer`, so `jobguard`, `logctx` and the decode-poison branch are executed by no test at all. `main()` is 38% of the package's statements, so the 80% floor is arithmetically unreachable from `make test` as the package is shaped today.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 2 |
| Performance | 3 |

**Findings by severity:** 2 critical, 14 high, 21 medium, 9 low, 4 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] Raw NATS subject interpolated into a log-facing error message — `hr-sync-worker/handler.go:58` — `errcode.BadRequest("unhandled hr subject " + subj)` violates CLAUDE.md §3 "Never put a raw token/body/subject in a cause or message — it reaches the server log". The subject tail is publisher-controlled (`chat.hr.{siteID}.>`), and the service's own test `handler_test.go:87-112` pins the other three messages as *constant* precisely to prevent this leak class; line 58 is the one branch that escaped it.
- [high] Duplicate-key on `hr_employee.account` is unhandled and wedges the site lane permanently — `hr-sync-worker/store.go:58-62`, `hr-sync-worker/main.go:141-143`, `portal-service/store_mongo.go:30-34` — portal-service enforces a unique index on `hr_employee(account)`; this writer upserts keyed `_id = employeeId`, so an account re-homed to a new employee id inserts a second row on the same account → E11000. The error has no `mongo.IsDuplicateKeyError`/`errors.As` handling anywhere in the service, so it returns as *transient*; with `MaxDeliver = -1` it retries forever, and because `MaxAckPending = 1` the parked batch blocks every later HR message for that site indefinitely.
- [medium] README and method name promise a `source`-scoped delete the code does not perform — `hr-sync-worker/README.md:11` vs `hr-sync-worker/store.go:111-116` — README says quit deletes `{account ∈ batch, source: "teams"}` and that "legacy-source rows survive"; the actual `DeleteMany` filters on `account` alone (no `source`, no `siteId`). `model.IEmployee` (`pkg/model/teams_employee.go:29-41`) has no `source` field at all. `QuitTeamsEmployees` likewise implies a Teams scope that isn't there.
- [medium] `$set` merge + `omitempty` means an upstream-cleared field is never unset — `hr-sync-worker/store.go:58` → `pkg/mongoutil/bulk.go:49-60`, `pkg/model/teams_employee.go:7-15` — `BulkUpsert` is documented "MERGE not REPLACE" and builds `$set` from `bson.Marshal`, which honours `omitempty`; all nine `IOrg` fields are `omitempty`. An employee moving to a group with no description keeps the stale `sectDescription`/`deptName` in `hr_employee` forever. The interface comment `store.go:26` says "replaces", which is not what happens.
- [medium] Subject routing hardcodes suffix literals instead of the existing `pkg/subject` builders — `hr-sync-worker/handler.go:23,34,45` vs `pkg/subject/subject.go:1806,1813,1821` (`OrgSyncEmployeesUpsert`/`OrgSyncUsersUpsert`/`EmployeesQuit`). If a builder is edited, this worker silently falls to the `default` branch and Ack-poisons *every* HR message; `handler_test.go:16-19` duplicates the same literals, so no test catches the drift.
- [medium] Asymmetric input guarding: empty key rejected for users, accepted for employees — `hr-sync-worker/store.go:75-78` guards `u.Account == ""` (with a documented clobber rationale) but `store.go:58-60` feeds `e.EmployeeID` straight in as `_id`; every row with a blank employeeId collapses onto a single `_id: ""` doc. Currently masked by the sole producer's filter (`teams-hr-sync/collect.go:40`), but `README.md:14-15` explicitly advertises the feed as an open contract any publisher/persister may implement.
- [low] `streamManager` is documented as test-injected but has no test — `hr-sync-worker/bootstrap.go:19-23`; `bootstrapStreams` is 0.0% in the coverage profile, while ten peer services ship a `bootstrap_test.go` with the same abstraction.
- [low] Bare `err` returned without context — `hr-sync-worker/main.go:119` — `return nil, err` from `CreateOrUpdateConsumer`; CLAUDE.md §3 requires `fmt.Errorf("create hr consumer for %s: %w", ...)`.

### Recommendations

- [high] Replace the interpolated subject with a constant message and move the subject to a structured field on the settle path — `handler.go:58` — keeps the poison-drop diagnosable without an unbounded publisher-controlled string in the log/error.
- [high] Detect `mongo.IsDuplicateKeyError` in `UpsertEmployees` and return `errcode.Permanent(...)` (with the offending account count, not the row) — `store.go:58-62` — converts an infinite, lane-blocking retry into a single Ack-drop plus an alertable log; otherwise one re-homed employee id silently stops HR sync for the whole site.
- [medium] Reconcile README, interface comments and code for both writes — `README.md:9-11`, `store.go:26,33-35` — either implement the documented `{account, source}` scoping or delete the claim; a `DeleteMany` must never be documented as narrower than it is.
- [medium] Route on `pkg/subject` builders (compare against `subject.OrgSyncEmployeesUpsert(siteID)` etc., or add matching `Is…` helpers there) — `handler.go:21-59` — removes the silent-total-poisoning failure mode from a builder edit.
- [medium] Guard `e.EmployeeID == ""` in `UpsertEmployees` with the same skip-and-warn shape already used for `Account` — `store.go:58` — prevents an `_id: ""` merge bucket from any publisher that isn't today's.
- [low] Add `bootstrap_test.go` using the existing `streamManager` seam (enabled → CreateOrUpdate per site; disabled → verify-only; error propagation) — `bootstrap.go:27` — cheap, matches ten peers, and lifts a 0% path.
- [low] Wrap the consumer-creation error — `main.go:119`.
