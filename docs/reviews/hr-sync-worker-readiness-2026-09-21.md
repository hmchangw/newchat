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

## 3. Architecture — score 3

### Evidence

- [high] The writer of `hr_employee` owns no index; the only index on the collection is created by a read-only consumer, and the writer cannot satisfy it — `hr-sync-worker/store.go:44` — `newMongoStore` has no `EnsureIndexes` (CLAUDE.md: indexes are created in the store constructor or a startup `EnsureIndexes`). `portal-service/store_mongo.go:29-36` creates a **unique** index on `hr_employee(account)` via `mongoutil.EnsureIndexWithRepair`, whose repair path drops a conflicting same-keys index (`pkg/mongoutil/indexes.go:157`). The writer keys upserts by `_id = employeeId` (`store.go:58`), so an account whose `employeeId` changes yields a second document with the same `account` and violates a constraint this service never declared.
- [high] A duplicate-key rejection stalls the whole site feed indefinitely — `hr-sync-worker/store.go:58-62`, `main.go:130,141-143`. `BulkUpsertByID` returns the raw Mongo error, `HandleMessage` wraps it with `fmt.Errorf` (`handler.go:32`), so `jsretry.Settle` classifies it transient and Naks. With `MaxDeliver=-1` and `MaxAckPending=1` that batch retries forever and every later HR message on that stream is head-of-line blocked, with no DLQ. The repo convention (`mongo.IsDuplicateKeyError`, `room-service/handler_teams.go:192`, `media-service/store_mongo.go:189`) is not used anywhere here.
- [high] `SITE_IDS` models one HR stream per site, but the feed is a single central stream — `main.go:75-83`, `config.go:11`, `deploy/docker-compose.yml:16-17`. The producer publishes all three subjects under `CENTRAL_SITE_ID` only (`teams-hr-sync/publisher.go:38,50,67`; `pkg/stream/stream.go:109`). Compose defaults `SITE_IDS=${ALL_SITE_IDS}`, so with bootstrap off every non-central entry fails `js.Stream` at `bootstrap.go:39` and the process exits (`main.go:70`); with bootstrap on it creates empty `HR-{site}` streams. There is also no remote-domain knob — `search-sync-worker` needs `HR_JETSTREAM_DOMAIN` to reach this same stream from another site (`search-sync-worker/main.go:64-66,330`) — so a non-central deployment cannot consume the feed at all.
- [medium] Subject routing bypasses `pkg/subject` and fails closed into a silent drop — `handler.go:23,34,45,58`. Routing is `strings.HasSuffix` against literals duplicating the grammar in `pkg/subject/subject.go:1807-1821`. A rename there still compiles; every message then hits the `default` branch, which is `errcode.Permanent` → Ack-drop, discarding the entire HR feed silently instead of failing loudly.
- [medium] README documents a contract the code does not implement — `README.md:9,11` vs `store.go:58,112`. The README claims employees are replaced by `{account, source}` and quits delete `{account ∈ batch, source:"teams"}` so "legacy-source rows survive". `model.IEmployee` has no `source` field (`pkg/model/teams_employee.go:29-42`); the upsert keys on `_id = employeeId` and `DeleteMany` filters on `account` alone. The README is the stated contract for replacing this worker with an external persister.
- [medium] Two implementations of the same documented `Store` contract disagree on the users identity key — `store.go:92-98` filters `{account}`, `teams-hr-sync/write_store.go:72-78` filters `{employeeId}`, with near-identical doc comments. `users.account` is unique (`user-service/mongorepo/users.go:43`), so the two paths cannot both converge on one row.
- [low] No empty-key guard on the employee upsert — `store.go:58-60`. An element with an empty `employeeId` becomes `_id:""`, collapsing all such rows onto one document; the sibling paths do guard (`store.go:75`, `teams-hr-sync/write_store.go:69`). Only the producer's filter (`teams-hr-sync/collect.go:40`) prevents it today.
- [low] The integration test re-implements the consume wiring — `integration_test.go:30-49` rebuilds the consumer config and settle path by hand and omits `jobguard.Run`, so drift in `startSiteConsumer`/`buildConsumerConfig` is invisible to it.
- [nitpick] Service-internal types exported in `package main` — `Store`, `MongoStore`, `EmployeeCollection`, `Handler` (`store.go:19,25,39`, `handler.go:15`).

### Recommendations

- [high] Add `EnsureIndexes` to `newMongoStore` (`store.go:44`) creating the `hr_employee(account)` unique index (plus what `QuitTeamsEmployees`, `store.go:112`, queries on) and drop index creation from the read-only `portal-service` (`portal-service/store_mongo.go:29`) — the writer must own the constraint it has to satisfy, and a reader's repair path can drop it.
- [high] Classify `mongo.IsDuplicateKeyError` as `errcode.Permanent` (or reconcile by re-keying) at `store.go:58`/`handler.go:31` — otherwise `MaxDeliver=-1` + `MaxAckPending=1` turns one bad row into an unbounded silent stall of the site's feed.
- [high] Reconcile the deployment model: either rename the knob to `HR_CENTRAL_SITE_ID` (one consumer) or add a `HR_JETSTREAM_DOMAIN` equivalent, and stop fanning out over `ALL_SITE_IDS` in `deploy/docker-compose.yml:17`.
- [medium] Route on `pkg/subject` builders (the siteID is already in hand at `main.go:77`) instead of `strings.HasSuffix`, and make the `default` branch alert rather than Ack-drop — `handler.go:23-58`.
- [medium] Realign `README.md:9-11` with `store.go` (employeeId-keyed upsert, account-only delete, no `source` scoping), or implement the documented scoping.
- [medium] Align the users identity key across the two writers (`store.go:92` vs `teams-hr-sync/write_store.go:72`); `account` is the unique-indexed field, so the feed path is the correct one.
- [low] Have `integration_test.go:30` call `startSiteConsumer` so the real consumer config, jobguard and settle path are what is covered.
