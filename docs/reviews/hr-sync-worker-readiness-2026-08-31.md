# hr-sync-worker — Production Readiness Review

**Service:** `hr-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

A small consumer with clean `jsretry` discipline, `jobguard` panic containment and good WHY-comments. Its `errcode.Permanent`-vs-transient tiering is correct on the decode/poison side but **not on the store side** — every store failure is classified transient (see below), so a non-retryable Mongo error retries forever — but three structural problems, each of which is a single-point failure for the HR feed.

**A permanent store error wedges the site's lane forever.** Every store failure is classified transient, `MaxDeliver` is forced to `-1` and `MaxAckPending=1`, so a non-retryable Mongo error retries indefinitely while blocking every subsequent batch. It is reachable today: `portal-service` enforces a **unique `account` index** on `hr_employee` while this worker upserts keyed on `_id = employeeId`, so a rehire yields a permanent E11000 that never becomes permanent to the worker — and the only health check is NATS liveness, which stays green throughout. **A quit deletes across sites**: the batch carries `SiteID`, the handler discards it, and the delete filters on `account` alone. And **stream ownership is inverted** — this consumer creates the producer's stream, while a sibling consumer's code credits "hr-syncer" (the publisher, `teams-hr-sync`) with owning it under a name no service directory carries.

Coverage is 21.1%, the lowest in the fleet: every store method and all bootstrap/consumer wiring is at 0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 6 | 12 | 8 | 1 | **28** |

---

## 2. Go code quality — 4 / 5

Small, idiomatic, correctly-tiered error handling with one bare-`err` return and some `pkg/model` tag drift on the events it consumes.

### Findings
- `medium` — bare `err` returned without context, violating "never return bare `err`" — `hr-sync-worker/main.go:120`
  Caller logs `"start site consumer failed"` with the site, but the consumer/stream name is lost; should be `fmt.Errorf("create %s consumer: %w", streamCfg.Name, err)`.
- `medium` — README documents behaviour the code does not implement: "Replace `hr_employee` by `{account, source}`" and quit "scoped `{account ∈ batch, source: "teams"}` — legacy-source rows survive" — `hr-sync-worker/README.md:9,11`. The upsert keys on `_id = employeeId` (`store.go:58-60`), the delete filters on `account` alone with no `source` predicate (`store.go:112-114`), and `model.IEmployee` has no `Source` field at all (`pkg/model/teams_employee.go:29-42`). Legacy-source rows do NOT survive a quit.
- `low` — `model.IHRSyncEmployeeQuitBatch` carries `json` tags only, no `bson` tags, against CLAUDE.md §3 "All model structs get both" — `pkg/model/teams_employee.go:59-63` (same for `ChangeType`, `:47`, `:53`).
- `low` — the HR subjects carry the whole workforce's `mail`/`engName`/`chineseName`; `logctx.CapturePayload` logs the full body and its denylist covers only `.sso.set`/`.sso.refresh` — `pkg/logctx/limiter.go:90-97`, invoked at `hr-sync-worker/main.go:124`. Double-gated (`DEBUG_LOG_PAYLOADS` + `X-Debug-Payload`) so off by default, but PII-bearing when enabled.
- `low` — SAST audit-coverage gap, not a service defect: gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked) per `GLOBAL_PREP.md`.

Correct-by-CLAUDE.md and worth noting: `errcode.Permanent(errcode.BadRequest(...))` for poison vs raw `fmt.Errorf` for infra (`handler.go:26,32`), no log-and-return, `slog` only, no `os.Getenv`.

### Recommendations
- `medium` — wrap the consumer-creation error at `main.go:120` with stream + site.
- `medium` — reconcile `README.md:9-11` with the code: either implement the `source` predicate or delete the claim (it is the contract other teams read).
- `low` — add `bson` tags to `IHRSyncEmployeeQuitBatch` / `ChangeType`.
- `low` — extend the `CapturePayload` denylist to `chat.hr.>` (workforce PII) or redact `mail`.

---

---

## 3. Architecture — 3 / 5

Clean consumer/store/handler separation with proper DI and a `BOOTSTRAP_STREAMS` gate, undermined by a stream-ownership inversion and a shutdown path that never waits for in-flight work.

### Findings
- `high` — a **consumer creates the producer's stream**. `bootstrapStreams` is the only place in the repo that creates `HR-{siteID}` (`hr-sync-worker/bootstrap.go:29-37`); the publisher `teams-hr-sync` creates nothing (`teams-hr-sync/publisher.go:38,50,67` — no `CreateOrUpdateStream` anywhere in that service), while `search-sync-worker/main.go:306-307` states the model outright: "HR is owned by hr-syncer. search-sync-worker is a pure consumer … and must not create their schemas."
  - **"hr-syncer" is a third name, and that is half the problem.** No directory by that name exists. It means the *publisher*: `pkg/stream/stream.go:109` ("`HR-{centralSiteID}`, populated daily by hr-syncer at the central site") and `search-sync-worker/spotlight_org.go:50` ("hr-syncer publishes into `HR-{centralSiteID}`") both use it for the thing that writes, which in this repo is `teams-hr-sync`. Read quickly, "hr-syncer" looks like `hr-sync-worker` — this consumer — and the inversion disappears. An earlier draft of this report leaned on the phrase without saying which service it names.
- `medium` — shutdown has no `wg.Wait()` equivalent. `cc.Stop()` returns immediately (`main.go:96-102`) and Mongo is disconnected two steps later (`main.go:105-108`), so an in-flight `HandleMessage` can hit a closed client. CLAUDE.md's worker order is `Stop → wg.Wait → Drain → disconnect`; the o11y facade exposes `Drain` for exactly this ("Stop for immediate teardown, Drain for drain-and-wait", `o11y@v0.11.0/nats/jetstream.go:183`) and `outbox-worker/main.go:201` documents the same race it guards with a WaitGroup.
- `medium` — `bootstrapStreams` does not no-op when disabled; it issues `js.Stream()` per site (`bootstrap.go:39-41`). CLAUDE.md says the helper "no-ops when `Enabled=false`". The fail-fast is defensible engineering but is a deviation from binding project law, and it makes startup depend on all `SITE_IDS` streams pre-existing in the **local** NATS — with no `HRJetStreamDomain` escape hatch of the kind `search-sync-worker/main.go:64` needed for exactly this stream.
- `low` — the Mongo implementation lives in `store.go:38-118` rather than `store_mongo.go`; peers split it (`roomlist-worker/store_mongo.go`, `message-worker/store_mongo.go`, `bot-message-worker/store_cassandra.go`).
- `low` — `deploy/Dockerfile:7` copies `teams-hr-sync/transform/`, which this service does not import (its only non-stdlib imports are `pkg/*`, nats, mongo-driver, o11y). Dead build input.

Correct: consumer-owned `Store` interface with three methods (`store.go:25-36`), constructor DI (`handler.go:19`), typed `caarlos0/env` config with `required,notEmpty` on URI/SITE_IDS, `mongoutil.PoolConfig` mounted as a named field (`config.go:20`), `stream.ConsumerSettings` with `envPrefix` (`config.go:22`), `pkg/shutdown.Wait` at 25s.

### Recommendations
- `high` — resolve it by **declaration, not by moving the code**: name `hr-sync-worker` the owner of `HR-{siteID}` in `pkg/stream/stream.go:109`, `search-sync-worker/main.go:306` and `spotlight_org.go:50`, replacing "hr-syncer" with the actual service names throughout. **Do not move creation to `teams-hr-sync`**, which an earlier draft of this report suggested: it is a one-shot K8s CronJob (`teams-hr-sync/README.md:3`, `main.go:95-104`), so a long-running consumer's startup would depend on a batch job having already run — and under `HR_SYNC_MODE=direct` that job skips JetStream entirely, so the stream would never be created at all.
- `medium` — replace `cc.Stop()` with `cc.Drain()` + wait on `Closed()` before `nc.Drain()`/Mongo disconnect.
- `medium` — keep the verify branch but document the deviation in `bootstrap.go`, and add an `HR_JETSTREAM_DOMAIN` knob if this worker is really expected to drain remote sites' streams.
- `low` — split `store_mongo.go` out; drop the unused `COPY` line in the Dockerfile.

---

---

## 4. Test coverage — 1 / 5

Coverage is **21.1% (128 statements)** — far below the 60% critical line, with every store method and the entire bootstrap/consumer wiring at 0%.

### Findings
- `critical` — 21.1% vs CLAUDE.md's 80% floor. Zero-coverage functions: `bootstrapStreams` (`bootstrap.go:27`), `main` (`main.go:31`), `startSiteConsumer` (`main.go:116`), `newMongoStore` (`store.go:44`), `UpsertEmployees` (`store.go:51`), `UpsertUserIdentities` (`store.go:69`), `QuitTeamsEmployees` (`store.go:111`). Only `HandleMessage` (91.7%), `NewHandler`, `buildConsumerConfig` are covered.
- `high` — `bootstrapStreams` has a purpose-built injectable seam ("`streamManager` … injected by tests", `bootstrap.go:19-23`) and **no test uses it**. Untested: the enabled/disabled branch split, the per-site loop, and the `verify` failure that is this service's production startup gate.
- `high` — `UpsertUserIdentities` is the service's riskiest code (it writes the live auth store) and its unit coverage is 0%. The empty-account skip (`store.go:75-78`), the publisher-supplied-vs-minted `_id` branch (`store.go:82-85`), the conditional `employeeId` (`store.go:89-91`) and the all-skipped `len(models)==0` no-op (`store.go:102-104`) are exercised only by the integration test.
- `high` — the assertions that actually protect production — "roles must never be touched" / "services must never be touched" (`integration_test.go:119-120`) — never run in CI: `deploy/azure-pipelines.yml:44` runs `go test ./hr-sync-worker/...` with no `-tags=integration`. (Fleet-wide: no service pipeline sets that tag.)
- `medium` — the two uncovered `HandleMessage` blocks are precisely the store-error → Nak-retry paths for `users.upsert` (`handler.go:42-44`) and `employees.quit` (`handler.go:53-55`); only the employees path is asserted (`handler_test.go:76-85`).
- `medium` — `integration_test.go:30-50` re-implements `startSiteConsumer` instead of calling it, using `stream.DurableConsumerDefaults` directly rather than `buildConsumerConfig` and omitting `jobguard.Run`, so neither the real consumer config nor panic containment is ever exercised end to end.

Good: `package main` tests, `go.uber.org/mock` mocks unedited and up to date (`GLOBAL_PREP.md`), table-driven malformed-payload subtest (`handler_test.go:51-60`), `TestMain` via `testutil.RunTests` and `testutil.MongoDB`/`testutil.NATS` (`integration_test.go:26,68-69`).

### Recommendations
- `critical` — unit-test `bootstrapStreams` through the existing `streamManager` seam: enabled creates one stream per site with `Name+Subjects` only, disabled verifies, verify-failure returns wrapped.
- `high` — add store unit tests (or promote the integration assertions) for `UpsertUserIdentities`: empty account skipped, supplied `_id` honoured, minted `_id` on insert, `employeeId` omitted for externals, all-empty input writes nothing.
- `high` — add `-tags=integration` to a pipeline stage so the "auth fields untouched" invariant is actually gated.
- `medium` — extend `TestHandleMessage_StoreErrorIsTransient` into a table across all three subjects.
- `medium` — have the integration test call `startSiteConsumer`/`buildConsumerConfig` rather than a copy.

---

---

## 5. Maintainability — 3 / 5

Nine small files with genuinely good WHY-comments; the drag is a README that misdescribes the contract and wiring duplicated between production and test.

### Findings
- `medium` — README is the published contract for "an external persister can replace this worker" (`README.md:14-15`) and two of its three rows are wrong (see D1). A replacement implementation built from it would filter on a `source` field that does not exist.
- `medium` — `store.go:56-57` says "`_id` = employeeId … keys the upsert", but `BulkUpsert` is documented as "$set per item (MERGE not REPLACE)" (`pkg/mongoutil/collection.go:181`), and the nine `IOrg` fields are `omitempty` (`pkg/model/teams_employee.go:6-15`). An employee moving out of a department leaves the old `sectName`/`deptName` in `hr_employee` forever — no code path clears them.
- `low` — the consumer wiring exists twice (`main.go:116-134` and `integration_test.go:30-50`) and has already diverged (no `jobguard`, different consumer settings in the copy).
- `nitpick` — `handler.go:22-59` repeats unmarshal → empty-check → store-call three times; readable today, but a fourth subject makes a small generic helper worthwhile.

Positives: no file over 146 lines, no function over ~30, no dead code, comments explain WHY (`main.go:113-115`, `store.go:66-68`, `store.go:80-82`, `store.go:100-101`) rather than restating WHAT.

### Recommendations
- `medium` — rewrite the README table from the code (`_id = employeeId`, unqualified account delete) or implement the documented `source` scoping.
- `medium` — decide merge-vs-replace explicitly for `hr_employee`; if org fields must be clearable, drop `omitempty` on `IOrg` or use `ReplaceOne`.
- `low` — export `startSiteConsumer` wiring for reuse by the integration test.

---

---

## 6. Integration — 3 / 5

Correct stream/consumer plumbing and no client-facing surface to document, but the subject's site token and the payload's `siteId` are both discarded, and two of three event types carry no event timestamp.

### Findings
- `high` — `IHRSyncEmployeeQuitBatch.SiteID` (`pkg/model/teams_employee.go:61`) is unmarshalled and **never used**: `handler.go:53` passes only `batch.Accounts`, and `QuitTeamsEmployees` deletes `{"account": {"$in": accounts}}` with no `siteId` predicate (`store.go:112-114`). The publisher sends every site's quits to the single central subject with the site in the body (`teams-hr-sync/publisher.go:67-68`), so a departure at site-b deletes that account's `hr_employee` row regardless of which site it belongs to.
- `medium` — CLAUDE.md requires every `pkg/model` NATS event struct to carry `Timestamp int64` set at the publish site. `employees.upsert` and `users.upsert` are bare arrays of `IEmployeeWithChange`/`IUserWithChange` (`pkg/model/teams_employee.go:45-54`) with no event timestamp, and the publish sites set none (`teams-hr-sync/publisher.go:38,50`). Only the quit batch complies (`publisher.go:68`, `time.Now().UTC().UnixMilli()`).
- `medium` — routing uses `strings.HasSuffix` on the raw subject (`handler.go:23,34,45`) while `pkg/subject` already owns these subjects (`pkg/subject/subject.go:1769,1775,1783`). The site token is never compared, so a message from any site's stream is applied identically, and `chat.hr.x.anything.employees.upsert` would match.
- `low` — `integration_test.go:98,99,135` hardcode `"chat.hr.site-a.employees.upsert"` etc. instead of the `subject.OrgSyncEmployeesUpsert`/`OrgSyncUsersUpsert`/`EmployeesQuit` builders, so a builder change would not fail this test.

Correct: stream name/subjects from `stream.OrgSyncStream` (`main.go:117`, `bootstrap.go:29`), durable consumer named after the service (`main.go:29,143`), `natsutil.DecodePayload` matching the publisher's zstd framing, request-ID stamping via `logctx.ConsumeContext` (`main.go:124`). No `chat.user.*` handler and no HTTP route beyond `/healthz`, so `docs/client-api.md` is correctly untouched; no OUTBOX/INBOX/Cassandra/`idgen` entity-format surface applies, and the one id it mints is `idgen.GenerateUUIDv7()` (`store.go:84`).

### Recommendations
- `high` — scope the quit delete by `siteId` (from `batch.SiteID`) — or state in code why cross-site account deletion is intended.
- `medium` — add an event-level `Timestamp` to the two upsert payloads (wrapper struct) and set it at `teams-hr-sync/publisher.go:38,50`.
- `medium` — route on `subject.OrgSync*` builders (compare full subject) instead of suffix matching.
- `low` — use the builders in the integration test.

---

---

## 7. Performance — 3 / 5

No allocation, N+1 or goroutine problems at this volume, and `jsretry` is used correctly — but the retry/concurrency configuration turns any permanent store error into an indefinite stall of the site's lane.

### Findings
- `high` — **poison-message lane wedge.** Every store failure is classified transient (`handler.go:31-33,42-44,53-55`), `MaxDeliver` is forced to `-1` (`main.go:142`, via `stream.WithUnlimitedRedelivery`, `pkg/stream/consumer.go:174-176`) and `MaxAckPending=1` (`main.go:144`). A non-retryable Mongo error therefore retries forever while blocking every subsequent batch on that stream. It is reachable: `portal-service` enforces a **unique `account` index on `hr_employee`** (`portal-service/store_mongo.go:27-34`) while this worker upserts keyed on `_id = employeeId` (`store.go:58-60`) — a rehire (same account, new employeeId) yields a permanent E11000 that never becomes permanent to the worker.
- `medium` — no delivery-count ceiling or alerting on the wedge: `msg.Metadata().NumDelivered` is available inside `jsretry` but nothing in this service escalates a message to `errcode.Permanent` after N attempts, and the only health check is NATS liveness (`main.go:87`), which stays green while the lane is stuck.
- `medium` — whole-batch processing with no chunking: the entire array is unmarshalled (`handler.go:24,35`) and turned into one `WriteModel` slice (`store.go:52,70`); a daily full-workforce sync retried on the 10-minute tail of `jsretry.DefaultBackoff` (`pkg/jsretry/jsretry.go:52-58`) re-does all of it every attempt.
- `low` — no index is asserted by this service on the fields it writes/filters (`account` on `hr_employee`, `account` on `users`); it depends on `portal-service`/`user-service` `EnsureIndexes` having run first, which is nowhere documented here. `QuitTeamsEmployees`' `DeleteMany` on `account` (`store.go:112`) is a collection scan without it.

Correct: `jsretry.Settle` everywhere, no bare `Nak()`/`NakWithDelay(0)` (`main.go:128,131`); `BackOff` derived by `stream.DurableConsumerDefaults`, never hardcoded (`main.go:142`); no `time.Sleep` in production code; `jobguard.Run` contains handler panics (`main.go:123`); one round-trip per subject (unordered BulkWrite, `pkg/mongoutil/collection.go:173`), no `$lookup`, no reads needing projection; `encoding/json` is correct here (not a designated sonic hot-path worker).

### Recommendations
- `high` — drain the lane on a non-retryable Mongo error — but **`errcode.Permanent` alone is the wrong instrument, and this report recommended it bare in an earlier draft.** Permanent Acks the whole batch message, and the failed items in it are real HR updates: they are silently lost, and because `teams-hr-sync`'s stream mode diffs against the persisted `hr_employee` rows, the next run re-derives the same delta and fails identically — a recurring silent loss, not a one-off. Three steps, in order:
  1. **Reconcile the key conflict that produces the error.** The upsert keys on `_id = employeeId` (`store.go:58-60`) while the unique index is on `account` (`portal-service/store_mongo.go:27-34`), so a rehire is a guaranteed E11000. Keying the upsert on `account`, or handling the rehire as an update of the existing account row, removes the case rather than classifying it.
  2. **Isolate the failed items.** An unordered `BulkWrite` applies what it can and reports per-item failures, so the batch's outcome is not one verdict — read the write errors and separate the rejected records from the applied ones.
  3. **Persist the rejected records durably** — a quarantine collection or dead-letter subject carrying the record and its error, with an alert on depth — and only then Ack. `errcode.Permanent` is what expresses that Ack once the record is safely elsewhere; on its own it is a delete.
- `high` — pair `MaxDeliver=-1` with a `NumDelivered` threshold that **alerts and quarantines** the batch (dead-letter subject or a parked collection it can be replayed from), not one that escalates to `errcode.Permanent`. A blanket escalation Ack-drops on attempt N, so a Mongo outage merely longer than N attempts becomes silent HR data loss — trading a visible wedge for an invisible one. Permanent classification belongs on errors proven non-retryable (duplicate key, document validation) **and only once the rejected records are durably quarantined** — see the recommendation above, which is the same requirement arriving from the other direction.
- `medium` — reconcile the `hr_employee` key: upsert on `account` (matching the unique index and the README) or drop the unique index — today the write key and the enforced constraint disagree.
- `medium` — chunk large batches (e.g. 1k `WriteModel`s per BulkWrite) so a retry is not all-or-nothing.
- `low` — document (or assert) the index prerequisite for `account` on both collections.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Reconcile the `_id`-vs-unique-`account` conflict, quarantine rejected records, then drain the lane (and add a `NumDelivered` ceiling that alerts, not Ack-drops)** | Performance | every store failure transient at `handler.go:31-33`, `:42-44`, `:53-55`; `MaxDeliver=-1` `main.go:142`; `MaxAckPending=1` `main.go:144` | A permanent error **retries forever while blocking every subsequent batch on that site's lane.** It is reachable: `portal-service/store_mongo.go:27-34` enforces a unique `account` index on `hr_employee` while this worker upserts on `_id = employeeId`, so **a rehire (same account, new employeeId) is a permanent E11000.** And nothing escalates: `msg.Metadata().NumDelivered` is available inside `jsretry` and unused, while the only health check (`main.go:87`) is NATS liveness, **which stays green while the lane is stuck.** Draining it with a bare `errcode.Permanent` swaps the wedge for silent loss: the rejected records are real HR updates, and the producer's next diff re-derives and re-fails them. |
| 2 | `high` | **Scope the quit delete by `siteId`** | Integration | `SiteID` unmarshalled at `pkg/model/teams_employee.go:61`, discarded at `handler.go:53`; delete filters `{"account": {"$in": …}}` at `store.go:112-114` | The publisher sends **every site's quits to one central subject with the site in the body** (`teams-hr-sync/publisher.go:67-68`). A departure at site-b therefore deletes that account's `hr_employee` row regardless of which site it belongs to. Either filter on it, or state in code why cross-site deletion is intended. |
| 3 | `high` | **Resolve the HR stream-ownership inversion — in the comments, not the code** | Architecture | this consumer is the **only** creator of `HR-{siteID}` (`bootstrap.go:29-37`); the publisher creates nothing (`teams-hr-sync/publisher.go:38`, `:50`, `:67`); `search-sync-worker/main.go:306-307` and `pkg/stream/stream.go:109` attribute HR to "hr-syncer", a name no directory carries | **The repo names three services for one stream and creates it from a fourth position** — the sole creator is a consumer, while the comments credit the publisher under a name ("hr-syncer") that reads like this worker's. Fix by naming `hr-sync-worker` the owner in all three comments; **not** by moving creation to `teams-hr-sync`, a one-shot CronJob that skips JetStream entirely in `direct` mode. |
| 4 | `critical` | **Unit-test `bootstrapStreams` through the seam that already exists for it** | Test coverage | `bootstrap.go:19-23` says `streamManager` is "injected by tests" — **and no test uses it**; whole service at 21.1% | Untested: the enabled/disabled branch split, the per-site loop, and the `verify` failure that **is this service's production startup gate**. The seam was built for this. |
| 5 | `high` | **Cover `UpsertUserIdentities`** | Test coverage | 0% unit coverage at `store.go:69`; branches at `:75-78`, `:82-85`, `:89-91`, `:102-104` | The service's riskiest code — **it writes the live auth store.** The empty-account skip, the supplied-vs-minted `_id` branch, the conditional `employeeId` and the all-skipped no-op are exercised only under the integration tag. |
| 6 | `high` | **Add an `-tags=integration` stage to the pipeline** | Test coverage | `deploy/azure-pipelines.yml:44` | The assertions that actually protect production — **"roles must never be touched", "services must never be touched"** (`integration_test.go:119-120`) — never run in CI. Fleet-wide gap, but this is the service where it guards the auth store. |
| 7 | `medium` | **Drain in-flight handlers before disconnecting Mongo** | Architecture | `cc.Stop()` `main.go:96-102`, Mongo disconnect `:105-108` | `Stop()` returns immediately, so **an in-flight `HandleMessage` can hit a closed client.** CLAUDE.md's worker order is `Stop → wg.Wait → Drain → disconnect`; the o11y facade exposes `Drain` for exactly this, and `outbox-worker/main.go:201` documents the same race it guards against. |
| 8 | `medium` | **Reconcile the `hr_employee` write key with the enforced constraint** | Perf / Maint | upsert on `_id = employeeId` `store.go:58-60` vs unique `account` index `portal-service/store_mongo.go:27-34`; merge semantics at `pkg/mongoutil/collection.go:181` | The write key and the enforced constraint disagree — the root cause of item 1. While here, decide merge-vs-replace: `BulkUpsert` is `$set`-per-item and the nine `IOrg` fields are `omitempty`, so **an employee moving out of a department leaves the old `sectName`/`deptName` in place forever**, with no code path that clears them. |
| 9 | `medium` | **Rewrite `README.md` from the code** | Maintainability | `README.md:9`, `:11` claim a `{account, source}` key and `source:"teams"` quit scoping | `model.IEmployee` **has no `Source` field at all**, and the delete has no source predicate — so "legacy-source rows survive" is false. The README declares itself the contract for an external persister replacing this worker. |
| 10 | `medium` | **Route on `pkg/subject` builders instead of `strings.HasSuffix`** | Integration | `handler.go:23`, `:34`, `:45`; builders exist at `pkg/subject/subject.go:1769`, `:1775`, `:1783` | The site token is never compared, so a message from any site's stream is applied identically, and **`chat.hr.x.anything.employees.upsert` would match.** Same fix removes the hardcoded subjects at `integration_test.go:98`, `:99`, `:135`. |

**Also worth doing.** Chunk large batches (e.g. 1k write models) so a retry on the 10-minute tail of `jsretry.DefaultBackoff` is not all-or-nothing on a full-workforce sync. Wrap the bare `err` at `main.go:120` with the stream and consumer name. Have the integration test call `startSiteConsumer`/`buildConsumerConfig` rather than a divergent copy that omits `jobguard`. Extend `TestHandleMessage_StoreErrorIsTransient` into a table across all three subjects — only the employees path is asserted. Split `store_mongo.go` out of `store.go` to match every peer, and drop the Dockerfile's dead `COPY teams-hr-sync/transform/`. Document (or assert) the `account`-index prerequisite on both collections — `QuitTeamsEmployees`' `DeleteMany` is a collection scan without it. And extend `logctx.CapturePayload`'s denylist to `chat.hr.>`: these subjects carry the whole workforce's `mail`/`engName`/`chineseName`, and the denylist covers only the SSO subjects.
