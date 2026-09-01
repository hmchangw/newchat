# hr-sync-worker — Production Readiness Review

**Service:** `hr-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

A small consumer with correct `errcode.Permanent`-vs-transient tiering, clean `jsretry` discipline, `jobguard` panic containment and good WHY-comments — but three structural problems, each of which is a single-point failure for the HR feed.

**A permanent store error wedges the site's lane forever.** Every store failure is classified transient, `MaxDeliver` is forced to `-1` and `MaxAckPending=1`, so a non-retryable Mongo error retries indefinitely while blocking every subsequent batch. It is reachable today: `portal-service` enforces a **unique `account` index** on `hr_employee` while this worker upserts keyed on `_id = employeeId`, so a rehire yields a permanent E11000 that never becomes permanent to the worker — and the only health check is NATS liveness, which stays green throughout. **A quit deletes across sites**: the batch carries `SiteID`, the handler discards it, and the delete filters on `account` alone. And **stream ownership is inverted** — this consumer creates the producer's stream, while a sibling consumer's code states the opposite ownership model outright.

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
- `high` — a **consumer creates the producer's stream**. `bootstrapStreams` is the only place in the repo that creates `HR-{siteID}` (`hr-sync-worker/bootstrap.go:29-37`); the publisher `teams-hr-sync` creates nothing (`teams-hr-sync/publisher.go:38,50,67` — no `CreateOrUpdateStream` anywhere in that service), while `search-sync-worker/main.go:306-307` states the model outright: "HR is owned by hr-syncer. search-sync-worker is a pure consumer … and must not create their schemas." Two consumers therefore hold contradictory beliefs about who owns HR.
- `medium` — shutdown has no `wg.Wait()` equivalent. `cc.Stop()` returns immediately (`main.go:96-102`) and Mongo is disconnected two steps later (`main.go:105-108`), so an in-flight `HandleMessage` can hit a closed client. CLAUDE.md's worker order is `Stop → wg.Wait → Drain → disconnect`; the o11y facade exposes `Drain` for exactly this ("Stop for immediate teardown, Drain for drain-and-wait", `o11y@v0.11.0/nats/jetstream.go:183`) and `outbox-worker/main.go:201` documents the same race it guards with a WaitGroup.
- `medium` — `bootstrapStreams` does not no-op when disabled; it issues `js.Stream()` per site (`bootstrap.go:39-41`). CLAUDE.md says the helper "no-ops when `Enabled=false`". The fail-fast is defensible engineering but is a deviation from binding project law, and it makes startup depend on all `SITE_IDS` streams pre-existing in the **local** NATS — with no `HRJetStreamDomain` escape hatch of the kind `search-sync-worker/main.go:64` needed for exactly this stream.
- `low` — the Mongo implementation lives in `store.go:38-118` rather than `store_mongo.go`; peers split it (`roomlist-worker/store_mongo.go`, `message-worker/store_mongo.go`, `bot-message-worker/store_cassandra.go`).
- `low` — `deploy/Dockerfile:7` copies `teams-hr-sync/transform/`, which this service does not import (its only non-stdlib imports are `pkg/*`, nats, mongo-driver, o11y). Dead build input.

Correct: consumer-owned `Store` interface with three methods (`store.go:25-36`), constructor DI (`handler.go:19`), typed `caarlos0/env` config with `required,notEmpty` on URI/SITE_IDS, `mongoutil.PoolConfig` mounted as a named field (`config.go:20`), `stream.ConsumerSettings` with `envPrefix` (`config.go:22`), `pkg/shutdown.Wait` at 25s.

### Recommendations
- `high` — move HR stream creation to `teams-hr-sync` (the declared owner) and reduce this service to verify-only, or amend the two contradictory comments so one owner is named.
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
