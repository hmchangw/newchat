# teams-hr-sync — Production Readiness Review

**Service:** `teams-hr-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

The producer of the workforce feed the whole Teams pipeline depends on, and the service where documentation drift has become a correctness risk in its own right. The code itself is small and well-factored — a well-chosen `emitter` seam for its two modes, correct `pkg/subject` and `pkg/idgen` usage, a request ID minted and propagated onto outbound messages, and a hand-rolled shutdown that is *correct* (the drain deadline is deliberately detached from the cancelled context).

Three things stand out. **`README.md` — which explicitly presents itself as the contract for "an external persister" replacing this worker — is materially wrong in four places**, including a `pkg/hrstore` package that does not exist and a `source:"teams"` scoping that the query does not perform. **A partial publish loses the users half of the feed forever**: the employees upsert is published first and persisted downstream, so the next run's diff finds the rows equal and never re-emits the users — directly contradicting `main.go:38-39`'s claim that "a lost publish self-heals". And **the entire direct-write path is at 0% coverage**, including the two guards whose own comments describe data corruption.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 2 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 5 | 10 | 9 | 5 | **31** |

---

## 2. Go code quality — 4 / 5

Error wrapping, naming and secret hygiene are genuinely good; the gaps are request-ID log coverage and missing `bson` tags on the wire structs this service owns.

### Findings
- `medium` — `requestId` is attached to only two log lines; every other log call uses the package-level `slog` and carries no correlation id — `teams-hr-sync/main.go:94` vs `main.go:147`, `write_store.go:69`. `WarnContext(ctx)` at `write_store.go:69` looks ctx-aware but the handler installed at `main.go:27` is a plain `slog.NewJSONHandler` that ignores ctx, so the id is dropped. CLAUDE.md requires the id "in all log lines".
- `medium` — `model.IEmployeeWithChange.ChangeType` and `IUserWithChange.ChangeType` have a `json` tag but no `bson` tag; `IHRSyncEmployeeQuitBatch` has no `bson` tags at all — `pkg/model/teams_employee.go:47,53,60-62`. CLAUDE.md: "All model structs get both `json` and `bson` tags."
- `low` — No log-level knob: `slog.NewJSONHandler(os.Stdout, nil)` hardcodes Info — `main.go:27`. CLAUDE.md asks for an `envDefault` log level.
- `low` — `.semgrep/msgraph-secrets.yml` — the rule that guards the Graph credential path this service feeds (`main.go:81-86`) — has **no fixture**: there is no `.semgrep/msgraph-secrets.go` beside it, so `make sast-semgrep-test` never exercises it (only `metrics.go` exists). CLAUDE.md §2 warns an unverified rule "can be disabled by a pattern edit without any scan failing".
- `low` — SAST audit coverage: gosec + repo-owned semgrep clean repo-wide; `govulncheck` and semgrep registry packs could not run (blocked egress, per GLOBAL_PREP) — environmental, not a defect.
- `nitpick` — CI runs raw `go vet`/`go test` and has neither a lint nor a `sast` stage, despite CLAUDE.md §5 calling SAST a blocking gate — `teams-hr-sync/deploy/azure-pipelines.yml:36-46`. Fleet-wide (only `translation-service` has it).

Secret handling verified clean: `TEAMS_CLIENT_SECRET` is read into config (`config.go:18`), passed straight to `msgraph.Config` (`main.go:84`), and never appears in any log or error; `grep` over all non-test files shows no secret reaching a sink.

### Recommendations
- `medium` — Replace the bare handler at `main.go:27` with one that reads the request id off ctx (or pass `logger` into `runStreamMode`/`runDirectMode`/the write store) so `main.go:147` and `write_store.go:69` correlate.
- `medium` — Add `bson` tags to `ChangeType` (both structs) and to `IHRSyncEmployeeQuitBatch`.
- `low` — Add `.semgrep/msgraph-secrets.go` fixture with `// ruleid: msgraph-no-credential-body-logging` lines so the credential rule is actually tested.
- `low` — Add `LOG_LEVEL` with `envDefault:"info"`; add lint + sast stages to the pipeline, copying `translation-service`.

---

---

## 3. Architecture — 4 / 5

Clean consumer-defined stores, a well-chosen `emitter` seam for the two modes, and correctly *no* stream bootstrap; deducted for the shutdown-helper deviation and a mapper interface defined implementer-side.

### Findings
- `medium` — No `pkg/shutdown.Wait`; shutdown is hand-rolled via `signal.NotifyContext` + defers — `main.go:71-72,141-149,246-250`. CLAUDE.md: "Use `pkg/shutdown.Wait` in every service's `main.go`." The hand-rolled version is *correct* (the drain deadline is deliberately detached from the cancelled ctx at `main.go:144`), but it is a documented deviation with no note saying why the helper does not fit a one-shot job.
- `low` — `transform.Mapper` / `EmployeeUserConverter` are declared in the same package as their only implementations — `transform/transform.go:20-28` beside `DefaultMapper` at :35. CLAUDE.md: "Define interfaces in the consumer, not the implementer." `Store`/`WriteStore` get this right (`store.go:14`, `write_store.go:22`).
- `low` — File layout omits `handler.go`/`routes.go` and adds `collect.go`/`differ.go`/`emitter.go`/`publisher.go`. Defensible for a CronJob with no handlers, but it is not the CLAUDE.md per-service layout and no README line claims the exception.
- `nitpick` — `obs.Init` is deliberately skipped (`main.go:135-136`), so the job emits no traces or metrics — only the end-of-run log line at `main.go:104-119`. Justified in-comment; flagged so the operability trade-off is explicit.

Verified compliant: no stream creation anywhere (`deploy/docker-compose.yml` comment confirms the HR stream is consumer-owned); config is a typed `caarlos0/env` struct with `required,notEmpty` on all secrets/URIs and `envDefault` on knobs (`config.go:15-63`); cross-field validation done in `run()` with fail-fast (`main.go:46-59`); `Pool mongoutil.PoolConfig` is mounted as a named field with no re-declared env tags (`config.go:45`); DI is constructor-based throughout.

### Recommendations
- `medium` — Either adopt `pkg/shutdown.Wait` or add a one-line comment in `main.go` recording why a one-shot CronJob binary opts out.
- `low` — Move `Mapper`/`EmployeeUserConverter` declarations into `package main` (the consumer), leaving the default impls in `transform`.
- `low` — Note the handler-less layout exception in `README.md` so the next reviewer does not read it as drift.

---

---

## 4. Test coverage — 1 / 5

**57.5% (266 statements)** — below the 60% line, so per CLAUDE.md §4 this is a `critical` finding with the score floored at 1; the whole direct-write path is at 0%.

### Findings
- `critical` — 57.5% statement coverage, below both the 60% floor and the 80% merge gate — `coverage_by_service.txt`.
- `critical` — **Every `WriteStore` method is 0.0%**: `UpsertEmployees` (`write_store.go:44`), `UpsertUserIdentities` (:62), `QuitTeamsEmployees` (:98), `newMongoWriteStore` (:37). The two guards whose own comments describe data corruption — the empty-`employeeId` skip ("would match every other keyless row and clobber it", `write_store.go:68-71`) and the empty-`models` no-op that avoids `mongo.ErrEmptySlice` (:89-91) — have zero tests. `integration_test.go` covers stream mode only; there is no direct-mode integration test at all.
- `high` — `integration_test.go:162` asserts `"only the teams-sourced departure quits; the legacy row never does"`, but the test never inserts a legacy row (the only inserts are the run-1 docs at :132) and the store has no source filter. The assertion message documents behavior the test does not exercise — false confidence.
- `medium` — Uncovered error branches that matter: the quit-write failure in `directEmitter.emit` (`emitter.go:70-72`, function at 83.3%), `publishZstd`'s marshal failure (`publisher.go:78-79`, 75%), and `ListTeamsEmployees`'s find error (`store_mongo.go:59-61`, 0% in the unit profile).
- `low` — Package-level shared `zstdTestDecoder` at `publisher_test.go:17` is mutable state shared across tests; safe in practice but against the independence rule.

Quality is otherwise strong: table-driven with descriptive subtests (`config_test.go:66-99`, `differ_test.go:20`), `package main`, generated mocks (`mock_store_test.go`, `mock_write_store_test.go`), publish injected as a field so no NATS is needed (`publisher.go:18`, `main_test.go:53`), `//go:build integration` + `TestMain(m){testutil.RunTests(m)}` (`integration_test.go:1,165`), containers from `testutil.MongoDB`/`testutil.NATS` (:73-74).

### Recommendations
- `critical` — Add a direct-mode integration test (`testutil.MongoDB`) asserting: employees upsert keyed on `_id == employeeId`, identity-only `$set` leaving a pre-existing `roles`/`password` field untouched, empty-`employeeId` rows skipped, and all-empty input writing nothing.
- `high` — Either add a source discriminator and filter on it in `ListTeamsEmployees`, or delete the misleading assertion message at `integration_test.go:162` and insert a real foreign row to prove whichever behavior is intended.
- `medium` — Add table cases for the three uncovered error branches (`emitter.go:70`, `publisher.go:78`, `store_mongo.go:59`).
- `low` — Move `zstdTestDecoder` construction inside the test that uses it.

---

---

## 5. Maintainability — 3 / 5

The code is small, well-factored and readable; the documentation and several load-bearing comments describe a service that no longer exists.

### Findings
- `high` — `README.md` is materially wrong in four places: it points the direct-write surface at `pkg/hrstore` "shared with `hr-sync-worker`" (:14-15, :48-51) — **that package does not exist** (`ls pkg/`), and `write_store.go:20-21` says the opposite ("Owned by this service … there is no shared store package"); it claims stream mode diffs `hr_employee` rows "(`source:"teams"` only)" (:8) when the query is an unfiltered `bson.M{}` (`store_mongo.go:57`) and `model.IEmployee` has no source field; it documents a `Source` field on `EmployeeFromMember` (:44) and a `transform.SourceTeams` tag (:54) that do not exist; and it names the constants `model.ChangeTypeNewHire`/`ChangeTypeUpdate` (:53) when they are `IChangeType*`.
- `medium` — `store.go:12` states "this producer never writes `hr_employee`", but `write_store.go:39` writes exactly that collection via the same `hrEmployeeCollection` constant. The comment predates direct mode and now misleads at the interface that most needs trust.
- `medium` — The `[]model.IUserWithChange` build loop is duplicated verbatim between `publisher.go:43-49` and `emitter.go:51-57`; the converter seam already exists, so a `usersFrom(converter, upserts)` helper removes it.
- `low` — `deploy/docker-compose.yml:19` sets `ORG_TYPE=group`, which no config field reads (`transform.go:34` notes the field was removed).
- `nitpick` — `transform.go:34` carries a leftover working note ("ponytail: no OrgType") in an exported package doc comment.
- `nitpick` — `QuitTeamsEmployees` hard-deletes (`write_store.go:99`) rather than marking a status; the name implies a state change.

No dead code, no oversized file (largest is `main.go` at 250 lines), no function above ~25 lines, comments overwhelmingly explain WHY (`main.go:35-39`, `:142-143`, `write_store.go:59-61`, `publisher.go:63-66` are all good examples).

### Recommendations
- `high` — Rewrite `README.md` against the current code: drop `pkg/hrstore`, drop the `source:"teams"` and `Source`/`SourceTeams` claims, fix the `IChangeType*` names, and document `WriteStore` as service-owned.
- `medium` — Fix the `store.go:12` comment to say the read surface is read-only while direct mode writes through `WriteStore`.
- `medium` — Extract the duplicated user-conversion loop into one helper used by both emitters.
- `low` — Delete `ORG_TYPE` from the compose file and the "ponytail" note from the package comment.

---

---

## 6. Integration — 3 / 5

Subjects and IDs go through the right builders, but the two upsert event structs carry no `Timestamp` and a partial publish permanently drops half the feed.

### Findings
- `high` — A partial publish loses the users half forever. `publishSync` publishes `employees.upsert` first and `users.upsert` second (`publisher.go:38` then :50); `hr-sync-worker` applies them as independent subjects (`hr-sync-worker/handler.go:23,34`). If the second publish fails, the employee rows are already persisted, so the next run's diff finds them equal (`differ.go:34`) and never re-emits the users. This contradicts `main.go:38-39`'s claim that "a lost publish self-heals".
- `medium` — CLAUDE.md: "Every NATS event struct in `pkg/model` must include a `Timestamp int64`". `IEmployeeWithChange` and `IUserWithChange` — both published as JetStream payloads (`publisher.go:38,50`) — have none (`pkg/model/teams_employee.go:45-54`). Only the quit batch has one, and it is set correctly at the publish site via `time.Now().UTC().UnixMilli()` (`publisher.go:68`).
- `medium` — The diff's equality test is whole-struct (`differ.go:34`) and the projection includes `_id` (`store_mongo.go:39` derives it from the `bson:"_id"` tag at `pkg/model/teams_employee.go:32`), while the mapper sets `ID = EmployeeID` (`transform.go:49`). Correctness therefore depends on the downstream writing `_id == employeeId` — enforced nowhere in this service, and the integration test has to stamp it by hand to make the diff work (`integration_test.go:129`). A downstream `_id` change makes every row diff as `update` on every run, republishing the whole org.
- `low` — `EmployeesQuit(p.central)` passes the central site to a builder documented as "a site's HR feed" (`pkg/subject/subject.go:1778-1783`), so the subject's site token no longer means what its name says. The reason is well argued in-comment (`publisher.go:63-66`), but the builder doc now contradicts its only caller.

Verified compliant: all three subjects come from `pkg/subject` builders, never `fmt.Sprintf` (`publisher.go:38,50,67`); `employeeId` uses `idgen.DeterministicID` → 17-char base62 (`transform.go:68`, `pkg/idgen/idgen.go:86-89,22`), matching the CLAUDE.md channel-room/native-user shape and pinned to `teamsmigrate.EmployeeIDFromGraphID`; the request id is minted with `idgen.GenerateRequestID()` and propagated onto outbound messages via `natsutil.WithRequestID` + `NewMsgEncoded` (`main.go:91-93`, `pkg/natsutil/request_id.go:79-88`). No `chat.user.*` handler and no HTTP route, so `docs/client-api.md` is correctly not in scope. Not an OUTBOX/INBOX participant, so the partition rules do not apply.

### Recommendations
- `high` — Make the two upserts atomic from the consumer's point of view — publish one combined batch on a single subject, or have the differ re-emit users whenever the stored row lacks a matching user identity.
- `medium` — Add `Timestamp int64 \`json:"timestamp" bson:"timestamp"\`` to `IEmployeeWithChange`/`IUserWithChange` and set it at `publisher.go:38,50`.
- `medium` — Exclude `_id` from the diff comparison (compare a projection struct, or clear `ID` on both sides before `!=`) so the producer stops depending on the consumer's key choice.
- `low` — Rename or re-document `subject.EmployeesQuit` to reflect that it is published on the central site.

---
