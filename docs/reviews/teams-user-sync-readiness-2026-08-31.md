# teams-user-sync — Production Readiness Review

**Service:** `teams-user-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

A small, well-built CronJob — textbook consumer-defined store, constructor DI, typed config with `required,notEmpty` on both credentials and both URIs, correctly mounted shared knobs, and business logic at **100% coverage** with every error path covered as a distinct subtest. The headline number (53.4%) understates it badly: the entire gap is `main()` wiring plus a store layer that *is* fully tested — behind `//go:build integration`, which the service's own pipeline never builds.

Three defects matter. **TLS verification of the Microsoft Graph and Azure token endpoints is disabled by default** — `GRAPH_TLS_INSECURE_SKIP_VERIFY` ships `envDefault:"true"`, the credential-bearing `client_credentials` POST rides that transport, and neither the compose file nor the pipeline sets the var, so every environment inherits the insecure default. **A user inserted before their HR row exists is permanently orphaned**: existing ids are skipped before any HR lookup, so the join is never re-attempted and `siteId`/`engName`/`mail` stay empty forever — five downstream consumers read those fields, and `message-worker` errors outright on an HR-less row. And **the directory walk has no 429 handling**, so one throttled response discards the whole run; the chats surface on the same client has a full retry loop and a tenant-wide throttle gate for exactly this.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 5 | 5 | 9 | 1 | **21** |

---

## 2. Go code quality — 4 / 5

Idiomatic, correctly-wrapped errors and disciplined secret-free structured logging throughout; the one real defect is a security-critical config knob that defaults to the insecure value.

### Findings
- `high` — `GraphTLSInsecureSkipVerify` defaults to **true**, so TLS verification of the Microsoft Graph / Azure token endpoint is disabled unless an operator explicitly opts back in — `teams-user-sync/config.go:20`
  The credential-bearing `client_credentials` POST (`pkg/msgraph/msgraph.go:379`) rides that transport (`pkg/msgraph/msgraph.go:252-259`). `deploy/docker-compose.yml` never sets the var, and neither does the pipeline, so every environment inherits the insecure default. `pkg/msgraph/msgraph.go:124-127` itself says "Never enable in production". Fail-safe requires `envDefault:"false"`.
- `medium` — one `Info` log line per HR-unmatched user, unbounded by directory size — `teams-user-sync/handler.go:106`
  The aggregate counter two lines later (`handler.go:117-118`) already carries the same information. A tenant with a large guest population emits one line per guest per run.
- `low` — a failed run logs twice: `run` emits the finished line with `succeeded:false` (`teams-user-sync/main.go:80-89`), then `main` logs `fatal error` (`teams-user-sync/main.go:24`). Both are needed for different reasons, but a reader sees two records per failure.
- `low` — audit-coverage gap, not a service defect: gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress, per `GLOBAL_PREP.md`), so third-party CVE exposure of this service's dependency set is unverified.

**Verified clean:** no secret ever reaches a log — the client secret is only form-encoded (`pkg/msgraph/msgraph.go:379`), token errors surface code-only (`pkg/msgraph/msgraph.go:404`), Graph non-200s surface status-only (`pkg/msgraph/msgraph.go:717`), and Mongo URIs are sanitized before logging (`pkg/mongoutil/mongo.go:66`). Every error is wrapped with what *this* function was doing (`handler.go:46,64,96,120`; `store_mongo.go:60,77,89`); no bare `err`, no string comparison, no `fmt.Println`, no interpolated log fields.

### Recommendations
- `high` — flip `config.go:20` to `envDefault:"false"` and set `GRAPH_TLS_INSECURE_SKIP_VERIFY=true` explicitly in the on-prem deployment manifest that actually needs it.
- `medium` — drop `handler.go:106` to `Debug`, or cap it (log the first N ids per run) and rely on the `hrUnmatched` counter.
- `low` — have `run` log the finished line only on success and let `main` own the failure record, or add the error to the finished line and drop `main.go:24`.
- `low` — re-run `make sast-vuln` from an egress-permitted environment before release.

---

---

## 3. Architecture — 4 / 5

Textbook consumer-defined store, constructor DI and typed env config with correctly mounted shared knobs; the gaps are the missing index-bootstrap hook and a filename that no longer matches its contents.

### Findings
- `medium` — no `EnsureIndexes` method and no index bootstrap anywhere, against CLAUDE.md's "Create indexes in the store constructor or a dedicated `EnsureIndexes` method at startup" — `teams-user-sync/store_mongo.go:43-49`
  The sibling service does exactly this (`teams-chat-sync/store_mongo.go:39`, `teams-chat-sync/main.go:114`). See D6 for the query this omission actually costs.
- `low` — `handler.go` contains no handler; it holds `Syncer`, a batch pipeline. CLAUDE.md's per-service layout defines `handler.go` as "Request/message handling logic". The peer job names the same thing `teams-chat-sync/syncer.go:23` — `teams-user-sync/handler.go:16`
- `low` — no `pkg/shutdown.Wait`; shutdown is `signal.NotifyContext` + deferred disconnects — `teams-user-sync/main.go:49-61`
  CLAUDE.md says "every service's `main.go`". Defensible for a one-shot CronJob binary and consistent with the other one-shot jobs (`teams-hr-sync/main.go:71`, `teams-chat-sync/main.go:104`, `teams-room-creation/main.go:48`), but it is an undocumented deviation from binding project law — CLAUDE.md wins, so it should be written down.
- `low` — no `pkg/obs.Init`, so no OpenTelemetry traces or Prometheus metrics; observability is a raw JSON slog handler — `teams-user-sync/main.go:21`
  Consistent with the other one-shot jobs (`teams-hr-sync/main.go:135` documents the choice), but this service has none of that justifying comment.

**Verified clean:** `Store` is defined in the consumer with exactly the three methods `syncPage` needs (`store.go:21-30`); `NewSyncer` accepts interfaces and returns a struct (`handler.go:24`); config is a typed `caarlos0/env` struct with `required,notEmpty` on both credentials and both URIs and `envDefault` on every operational knob (`config.go:11-47`), with zero `os.Getenv`; `mongoutil.PoolConfig` is mounted as a named field and never re-declared (`config.go:38`); the two Mongo lanes use `envPrefix` correctly (`config.go:32-33`). The service touches no NATS at all, so `BOOTSTRAP_STREAMS`, `pkg/stream`, consumer patterns and INBOX/OUTBOX ownership are genuinely out of scope, not skipped.

### Recommendations
- `medium` — add `func (s *mongoStore) EnsureIndexes(ctx) error` and call it from `run` before the sync, mirroring `teams-chat-sync/main.go:114`.
- `low` — rename `handler.go` → `syncer.go` and `handler_test.go` → `syncer_test.go` to match the peer job.
- `low` — add a one-line comment at `main.go:49` recording why a CronJob binary uses `signal.NotifyContext` instead of `shutdown.Wait`, as `teams-hr-sync/main.go:135` does for `obs.Init`; or amend CLAUDE.md to sanction the one-shot-job pattern.
- `low` — add the same justifying comment for the absent `obs.Init`.

---

---

## 4. Test coverage — 1 / 5

Precomputed statement coverage is **53.4% (118 statements)**, below the 60% critical threshold — floored at 1 by the CLAUDE.md Section 4 rule, even though the business logic itself is fully covered.

### Findings
- `critical` — 53.4% is below both the 80% floor and the 60% critical line — `teams-user-sync/store_mongo.go:43-92`, `teams-user-sync/main.go:34-102`
  The number is dominated by four functions at 0.0% (`newMongoStore`, `ExistingIDs`, `HRUsers`, `UpsertTeamsUsers`) plus `run` at 16.7% and `disconnect` at 0.0%.
- `high` — the entire store layer *is* tested, but only behind `//go:build integration` (`store_integration_test.go:19-99`), and the CI pipeline never builds that tag — `teams-user-sync/deploy/azure-pipelines.yml:44`
  The single test step is `go test ./teams-user-sync/... -race`, with no `-tags=integration` stage and no `make lint`/`make sast` stage even though CLAUDE.md Section 5 calls SAST a blocking CI gate. Only 4 of the repo's 29 service pipelines have any of these. So the 0%-covered store code is verified on no machine but a developer's.
- `low` — `run` is exercised only on its two fail-fast branches (`main_test.go:9-26`); the Mongo-connect, Graph-client-construction and stats-logging paths (`main.go:52-93`) are never executed in any test, tagged or not.

**Quality assessment — the percentage understates this service.** `handler.go` is at **100.0%** across all four functions, and it is meaningful coverage, not vanity: all four error paths are covered as distinct subtests (`handler_test.go:224-268` — Graph abort, `ExistingIDs`, `HRUsers`, `Upsert`), plus the empty-page and empty-tenant boundaries (`handler_test.go:198-219`), the invalid-UPN and HR-miss branches (`handler_test.go:91-136`) and a 7-case table for `splitUPN` (`handler_test.go:271-292`). Structure is compliant throughout: `package main`, table-driven with descriptive subtest names, a fresh `gomock.NewController` and fresh mocks per subtest with no shared state, mocks generated into `mock_store_test.go`, the Graph client injected as an interface (`fakeLister`, `handler_test.go:19`) so no unit test touches a network. Integration tests use `testutil.MongoDB` with a `TestMain` calling `testutil.RunTests(m)` (`store_integration_test.go:17`) and register cleanup on the two inline `httptest` servers (`integration_test.go:56`, `testhelpers_integration_test.go:21`) — no rogue `testcontainers.GenericContainer`.

### Recommendations
- `critical` — add an `-tags=integration` stage to `deploy/azure-pipelines.yml` so the store tests that already exist actually gate merges; that alone moves the measured number over 80% without writing a line of test.
- `high` — add `make lint` and `make sast` stages to the pipeline, per CLAUDE.md Section 5.
- `medium` — cover the specific uncovered path that matters: `store_mongo.go:59-61` and `:76-78` (a Mongo error mid-walk must abort the run, not silently return a partial `existing` map that would cause real users to be skipped as "already present").
- `low` — extract the Mongo/Graph wiring out of `run` behind a small seam so `main.go:52-93` becomes reachable, or accept it as untestable glue and document that.

---

---

## 5. Maintainability — 4 / 5

At 1,115 lines across 12 files with the largest production file at 134 lines, nothing has outgrown its purpose; the comments explain WHY at a genuinely high standard.

### Findings
- `medium` — `syncPage` does four distinct jobs in 73 lines — page accounting, existence diff, UPN parsing/candidate construction, HR join and write — with the HR-merge loop mutating candidates in place — `teams-user-sync/handler.go:51-124`
  It is still readable, but it is the one function where adding a fifth step (e.g. a refresh lane, see D5) means editing a block that already carries every concern.
- `low` — `RunStats` is threaded as a `*RunStats` out-parameter mutated from inside `syncPage` (`handler.go:44,52-53,66`), which is why every counter assertion in the tests has to reason about accumulation across pages. Returning a per-page `RunStats` and summing in `UpdateUsers` would make each page's contribution independently assertable.
- `nitpick` — `handler.go:13-15`'s doc comment says "insert the users missing from teams_user", while the method it documents is named `UpdateUsers` and the store method `UpsertTeamsUsers`. The comment is the accurate one; the names promise more than the code does.

**Verified clean:** no dead code, no duplicated logic, no leaky abstraction — `mongoutil.Collection[T]` keeps projection and cursor handling out of the service entirely (`store_mongo.go:37-49`). Comment discipline is a strength worth naming: `main.go:29-33` explains why the binary exits after one pass (CronJob owns the schedule), `main.go:96-97` explains why `disconnect` builds its own context, `config.go:35-37` explains why `Pool` sits at the top level, and `handler.go:104-105` explains why the log carries the GUID rather than the account. These are all WHY, not WHAT.

### Recommendations
- `medium` — the refactor I would actually do: split `syncPage` into `newCandidates(users, existing, *RunStats) []model.TeamsUser` and `enrichWithHR(ctx, candidates, *RunStats) error`, leaving `syncPage` as diff → build → enrich → write. Roughly 25 lines each, each independently table-testable, and it creates the seam a refresh lane would slot into.
- `low` — have `syncPage` return a `RunStats` for the page and accumulate in `UpdateUsers`, removing the out-parameter.
- `low` — rename `UpdateUsers` → `InsertMissingUsers` (or make the method match the name, per D5).
- `nitpick` — align the `Syncer` doc comment at `handler.go:13` with whichever of the two the team chooses.

---
