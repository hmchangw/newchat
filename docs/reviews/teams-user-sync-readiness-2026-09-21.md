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


## 2. Code quality — score 4

### Evidence

- [high] Graph TLS verification is disabled by default, not opt-in — `teams-user-sync/config.go:20` — `GraphTLSInsecureSkipVerify bool \`env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"true"\`` means an operator who sets nothing gets MITM-able Graph traffic carrying the app-only bearer token (`pkg/msgraph/msgraph.go:278` sets `InsecureSkipVerify: true` under a `#nosec G402` whose justification at `pkg/msgraph/msgraph.go:132` explicitly reads "Opt-in, dev/on-prem"). The directly comparable CronJob sibling `teams-hr-sync/config.go:28` defaults it to `false`; `user-presence-service/sync/main.go:44` also defaults false. `config_test.go:30` locks the insecure default in as intended behaviour, so this is a deliberate inversion of secure-by-default rather than an oversight. gosec is clean only because the unsafe call lives in `pkg/`, suppressed there.
- [low] The one error log line of the whole run drops the run's request id — `teams-user-sync/main.go:24` — `run()` builds `logger := slog.With("requestId", idgen.GenerateRequestID())` (`main.go:76`) and every progress line carries it, but the terminal failure is logged by `main` on the *default* logger, so the log that an operator actually greps for is the only one without the correlation id that ties it to the run's other lines.
- [low] Hand-written projections duplicate the decode struct's bson tags — `teams-user-sync/store_mongo.go:75` vs the `hrRow` tags at `store_mongo.go:26-31` — adding a field to `hrRow` without also editing the `bson.M{"account":1,"siteId":1,"engName":1,"mail":1}` literal decodes silently as a zero value. The sibling solves exactly this by deriving the projection from the struct (`teams-hr-sync/store_mongo.go:22-41`, `bsonProjection`). Projection discipline itself is correct — both reads project explicitly.
- [low] No `pkg/obs.Init`; logging is hand-wired — `teams-user-sync/main.go:21` — `slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))` bypasses the SDK CLAUDE.md §1 says each service wires once, so this job emits no traces/metrics and no OTLP log export, and has no level knob (`nil` HandlerOptions pins Info). Context: `teams-hr-sync/main.go:141` carries an explicit "One-shot job: no obs.Init" comment, so there is repo precedent for batch binaries opting out — flagging for the synthesizer's consistency call, not as a lone defect.
- [nitpick] Capacity arithmetic can panic on a misbehaving store — `teams-user-sync/handler.go:68` — `make([]model.TeamsUser, 0, len(users)-len(existing))` panics if `ExistingIDs` ever returns more keys than requested. Unreachable with the real Mongo store (the `$in` filter bounds it), but the slice is only a growth hint and `len(users)` would be free of the assumption.
- [nitpick] One Info line per HR-unmatched user — `teams-user-sync/handler.go:106` — unbounded by directory size on a first run against a large tenant; the aggregate at `handler.go:117` already reports the same fact as counts. Self-limiting on later runs (unmatched users get upserted and then count as existing). The choice to log the Graph GUID rather than the UPN-derived account is correct and commented.

### Recommendations

- [high] Flip `envDefault` to `"false"` on `GRAPH_TLS_INSECURE_SKIP_VERIFY` and set it to `true` explicitly in the on-prem deployment manifest — `teams-user-sync/config.go:20`, plus the assertion at `config_test.go:30` — makes the insecure path an audited opt-in, matches `teams-hr-sync`, and removes a silent token-exposure default.
- [medium] Log the run failure through the request-id logger — `teams-user-sync/main.go:23-26` — have `run()` return the logger (or call `slog.SetDefault(logger)` after `main.go:76`) so the failure line correlates with the run's other output.
- [low] Derive the `hr_employee` projection from `hrRow` instead of a literal — `teams-user-sync/store_mongo.go:73-75` — reuse the `bsonProjection`-style helper already proven in `teams-hr-sync`; kills the rename-drift class of bug.
- [low] Decide obs wiring repo-wide for one-shot jobs — `teams-user-sync/main.go:21` — either wire `pkg/obs.Init` here or add the same explicit "one-shot job: no obs.Init" comment `teams-hr-sync/main.go:141` carries, so the omission reads as a decision rather than a miss.
- [nitpick] Use `len(users)` as the `candidates` capacity — `teams-user-sync/handler.go:68`.

### Reviewer notes

Verified clean on this dimension, so not reported as findings: every error is wrapped with a what-this-function-was-doing prefix (no bare `err`, no `"error: %w"`); no string error comparisons; no `map[string]interface{}`, `os.Getenv`, `fmt.Println`, `time.Sleep` or unterminated goroutines in the service; `model.TeamsUser` carries paired camelCase `json`+`bson` tags with `bson:"_id"` (`pkg/model/teamsuser.go:11-31`); no secret is logged (`GraphProxyPassword` never reaches a log line, `mongoutil` redacts URIs); `Pool mongoutil.PoolConfig` (`config.go:47`) is mounted from the owning package rather than re-declared. `pkg/errcode` tiering is N/A — this binary registers no NATS or HTTP handler, so there is no error boundary to adapt. `go vet ./teams-user-sync/...` exits 0.
SAST per the shared summary: gosec medium+ clean and the 20 repo-owned semgrep rules clean; `govulncheck` and the semgrep registry packs were blocked by sandbox egress (403) and were not retried, so dependency-vulnerability coverage for this service is UNVERIFIED. No SAST finding under `teams-user-sync/` to fold in.

## 3. Architecture — score 4

### Evidence

- [high] The shared GRAPH_* knobs are re-declared per service and the defaults have already diverged — `teams-user-sync/config.go:11-34` — `pkg/msgraph.Config` (`pkg/msgraph/msgraph.go:86`) carries no `env` tags, so five consumers re-declare the same operator-facing vars; `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults to `true` here and in teams-chat-sync/teams-chat-member-sync but `false` in `teams-hr-sync/config.go:28` and `user-presence-service/sync/main.go:44`. This is exactly the failure mode CLAUDE.md §Configuration forbids ("declared once, in the package that owns the thing it configures"), and here the divergent knob is TLS verification against Graph.
- [medium] The directory walk bypasses the throttle gate `pkg/msgraph` already implements — `pkg/msgraph/msgraph.go:832` — `fetchUsersPage` calls `httpClient.Do` directly and turns any non-200 into `graph returned status %d` (`msgraph.go:841`), while every other paged walk (`chats.go:103`, `groups.go:135`, `members.go:56`) goes through `getThrottled` (`chats.go:127`) with Retry-After backoff. A single 429 mid-walk therefore aborts the whole run (`handler.go:46`); the next CronJob fire restarts from page 1 into the same throttle.
- [medium] Reconciliation is insert-only, so HR fields on existing rows are frozen forever — `teams-user-sync/handler.go:71-73` — users already in `teams_user` are skipped before the HR join, and no other service writes `siteId`/`engName`/`mail`/`displayName` on that collection (only `teams-chat-sync` writes `from`). A user synced while `HRUnmatched` (`handler.go:103`) keeps an empty `siteId` permanently, and `teams-chat-sync` reads that field to stamp migrated chats (`teams-chat-sync/store_mongo.go:79,146`), falling back to `DefaultSiteID`. There is no backfill path in the repo.
- [low] `HRUsers` depends on an index owned by another service — `teams-user-sync/store_mongo.go:73-75` — the `$in` on `hr_employee.account` (up to `GRAPH_PAGE_SIZE`=500 terms per page) is only index-backed because `portal-service/store_mongo.go:29-33` ensures it; `newMongoStore` (`store_mongo.go:43`) has no `EnsureIndexes`, so a site deployed without portal-service COLLSCANs the HR collection once per page.
- [low] No `obs.Init` and no `pkg/shutdown.Wait` — `teams-user-sync/main.go:21,49` — the job logs JSON via a bare handler and relies on `signal.NotifyContext`. This matches the one-shot cron family (teams-chat-sync, teams-chat-member-sync, teams-room-creation, teams-room-verify) and is defensible, but unlike `teams-hr-sync/main.go:141` the deviation from CLAUDE.md §Observability/§Graceful Shutdown is undocumented here, and the run emits no traces/metrics — the only failure signal is the exit code.
- [nitpick] The batch runner lives in `handler.go` and is exported from `package main` — `teams-user-sync/handler.go:16,24,29` — peers keep the equivalent type in `syncer.go` and unexported (`teams-chat-sync/syncer.go:43`). `Store` being exported matches the repo-wide norm (11 services), so only `Syncer`/`NewSyncer`/`RunStats` stand out.

Otherwise the structure is sound: the `Store` interface is consumer-owned with exactly three methods (`store.go:21-30`), deps are injected by constructor (`handler.go:24`, `main.go:75-77`), read/write Mongo lanes are separated (`main.go:52-61`), `Pool mongoutil.PoolConfig` is mounted from the owning package (`config.go:47`), and every query projects explicitly (`store_mongo.go:58,75`). The service has no NATS surface at all, so this dimension's subject/stream/consumer/outbox/jsretry rules do not apply.

### Recommendations

- [high] Give `pkg/msgraph.Config` the `env` tags and have each consumer mount it as a named field with `envPrefix`, converging on `GRAPH_TLS_INSECURE_SKIP_VERIFY=false` with an explicit per-deployment override — `teams-user-sync/config.go:11-34` — removes the split-brain TLS default across the Graph consumers.
- [medium] Route `ListUsers` through `getThrottled` like the other walks — `pkg/msgraph/msgraph.go:825` — turns a 429 on page N from a whole-run abort into a Retry-After wait, and arms the tenant-wide gate for peers.
- [medium] Add a refresh lane for rows that already exist — `teams-user-sync/handler.go:71` — at minimum re-resolve HR fields for rows whose `siteId` is empty, so a user synced before their `hr_employee` row appeared is eventually repaired instead of being pinned to the default site.
- [low] Either add `EnsureIndexes` for `hr_employee.account` at startup or record the portal-service dependency in `store_mongo.go` — `teams-user-sync/store_mongo.go:43` — a per-page COLLSCAN over the HR collection is a silent cliff on a large tenant.
- [low] Document the one-shot no-`obs.Init`/no-`shutdown.Wait` choice as `teams-hr-sync` does, and consider emitting `RunStats` as metrics — `teams-user-sync/main.go:21,82` — today the run summary exists only as one log line.
- [nitpick] Rename `handler.go` to `syncer.go` and unexport `Syncer`/`NewSyncer`/`RunStats` — `teams-user-sync/handler.go:16` — aligns with the sibling sync jobs and CLAUDE.md's "export only what other packages consume".

## 4. Test coverage — score 1

### Evidence

- [critical] coverage below repo minimum 80%, currently 53.4% — `teams-user-sync/main.go:34` — 63/118 statements in the provided profile. Per-file split I derived from `cov.out`: `handler.go` 58/58, `main.go` 5/37, `store_mongo.go` 0/23. Under 60%, so CLAUDE.md §4 floors this dimension at 1.
- [high] every store method and the store constructor sit at 0% in the unit profile — `teams-user-sync/store_mongo.go:43`, `:51`, `:68`, `:85` — they are exercised only by `//go:build integration` tests (`store_integration_test.go:19-99`), so nothing in a default `go test` run touches the Mongo adapter.
- [high] the service pipeline never runs the integration suite and never gates on coverage — `teams-user-sync/deploy/azure-pipelines.yml:44` — the only test step is an untagged `go test ./teams-user-sync/...`; the emitted `coverage-teams-user-sync.out` is written and then discarded, although the repo already owns a threshold checker (`Makefile:155`, `tools/coveragecheck`). CI-verified coverage is therefore exactly the 53.4% above.
- [medium] `run()` is 16.7% covered and is the whole binary — `teams-user-sync/main.go:34` — only the parse-config and page-size fail-fast branches are hit. The `cfg.Pool.Validate()` branch (`main.go:43`) is 0% even though `config_test.go:61` already proves the validator rejects a zero pool; the Mongo connects, the Graph lister construction, the syncer wiring and the end-of-run stats log line (`main.go:52-95`) are untested.
- [medium] no test can detect a read/write lane swap — `teams-user-sync/integration_test.go:63` and `store_integration_test.go:22` both construct `newMongoStore(db, db)` with one database standing in for both lanes. The only place the two lanes differ is `main.go:75`, which is at 0%, so transposing `readDB`/`writeDB` in `store_mongo.go:43-49` (sending upserts through the secondary-preferred read client) would ship with a green suite.
- [medium] the documented partial-progress contract is unasserted — `teams-user-sync/handler_test.go:230`, `:241`, `:253`, `:267` — all four error subtests discard stats with `_`, although `UpdateUsers` deliberately returns the partial `stats` alongside the error (`handler.go:46`) and `main.go:82` logs them as the operator's only per-run signal. A regression that zeroed stats on failure would pass.
- [low] `disconnect` is 0% — `teams-user-sync/main.go:100` — the 10s bounded-cleanup path that exists precisely because the run ctx may already be canceled has no test, and there is no test for a canceled context mid-walk (`main.go:49` sets up SIGTERM cancellation that nothing asserts).
- [low] `GRAPH_PAGE_SIZE` boundaries are half-tested — `teams-user-sync/main_test.go:18` covers only 1000; the `<= 0` half of the condition at `main.go:40` and the accepted boundaries 1 and 999 have no case, so an off-by-one in the guard is invisible.
- [low] the package's only `TestMain` lives inside a store test file — `teams-user-sync/store_integration_test.go:17` — deleting or renaming that file silently drops `testutil.RunTests`, and with Ryuk disabled repo-wide that means leaked containers rather than a visible failure.
- [nitpick] `unsetProxyEnv` mutates process env with raw `os.Unsetenv`/`os.Setenv` — `teams-user-sync/config_test.go:121-128` — it restores correctly via `t.Cleanup`, but it bypasses `t.Setenv`'s parallel guard, so the package silently becomes order-sensitive the day any test calls `t.Parallel()`.

### Recommendations

- [high] Add an integration step (`go test -tags integration -race ./teams-user-sync/...`) to `teams-user-sync/deploy/azure-pipelines.yml:44` and feed both profiles to `tools/coveragecheck -min 80`. Without it the store adapter and the end-to-end test are dead weight, and the 53.4% is the real number, not an artifact.
- [high] Cover `run()`'s remaining fail-fast branch cheaply: a `main_test.go` subtest setting `MONGO_MAX_POOL_SIZE=0` reaches `main.go:43` with no I/O (`mongoutil.Connect` pings eagerly, so it aborts before any dial). Then extract the post-config wiring into a small `newRunDeps(cfg)` so `main.go:52-77` becomes assertable, and give the two Mongo lanes distinct databases in `integration_test.go:63` so a lane swap fails a test.
- [medium] Assert the returned `RunStats` in the four error subtests at `handler_test.go:221-268` (page-1 counters must survive a page-2 failure) — it is a one-line change per subtest and pins the idempotent-rerun contract that `handler.go:38-40` documents.
- [medium] Add a `UpdateUsers` context-cancellation case and a `disconnect` test (`main.go:100`) driven by an already-canceled parent ctx, so the SIGTERM abort path the CronJob relies on is exercised.
- [low] Extend `TestSplitUPN`-style table coverage to the page-size guard (0, 1, 999, 1000) at `main_test.go:18`, and move `TestMain` into a dedicated `main_integration_test.go` so cleanup cannot be deleted with a store file.

## 5. Maintainability — score 4

### Evidence

- [medium] `splitUPN` is duplicated verbatim (body + doc comment) in two services that must agree — `teams-user-sync/handler.go:128` and `teams-hr-sync/transform/transform.go:87` — both derive the lowercased UPN local part that becomes `teams_user.account` / the `hr_employee.account` join key. Two independent copies of the key-derivation rule can drift silently and de-join the two collections; it belongs in `pkg/` (next to `msgraph.GraphUser`).
- [low] The HR field list is restated in four unlinked places — `pkg/model/teamsuser.go:21-28`, `store.go:12-16` (`hrUser`), `store_mongo.go:26-31` (`hrRow`), `store_mongo.go:75` (the projection). Adding one HR field means editing all four; forgetting only the projection compiles, passes the unit tests (mocked `Store`), and silently writes an empty value — indistinguishable from the legitimate "no hr row" case at `handler.go:103`.
- [low] The `*BulkResult` from the write is discarded — `store_mongo.go:86` (`if _, err := s.writeTeamsUsers.BulkUpsertByID(...)`). The end-of-run log line is this job's entire output, and its `upserted` counter (`handler.go:122`) counts candidates *submitted*, not Mongo's matched/upserted counts, which the helper already returns.
- [low] One Info line per HR-unmatched user — `handler.go:106`. On the first run against an empty `teams_user` (or any HR outage) every user is unmatched, so a directory-scale tenant emits one log record per user, while the aggregate at `handler.go:117` already carries `requested/matched/unmatched`. The per-user detail is unbounded by anything the operator controls.
- [low] `syncPage` is the whole service in one 74-line function with four phases (id diff, candidate build, HR join, write) plus pointer-mutated accounting — `handler.go:51-124`. Under the ~80-line bar, but each phase is only reachable through a full `UpdateUsers` run with three mock expectations (see `handler_test.go:221-268`), which is why the error-path tests all assert on substrings rather than behaviour.
- [low] `disconnect` is copied into three services — `main.go:100`, `teams-hr-sync/main.go:252`, `teams-room-verify/main.go:47` — identical 10s-timeout-on-`context.Background()` shutdown helper; a `mongoutil` one-liner.
- [nitpick] `handler.go` contains no handler — it holds the batch `Syncer`, while the sibling batch job names its parts by role (`teams-hr-sync/differ.go`, `emitter.go`, `publisher.go`). The layout convention in CLAUDE.md is written for request/message services.
- [nitpick] Log level is hardcoded — `main.go:21` passes `nil` handler options, so raising verbosity on a failing CronJob run needs a rebuild. Also, unlike `teams-hr-sync/main.go:141`, the deliberate "one-shot job, no `obs.Init`" decision is not recorded anywhere in this service, so the next reader cannot tell it from an omission.
- [nitpick] The only prose about this service is stale — `docs/superpowers/plans/2026-07-13-teams-user-sync.md:21` describes `robfig/cron` with `cron.SkipIfStillRunning` and an `hr` collection; the shipped binary is one-shot (K8s CronJob owns the schedule, `main.go:29-33`) and reads `hr_employee` (`store_mongo.go:16`).

### Recommendations

- [medium] Move `splitUPN` into a shared package (e.g. `pkg/msgraph`) and have both `teams-user-sync/handler.go:128` and `teams-hr-sync/transform/transform.go:87` call it — one definition of the account-join key, one test table.
- [low] Collapse `hrUser` (`store.go:12`) and `hrRow` (`store_mongo.go:26`) into one type and derive the projection from its bson tags (or add a store integration assertion that every `hrUser` field round-trips) so a new HR field cannot be half-added.
- [low] Use the returned `*BulkResult` at `store_mongo.go:86` to report real upserted/matched counts in the run summary — the job is unobservable apart from that line.
- [low] Demote the per-user `hr id not found` log at `handler.go:106` to Debug (or cap it), keeping the aggregate at `handler.go:117` — a cold-start run currently logs the whole directory.
- [low] Extract the four phases of `syncPage` (`handler.go:51-124`) into small functions returning candidates/stat deltas, so the HR-join and candidate-build rules are testable without mocking three store calls.
- [nitpick] Hoist `disconnect` (`main.go:100`) into `pkg/mongoutil` and drop the three copies.
- [nitpick] Add a `LOG_LEVEL` knob at `main.go:21` and a one-line comment recording the no-`obs.Init` choice, mirroring `teams-hr-sync/main.go:141`.
