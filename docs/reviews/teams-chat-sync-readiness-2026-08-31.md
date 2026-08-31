# teams-chat-sync — Production Readiness Review

**Service:** `teams-chat-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

The best-instrumented of the six `teams-*` CronJobs. It owns the three partial indexes that three downstream jobs scan on and creates them at startup; its `siteId` immutability via `$setOnInsert` is pinned by both a unit and an integration test; and it carries a named regression guard for a real past data-loss bug (the shared-chat claim). Error wrapping, secret hygiene and mock discipline are all clean.

One finding would block a release. **`ListUserChats` follows the server-supplied `@odata.nextLink` with the bearer token attached and no origin pin** — the sibling users pager in the same package *does* pin it, with a comment saying exactly why ("a tampered nextLink must not exfiltrate the token to another host"). Paired with the second defect — **`GRAPH_TLS_INSECURE_SKIP_VERIFY` defaulting to `true`**, which removes the TLS barrier that would otherwise make injection hard — an app-only `Chat.Read.All` token can be sent to an arbitrary host. Two further items are structural: `EnsureIndexes` failure only warns and the run continues, so three downstream jobs can silently degrade to collection scans with no alert; and SIGTERM produces a failure storm rather than a graceful stop.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 8 | 7 | 2 | **20** |

---

## 2. Go code quality — 4 / 5

Idiomatic, consistently context-wrapped, secret-hygiene rule verifiably upheld — but the Graph client is configured insecure-by-default in code while the shipped compose says otherwise.

### Findings
- `high` — `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults to **`true`**, so any deployment that simply omits the var (the k8s CronJob manifest is not in-repo) runs Graph calls with TLS verification disabled — `teams-chat-sync/main.go:51`. The service's own compose sets `:-false` (`teams-chat-sync/deploy/docker-compose.yml:18`) and `main_test.go:32` pins the `true` default, so the two disagree deliberately. Fleet is split: `teams-hr-sync/config.go:28` and `user-presence-service/sync/main.go:43` default `false`; `teams-user-sync/config.go:20` and `teams-chat-member-sync/main.go:44` default `true`. A fail-open default for a credential-bearing client is the wrong direction, and it is the enabling half of the D5 nextLink finding.
- `low` — bare `return err` from `validateConfig` — `teams-chat-sync/main.go:98`. CLAUDE.md §3 says never return bare `err`; the error is already self-describing (`"invalid config: …"`), so this is cosmetic, but it is a literal rule violation.
- `low` — audit-coverage gap, not a service defect: gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (egress 403). No dependency-CVE signal exists for this service.

**Secret-handling rule — verified clean.** `GraphClientSecret` is read at `main.go:43`, passed by value into `msgraph.Config` at `main.go:121`, and never touched again. No log site in the service takes a credential: the only fields logged are `userID`/`chatID`/`error` (`syncer.go:163`, `syncer.go:219`) and counters (`syncer.go:180-183`). The `pkg/msgraph` boundary is likewise disciplined — the token endpoint surfaces only status + OAuth error code, never the form body (`pkg/msgraph/msgraph.go:405-406`), and throttle logs explicitly exclude the token and endpoint (`pkg/msgraph/chats.go:141-151`). `slog` JSON handler set once at `main.go:60`; no `fmt.Println`/`log.Println` anywhere. No `errcode` usage, correctly — this job has no client-facing boundary.

### Recommendations
- `high` — Flip `main.go:51` to `envDefault:"false"` and make the on-prem proxy deployment set it explicitly; align `teams-user-sync` and `teams-chat-member-sync` in the same PR so the fleet has one default.
- `low` — Wrap at `main.go:98`: `return fmt.Errorf("validate config: %w", err)`.
- `low` — Port `teams-chat-member-sync/log_test.go`'s `recordingHandler` pattern here as a regression guard asserting no emitted record contains the client secret or a bearer token.

---

---

## 3. Architecture — 4 / 5

Textbook consumer-defined interfaces, constructor DI, and correct shared-knob mounting; the deviations are all conscious and match the CronJob family, with config-safety the one real gap.

### Findings
- `medium` — `EnsureIndexes` failure only warns and the run continues — `teams-chat-sync/main.go:114-116`. This service *owns* the three partial indexes that `teams-chat-member-sync`, `teams-room-creation` and `teams-room-verify` scan on (`store_mongo.go:44-68`). If creation silently fails on every run, three downstream jobs degrade to collection scans with no alert and no failed run to notice.
- `low` — `pkg/shutdown.Wait` is not used (`main.go:104` uses `signal.NotifyContext`). CLAUDE.md §6 says "every service's `main.go`", so CLAUDE.md wins on the letter — but all five sibling one-shot jobs use `signal.NotifyContext` and none use `shutdown.Wait`, so the rule plainly targets long-running services. Worth an explicit CLAUDE.md carve-out for run-to-completion jobs rather than a code change.
- `nitpick` — File-organization analog is fine (`syncer.go` replaces `handler.go`; no `routes.go`/NATS because there is no server surface), but `worker_test.go` tests `syncer.go` and no `worker.go` exists — the pair is unfindable by name.

**Verified compliant:** `TeamsUserStore`/`TeamsChatStore`/`chatsFetcher` are all defined in the consumer with only the methods used, `<Domain>Store` naming, `//go:generate mockgen` present (`store.go:11-36`); `newSyncer`/`newMongoStore` accept interfaces and return structs; config is a single `caarlos0/env` struct with no `os.Getenv`, `required,notEmpty` on `MONGO_URI` and `SYNC_DEFAULT_SITE_ID`, `envDefault` on every non-critical knob (`main.go:24-57`); `Pool mongoutil.PoolConfig` is mounted as a named field, never re-declared (`main.go:30`). No JetStream/INBOX/OUTBOX/`BOOTSTRAP_STREAMS` surface exists — correctly absent, not missing.

### Recommendations
- `medium` — Make `EnsureIndexes` fatal (`return fmt.Errorf(...)`), or at minimum emit a distinct metric/`slog.Error` so a persistently index-less collection is alertable.
- `low` — Rename `worker_test.go` → `syncer_test_run.go` or fold into `syncer_test.go`.
- `low` — Add the CronJob exemption to CLAUDE.md §6's `shutdown.Wait` rule so six services stop reading as non-compliant.

---

---

## 4. Test coverage — 2 / 5

Unit coverage is **67.6% (136 stmts)** — below the CLAUDE.md §4 80% floor, so the score is floored at 2, though the *quality* of what is tested is well above what that number implies.

### Findings
- `high` — 67.6% statement coverage, below the mandated 80% floor — `coverage_by_service.txt`. Fleet-wide (34/35 services under the floor), not a local regression.
- `medium` — The real untested surface is `run()` — `main.go:91-143`, 0.0% in `covfunc.txt`. Nothing exercises the wiring: the `mongoutil.Connect` failure path (`:107-110`), the `EnsureIndexes`-fails-and-continues branch (`:114-116`), or the `msgraph.NewChatsClient` error path (`:128-130`). The other 0.0% functions (`newMongoStore`, `EnsureIndexes`, `ListUsers`, `SetFrom`, `UpsertChats`) *are* covered — by `integration_test.go:33-291`, which the unit profile cannot see, so ~15 points of the gap is measurement artifact.
- `medium` — Test-fixture drift from production: `newTestSyncer` constructs `syncConfig` with `DefaultSiteID` unset (`worker_test.go:32`), so nine of the eleven `TestRun_*` cases run a configuration that `main.go:39`'s `required,notEmpty` makes unreachable in production. The empty-vote skip branch (`syncer.go:218-221`) is therefore heavily tested while the actual production behaviour — default siteID applied — rests on a single case (`worker_test.go:241-265`).
- `low` — No test covers cancellation: neither `syncer.run` under a cancelled `ctx` nor the SIGTERM path (see D6).

**Verified compliant:** `package main` throughout; table-driven with descriptive subtest names (`main_test.go:77-137`, `syncer_test.go:33-67`, `syncer_test.go:113-142`); mocks generated into `mock_store_test.go` and confirmed non-stale repo-wide; no real DB/NATS in unit tests; genuine error-path coverage (`TestRun_GraphFailureHoldsWatermarkAndFailsRun`, `TestRun_UpsertFailureHoldsWatermark`, `TestRun_SetFromFailureFailsUser`, `TestRun_ListUsersFailure`) plus a named regression guard for the shared-chat-claim data-loss bug (`worker_test.go:119-157`) and a boundary table on `inlineMemberThreshold` (`syncer_test.go:113-142`). Integration side is fully to spec: `//go:build integration`, `TestMain(m) { testutil.RunTests(m) }` at `integration_test.go:21`, `testutil.MongoDB(t, "teamsstore")` everywhere, zero inline `testcontainers.GenericContainer`.

### Recommendations
- `high` — Extract the wiring in `main.go:107-140` behind a seam (e.g. `newDeps(ctx, cfg)` returning the store + fetcher) so the connect/index/client error branches become unit-testable; that alone closes most of the gap.
- `medium` — Set `DefaultSiteID: "site-default"` in `newTestSyncer` (`worker_test.go:32`) and add one explicit `DefaultSiteID: ""` case for the defensive skip, so the default configuration under test matches production.
- `medium` — Add `TestRun_ContextCancelled` asserting the intended SIGTERM semantics once D6's finding is resolved.

---

---

## 5. Maintainability — 4 / 5

Small files, small pure functions, comments that explain WHY — the debt is triplicated prose and a worker-pool skeleton copy-pasted from a sibling.

### Findings
- `medium` — The three-branch upsert rule (oneOnOne / small-inline / large-deferred) is narrated in full **three times**: `store.go:23-28`, `store_mongo.go:95-99`, and `store_mongo.go:112-121`. All three must be edited together; nothing enforces that, so they will drift. Keep the one at `chatUpsertModel` and reduce the other two to a pointer.
- `medium` — The fan-out skeleton — `summary` struct with `atomic.Int64` counters, unbuffered `jobs` channel, `MaxWorkers` goroutines, `close(jobs)` + `wg.Wait()`, "N of M failed" return — is duplicated near-verbatim in `teams-chat-sync/syncer.go:131-189` and `teams-chat-member-sync/syncer.go:113-158`. Two copies is tolerable; a third (this is a family of six jobs) is the point to extract a `pkg/batchrun` worker-pool helper parameterized by job type.
- `low` — `inlineMemberThreshold = 25` (`syncer.go:94`) is a cross-service correctness constant — its own comment says exceeding Graph's inline-expansion cap silently loses members — yet it is a bare local `const` with no link to Graph's documented cap and no counterpart in `teams-chat-member-sync`. Compare `model.TeamsRoomVerifyMaxChatIDs` (`pkg/model/teams.go:110`), which the repo deliberately hoisted into `pkg/model` for exactly this reason.
- `nitpick` — Four `//nolint:gocritic // hugeParam` suppressions on value receivers (`main.go:71`, `syncer.go:103`, `:194`, `:206`) plus one `rangeValCopy` (`store_mongo.go:102`). Each is individually justified; five in a 1725-line service suggests `model.TeamsChat`/`Config` are simply large.

No dead code, no leaky abstractions, no function over ~35 lines, no file over 234 lines.

### Recommendations
- `medium` — Collapse the triplicated upsert narrative to one authoritative comment at `store_mongo.go:112`.
- `medium` — Extract the worker-pool + summary into `pkg/batchrun` (generic over the job type) and adopt it in both chat-sync jobs; this is the refactor I would actually do.
- `low` — Move `inlineMemberThreshold` to `pkg/model` beside `TeamsRoomVerifyMaxChatIDs`, with the Graph cap cited in the doc comment.

---
