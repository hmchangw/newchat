# teams-room-verify — Production Readiness Review

**Service:** `teams-room-verify` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

A convergence auditor built with real care. The cross-service contract with `teams-room-inspector` is shared through `pkg/model` with the batch cap enforced identically at both ends; the misroute guard is tolerant of older inspectors by design; the query shape matches the partial index `teams-chat-sync` owns and documents for it; and the covered paths are the ones that matter — including the two subtle convergence cases (guest members with empty accounts, duplicate accounts) that would otherwise flag chats forever.

The gaps are operational rather than logical. **The flagged-chat scan is unbounded**: `needVerify` is raised on every room creation, so the flagged set is exactly the backlog that grows when a site's inspector is down — **the failure mode this job exists to detect is also the one that makes it OOM.** **A canceled context does not stop batch dispatch**, so a routine pod eviction emits a warning per remaining batch and a summary indistinguishable from a total federation outage. And `MarkVerified` discards its `BulkWrite` result, so `chats_ok` over-reports every run in which member-sync re-wrote a chat mid-pass — the exact scenario the compare-and-set exists for.

Coverage is 78.9% — **1.1 points** under the floor, entirely `main()` wiring plus a store the integration tests do cover.

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
| Count | 0 | 2 | 10 | 10 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, consistently wrapped errors and disciplined slog usage throughout; deductions are for a missing log-level knob, a doubled error context, and a silently discarded bulk-write result.

### Findings
- `medium` — `BulkWrite`'s result is discarded, so a compare-and-set miss is indistinguishable from a successful clear — `teams-room-verify/store_mongo.go:58`
  `MarkVerified` returns `nil` whether it cleared N chats or 0. The runner has already counted those chats as `ok` (`runner.go:160-161`), so `chats_ok` in the summary (`runner.go:215`) over-reports every run in which member-sync re-wrote a chat mid-pass — the exact scenario the CAS exists for. `mongoutil.BulkResult` carries the matched/modified counts.
- `medium` — no `LOG_LEVEL` config; the JSON handler is built with nil options — `teams-room-verify/main.go:31`
  CLAUDE.md §3 explicitly names log level as config that must have an `envDefault`. Consequence: `restyutil`'s per-request `slog.Debug` (`pkg/restyutil/restyutil.go:75`) is unreachable in production, so there is no way to turn on per-inspector-call tracing during an incident without a rebuild.
- `medium` — no run/correlation ID is generated or put on the context — `teams-room-verify/main.go:71`
  `restyutil` already emits `request_id` when one is on the context (`pkg/restyutil/restyutil.go:97-99`), and CLAUDE.md requires an ID generated at the entry point via `idgen.GenerateRequestID()`. Nothing correlates one CronJob pass's outbound calls, mismatch warnings and summary lines.
- `low` — the same error context is applied twice on the list path — `teams-room-verify/runner.go:67` duplicating `teams-room-verify/store_mongo.go:38`
  Produces `run: list chats needing verify: list chats needing verify: querying teams_chat: …`. CLAUDE.md asks the wrapper to describe what *this* function was doing.
- `low` — `MONGO_PASSWORD`/`MONGO_USERNAME` carry `envDefault:""` — `teams-room-verify/config.go:17-18`
  CLAUDE.md: "never default secrets". House convention across the fleet (`teams-room-creation/config.go:17`), and the empty default is what makes local no-auth Mongo work, so this is a convention-vs-rule conflict worth resolving once fleet-wide rather than here.
- `low` — SAST audit-coverage gap: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress). Environmental, not a service defect.

### Recommendations
- `medium` — Have `MarkVerified` return `*mongoutil.BulkResult` (or the matched count) and log `chats_cas_missed` in the site summary; the `ok` counter currently lies whenever the CAS does its job.
- `medium` — Add `LogLevel string \`env:"LOG_LEVEL" envDefault:"info"\`` to `Config` and build the handler with `&slog.HandlerOptions{Level: …}`.
- `medium` — Generate one run ID with `idgen.GenerateRequestID()` in `run()` and attach it to `ctx` before `r.run(ctx)`, so resty logs and every `WarnContext` share it.
- `low` — Drop the duplicated `"list chats needing verify: "` prefix at `runner.go:67`; the store already supplies it.

---

---

## 3. Architecture — 4 / 5

Textbook consumer-side store interface, constructor DI and a dependency-free runner; the only real gaps are the missing `pkg/obs.Init` wiring and two filename/knob conventions.

### Findings
- `medium` — the service never calls `pkg/obs.Init`, so it emits no OTel traces or Prometheus metrics — `teams-room-verify/main.go:30-36`
  CLAUDE.md §1 requires each service to wire the o11y SDK once via `pkg/obs.Init`; 20 services do. All five `teams-*` jobs skip it, so this is a family-wide gap, not a one-off — but the effect here is that a CronJob whose entire product is an audit signal exports zero metrics, and `mongoutil.Connect` silently takes its uninstrumented path (`pkg/mongoutil/mongo.go:41-42` runs only when `cfg.obs != nil`).
- `low` — no shared-knob mounts: neither `mongoutil.PoolConfig` nor `mongoutil.BreakerConfig` is mounted as a named field — `teams-room-verify/config.go:14-30`
  Both Mongo clients run on library defaults. Acceptable for a low-QPS job, but CLAUDE.md's named-field pattern is how the pool is made operable at all.
- `low` — the integration test file is named `store_mongo_test.go`, not `integration_test.go` — `teams-room-verify/store_mongo_test.go:1`
  Correct build tag and a correct `TestMain` (`:17`), just off the prescribed per-service filename; `runner.go` likewise stands in for the prescribed `handler.go`, which is reasonable for a job with no request handler.
- `nitpick` — the site registry is a JSON document smuggled through one env var — `teams-room-verify/config.go:24`
  In tension with "no config files", but it has an in-repo precedent (`portal-service/main.go:40`), is validated eagerly (`config.go:35-49`), and is the only way to express sites on different domains. Not a defect.

Positives worth recording: `TeamsChatStore` declares exactly the two methods this consumer needs (`store.go:25-31`); `verifyFunc` is injected as a function field (`client.go:17`) so the runner needs no HTTP; `runConfig` (`runner.go:14-19`) keeps the pass pure; `validateConfig` fails fast before any dial (`main.go:60-62`); config is a typed `caarlos0/env` struct with no `os.Getenv`; the Dockerfile matches the mandated `golang:1.25.13-alpine` → `alpine:3.21` pair and runs non-root (`deploy/Dockerfile:1,9-12`). No NATS/JetStream at all, so the stream-bootstrap, subject-builder and consumer-pattern rules are correctly N/A.

### Recommendations
- `medium` — Wire `pkg/obs.Init` in `main()` and pass the observability option through to both `mongoutil.Connect`/`ConnectRead`; do it for the whole `teams-*` family in one PR.
- `low` — Mount `Pool mongoutil.PoolConfig` (and `Breaker`) as named fields rather than relying on driver defaults.
- `low` — Rename `store_mongo_test.go` → `integration_test.go` to match the per-service layout.

---
