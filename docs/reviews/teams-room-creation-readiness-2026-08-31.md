# teams-room-creation — Production Readiness Review

**Service:** `teams-room-creation` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

The cleanest cross-service contract in the `teams-*` family, and it was verified end to end rather than assumed: subject from `pkg/subject` matching the sole binding of `ROOMS-TEAMS-{siteID}`; stream created by its owner `room-worker` and by nothing here; zstd framing round-tripped through `natsutil.DecodePayload`; the wire struct a **legal direct conversion** from the source type, so divergence becomes a compile error; and `Timestamp` stamped at the publish site with `Now` injected. Bounded concurrency, an index-backed precisely-projected read, one bulk write per batch.

The gaps are all about what happens when something goes wrong. **A batch too large for the NATS `max_payload` fails, logs at WARN, and is retried identically forever** — nothing splits, dead-letters or alerts, and with no metrics wired the stall is invisible. **`MarkRoomsCreated` discards its bulk result**, so a CAS that matches nothing is indistinguishable from a clean clear. And coverage at 55.9% is under the critical line, with the zstd publish contract — the actual cross-service wire format — proven only under Docker.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 1 | 12 | 12 | 2 | **28** |

---

## 2. Go code quality — 4 / 5

Idiomatic, tightly-scoped Go with correct `%w` wrapping and JSON `slog` throughout; the only real gap is a missing run-scoped correlation ID that both sibling jobs already have.

### Findings
- `medium` — No request/correlation ID is minted for the run, so every log line and every published message is uncorrelated — `teams-room-creation/main.go:36-48`
  CLAUDE.md §3 "Request Logging & Tracing" requires an ID generated at the entry point and propagated via `context.Context`. `teams-hr-sync/main.go:91` (`natsutil.WithRequestID`) and `teams-user-sync/main.go:74` both do this. Consequence: `natsutil.NewMsg` returns a nil `Header` (acknowledged at `teams-room-creation/publisher.go:33-35`), so room-worker mints a fresh ID and the CronJob run cannot be traced to the rooms it created.
- `low` — `publisher.go:31-37` hand-rolls the nil-header + `Nats-Encoding` guard that `natsutil.NewMsgEncoded` already owns — `pkg/natsutil/request_id.go:76-86`
  The pkg doc explicitly says "callers don't need to know the quirk"; this is the duplicate that drifts.
- `low` — `fmt.Errorf` with no format verb where `errors.New` is correct — `teams-room-creation/config.go:38`, `:41`
- `low` — SAST audit-coverage gap, environmental not a service defect: gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked) — per `GLOBAL_PREP.md`.
- `nitpick` — Log key style is snake (`"site_id"`, `runner.go:79`) while sibling jobs use camel (`"requestId"`, `teams-user-sync/main.go:74`); the repo is genuinely mixed.

Positives verified: no `fmt.Println`/`log.Println`; no bare `err` returns; no string error comparison; no token/body logging; no `errcode` misuse (correctly absent — this service has no client boundary); `//nolint` directives carry reasons.

### Recommendations
- `medium` — Mint `idgen.GenerateRequestID()` in `run()`, stamp it via `natsutil.WithRequestID`, and use it as the base logger, matching `teams-hr-sync/main.go:91-93`.
- `low` — Replace `publisher.go:31-37` with `natsutil.NewMsgEncoded(ctx, subj, natsutil.EncodeZstd(data), natsutil.EncodingZstd)`.
- `low` — `errors.New` for the two verbless `fmt.Errorf` calls in `config.go`.
- `low` — Re-run `make sast-vuln` in a network-enabled CI leg before release; this environment cannot certify dependency CVEs.

---

---

## 3. Architecture — 4 / 5

Textbook service shape — consumer-owned store interface, constructor DI, no stream creation, typed env config — weakened by missing observability wiring and an unbounded shutdown path.

### Findings
- `medium` — No `pkg/obs.Init`, no metrics of any kind; the job emits nothing an operator can alert on (batches published/failed, chats stalled) — `teams-room-creation/main.go:63-66`
  CLAUDE.md §1 requires each service to wire the o11y SDK once via `pkg/obs.Init`; `teams-hr-sync` and `teams-room-inspector` do. The `noop.NewTracerProvider()` shortcut is documented but is still the deviation.
- `medium` — Both Mongo disconnects run on an unbounded `context.Background()`, so an unresponsive node holds the deferred cleanup past the pod's termination grace period — `main.go:55`, `main.go:61`
  `teams-room-verify/main.go:41-51` defines exactly this pattern correctly (`disconnectTimeout = 10s`, fresh non-cancelled context) and documents why; this service took the fresh-context half and dropped the deadline. The NATS drain, by contrast, is bounded correctly at `main.go:73`.
- `low` — `pkg/shutdown.Wait` is not used, contrary to CLAUDE.md §6 "in every service's `main.go`"; `signal.NotifyContext` (`main.go:48`) is the right primitive for a run-to-completion CronJob and matches all sibling `teams-*` jobs, so this reads as a rule that predates the job services rather than a defect.

Verified compliant: `TeamsChatStore` defined in the consumer with exactly two methods (`store.go:26-34`); `newRunner`/`newMongoStore` accept interfaces, return structs; file layout matches the per-service convention (no `routes.go`/`handler.go` — correct, no HTTP or subscribe surface); **no `bootstrap.go` and no stream creation** — ROOMS-TEAMS is created by its owner `room-worker` (`room-worker/bootstrap.go:43-47`), exactly as the `BOOTSTRAP_STREAMS` rule demands; config is a typed `caarlos0/env` struct with `required,notEmpty` on both connection strings and `envDefault` on the knobs (`config.go:14-28`); `Pool mongoutil.PoolConfig` is mounted as a named field, not re-declared (`config.go:19`).

### Recommendations
- `medium` — Wire `pkg/obs.Init` and emit at least three counters: chats listed, batches published, batches failed — a silent CronJob whose batches all fail looks identical to a healthy no-op run.
- `medium` — Copy `teams-room-verify`'s bounded `disconnect(client)` helper verbatim into `main.go`.
- `low` — Add a startup existence check for `ROOMS-TEAMS-{siteID}` per site (mirroring `room-worker/bootstrap.go:57-60`'s fail-fast) so a misprovisioned site surfaces at startup rather than as a warn-per-run forever.

---
