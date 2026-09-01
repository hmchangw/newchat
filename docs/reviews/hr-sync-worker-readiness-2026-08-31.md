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
