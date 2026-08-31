# roomlist-worker — Production Readiness Review

**Service:** `roomlist-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.7 / 5

The strongest service in this audit round, and the only one where no expert found a `critical` and only one found a `high`. It is the worker CLAUDE.md's own architecture notes single out — the one that took the room/subscription writes off `broadcast-worker`'s fan-out path so no MongoDB failure can block delivery — and it honours that contract verifiably: `MaxDeliver=-1`, coalescing with `msgbucket.NewerRow` tie-breaks, bounded batches, back-pressure, a narrow sonic projection, and no read path at all. The tested logic is unusually good: the coalescer, the watermark filters and the settle semantics are each pinned by named regression tests.

What holds it at 3.7 is organisational rather than behavioural. **`main.go` has quietly absorbed five concerns** — the consume loop, the readiness state machine, the self-SIGTERM escalation, the flush-budget validator and the consumer config — four of which already have dedicated `*_test.go` files with **no matching source file**, while `handler.go` contains no handler. And the consumer runs a **third pattern CLAUDE.md does not sanction**: `cons.Messages()` (the high-throughput shape) driven by exactly one goroutine with no semaphore and no `MAX_WORKERS` knob. The reasoning behind that choice is sound and documented; the deviation from binding project law is not.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 1 | 13 | 17 | 11 | **42** |

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go with correct `errcode` tiering, `errors.Is/As` throughout, and slog-only structured logging; deductions are for a handful of literal CLAUDE.md Section-3 violations and one latent nil-error trap.

### Findings
- `low` — Three store methods return a bare `err`, which CLAUDE.md forbids outright ("Never return bare `err`") — `roomlist-worker/store_mongo.go:144`, `:155`, `:166`.
  Mitigated (not cured) by `mongoutil.Collection.BulkWrite` wrapping as `bulk write %s: %w` (`pkg/mongoutil/collection.go:176`) and `flush.write` adding `flush %s: %w` (`roomlist-worker/flush.go:154`), so the chain does carry context — but the rule is stated without exception and the file-header comment at `store_mongo.go:14-17` documents the *reason* for the typed collection, not for skipping the wrap.

- `low` — `consumeState.Check` dereferences a `*error` that could hold a nil error, producing `%!w(<nil>)` instead of a diagnosis — `roomlist-worker/main.go:319-325`.
  `stopped(err)` is currently only reached from `main.go:344` inside an `if err != nil`, so it is latent, not live. The shadowing of `err` for a `*error` (`if err := s.reason.Load(); err != nil`) is what makes the trap easy to reintroduce; storing a non-pointer sentinel or guarding `*err == nil` removes it.

- `low` — `flushOutcome.mongoErrs` puts the driver's raw `BulkWriteException.Error()` text into a log field — `roomlist-worker/flush.go:119`, emitted at `flush.go:204`.
  The rationale at `flush.go:114-119` is sound and the writes here carry only `roomId`/`u.account`/timestamps, never message content, so no body can leak today. It is a standing hazard only if this batch ever grows a content-bearing field; worth a one-line comment pinning that invariant.

- `low` — Audit-coverage gap, not a service defect: per `GLOBAL_PREP.md`, gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean, but `govulncheck` and the semgrep registry packs could not run — `vuln.go.dev` and `semgrep.dev` are 403-blocked by session egress. Dependency-CVE and third-party-pattern coverage for this service is therefore unverified.

- `nitpick` — `NewMongoStore` is exported but returns the unexported `*mongoStore`, inside `package main` where nothing external can consume either — `roomlist-worker/store_mongo.go:23`.
  Contradicts "export only what other packages consume". It matches sibling precedent (`broadcast-worker/store_mongo.go:53`, `upload-service/store_mongo.go:19`), so this is fleet-wide drift rather than a local lapse; `media-service`/`search-service` use the correct `newMongoStore`.

- `nitpick` — Stale suppression: `// #nosec G601` plus `// nosemgrep: gosec.G601-1` guard a loop-variable-alias that cannot occur, and the justification says so ("go.mod requires go 1.25; since 1.22 each iteration has its own loop variable") — `roomlist-worker/store_mongo.go:68-69`.
  A suppression whose own comment explains the rule is inapplicable is dead weight that trains readers to skim `#nosec` lines.

- `nitpick` — 73 top-level test functions against only 10 `t.Run` subtests across the package (`batch_test.go`, `flush_test.go`, `consumeloop_test.go`), where CLAUDE.md Section 4 prefers table-driven form for input/output variations of one behaviour. `batch_test.go:38-183` is eleven near-identical one-assertion functions over `batch.add` that read as one natural table; `handler_test.go:14` and `config_test.go:29` already do it correctly.

### Recommendations
- `low` — Wrap the three `BulkWrite` returns in `store_mongo.go` with this function's own context (`fmt.Errorf("bulk update room last message: %w", err)`), or add an explicit header comment stating the double-wrap is deliberately avoided as redundant. Either satisfies the rule; silence does not.
- `low` — Make `consumeState.reason` hold a non-nil value by construction (store a sentinel when `err == nil`) and rename the loaded variable off `err` so the `*error`/`error` distinction is visible at the call site — `main.go:319-325`.
- `low` — Add a one-line invariant comment beside `flushOutcome.mongoErrs` (`flush.go:119`) recording that the batch carries no message content, so the "never log full message bodies" rule stays checkable if fields are added.
- `nitpick` — Rename `NewMongoStore` → `newMongoStore` (`store_mongo.go:23`); it is a mechanical, zero-risk change within `package main`, and worth raising fleet-wide since four services already have it right.
- `nitpick` — Delete the `#nosec G601` / `nosemgrep` pair at `store_mongo.go:68-69` and confirm `make sast` still reports clean for the file.
- `nitpick` — Collapse `batch_test.go:38-183` into one table-driven `TestBatch_Add` with the existing function names as case names; it preserves every assertion and removes the largest block of structural duplication in the package.
