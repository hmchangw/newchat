# Production-Readiness Review — `history-service`

| | |
|---|---|
| **Service** | `history-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/production-readiness-history-service-7vbzpz` |
| **Base commit** | `e7e94c9` |
| **Overall score** | **3.4 / 5** |
| **Method** | 6 independent expert audits (code quality, architecture, test coverage, maintainability, integration, performance), each reading `CLAUDE.md` + the full service tree + the `pkg/` packages it imports |

## TL;DR

`history-service` is a well-engineered read/mutate service — 19 NATS request/reply RPCs over Cassandra message history and MongoDB room metadata, ~6.1k lines of production code behind ~15.6k lines of tests. Five of six dimensions score 4-ish: error wrapping, `pkg/errcode` tier discipline, `pkg/subject` builder usage, bucket math, page clamping, graceful shutdown and `docs/client-api.md` sync are all clean, and gosec reports zero findings. Two things hold it back from production-ready. First, **measured unit coverage is 53.9%**, far below the repo's mandatory 80% floor, and nothing in CI enforces the floor — the gap is concentrated in `internal/cassrepo` (32.1%) and `internal/mongorepo` (3.5%), including the at-rest-encryption helper whose failure mode is writing plaintext to an unencrypted column. Second, **canonical mutation events are fire-and-forget**: edits, deletes, pins and reactions commit to Cassandra and then publish to MESSAGES-CANONICAL on the request context with failures swallowed by a `slog.Warn`, so a NATS blip permanently desynchronizes connected clients and the search index while the client sees success. Everything else is bounded, fixable debt: a service layout that invents a third convention the Makefile already special-cases, a missing `bootstrap.go`, three Mongo aggregation shapes that don't scale with room size, and double JSON encoding on every page.

## Dimension scores

| Dimension | Score | One-line verdict |
|---|---|---|
| Go code quality | 4 / 5 | Idiomatic and disciplined; gosec clean. One unprojected Mongo read, one log-and-return, five context-less `slog` calls. |
| Architecture | 3.5 / 5 | Clean DI and subject discipline, but `cmd/`+`internal/` is a third layout, no stream bootstrap, and a non-durable dual write. |
| Test coverage | 1 / 5 | Floored by the 80% rule: 53.9% measured. `internal/service` is excellent at 93%; the two repo packages are 32% and 3.5%. |
| Maintainability | 4 / 5 | No god-files, WHY-comments with issue refs. Canonical-event mapping duplicated 7× with observed drift. |
| Integration | 4 / 5 | Zero raw-`Sprintf` subjects, correct bucket math, client-API docs in sync. Event durability is the gap. |
| Performance | 4 / 5 | Pages clamped, classic N+1s already batched. Double JSON encode, `$facet` + uncapped `$skip`, correlated `$lookup`. |
| **Average** | **3.4 / 5** | |

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 2 |
| `high` | 14 |
| `medium` | 19 |
| `low` | 10 |
| `nitpick` | 8 |
| **Total** | **53** |

Counts are per-dimension as reported and therefore include cross-dimension duplicates — six issues were independently found by two or three experts, which is a signal of their weight rather than double-counting to inflate the total. The repeats are: the missing `bootstrap.go` / `BOOTSTRAP_STREAMS` (architecture, integration, maintainability), the `cmd/`+`internal/` layout (architecture, maintainability), the unprojected `GetSubscription` read (code quality, integration), `$facet` + uncapped `$skip` on thread-parent lists (code quality, performance), the duplicated limit clamp (code quality, maintainability), and the fire-and-forget canonical publish (architecture, integration).

### Synthesis note on the two `critical` tags

The two `critical` findings are not equivalent, and the report is explicit about that rather than letting the tag flatten them:

- **Coverage at 53.9%** is `critical` by the letter of `CLAUDE.md` §4, and the substance backs it: security-relevant code (`blankQuotedBody`, `decryptIfNeeded`) and 371 statements of `cassrepo` have no unit coverage, and no CI gate would catch further drift. Treat this one as genuinely blocking.
- **The `cmd/`+`internal/` layout** was tagged `critical` by the architecture expert as a guidelines violation. It carries no runtime risk — it is a convention and tooling-tax problem (`Makefile:169-180` special-cases this one service; four sibling services hand-copy wire structs they cannot import). It is real and worth fixing, but it is ranked below the availability and correctness items in the prioritized action list at the end of this report.

### Audit gaps

Two of the three blocking SAST scanners could not run in this environment: `govulncheck` was rejected by the agent proxy reaching `vuln.go.dev:443`, and `semgrep`'s registry ruleset (`p/golang`) was rejected reaching `semgrep.dev`. gosec ran clean repo-wide (0 medium+ findings), and the repo's nine **local** `.semgrep` rules ran offline over 33 files with 0 findings — including the `errcode` guardrails. No findings were invented from the two blocked scanners. Integration tests were not executed (Docker unavailable in the audit sandbox), so `cassrepo`/`mongorepo` coverage is measured without the `integration` tag; see the Test coverage chapter for why that measurement gap is itself a finding.
