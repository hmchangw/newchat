# Production-Readiness Review — `room-service`

| | |
|---|---|
| **Service** | `room-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/new-session-439bi9` |
| **Overall score** | **2.83 / 5** |
| **Method** | Six independent expert audits (code quality + SAST, architecture, test coverage, maintainability, integration, performance) against `CLAUDE.md` and general Go-microservice practice |

## Executive summary

`room-service` is a well-built service carrying a large and mostly undisclosed amount of structural debt. The things that are easy to get wrong in this codebase are right: every one of its 27 NATS registrations and 12 publishes goes through `pkg/subject` (zero raw `fmt.Sprintf` subjects), stream configs come from `pkg/stream`, `bootstrapStreams` respects the `BOOTSTRAP_STREAMS` opt-in and sets only `Name + Subjects`, all cross-site federation goes through `outbox.Publish` to the local OUTBOX rather than a direct remote INBOX publish, every federated event type is registered in exactly one `pkg/outbox` partition set, every published event carries a `Timestamp` set at the publish site, config is a typed `caarlos0/env` struct with no `os.Getenv` anywhere, shutdown uses `pkg/shutdown.Wait` with the documented ordering, and `gosec` is clean. Handler-level error-path testing is genuinely strong and mocks are fresh.

What holds it back is scale and verification. The service is 6,314 production LOC in nine flat files, with a 2,643-line `handler.go` spanning eight unrelated domains and a 47-method `RoomStore` god interface — both peers of comparable size (`user-service`, `history-service`) already moved to the sanctioned sub-package layout and keep no file over 993 lines. Unit coverage is **57.7%**, below the repo's 60% critical threshold, because the 2,046-line `store_mongo.go` sits at 2.6% and is verified only behind the `integration` build tag that `make test` does not run; no CI job enforces the documented 80% floor. On the runtime side the store leaks `mongo.ErrNoDocuments`/`IsDuplicateKeyError` into handlers, a missing room returns 500 from four RPCs and 404 from two others, several hot reads fetch whole documents against the "always project precisely" rule, mention autocomplete joins an entire room before `$limit`, and a per-bot `GetApp` loop N+1s without a request-size cap. One correctness bug stands out: rebalanced `section_moved` federation events all share a `Nats-Msg-Id` and are silently deduplicated by JetStream, leaving cross-site users with stale `sectionOrder`.

None of this blocks the service from running — it runs, and its tests pass. It blocks confident change. Close the coverage gap and enforce it, fix the dedup-ID bug and the not-found/500 inconsistency, then decompose.

## Dimension scores

| # | Dimension | Score |
|---|-----------|:-----:|
| 1 | Go code quality (incl. SAST) | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | **1 / 5** |
| 4 | Maintainability | **2 / 5** |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |
| | **Average** | **2.83 / 5** |

## Findings by severity

| Severity | Count |
|----------|:-----:|
| `critical` | 1 |
| `high` | 16 |
| `medium` | 30 |
| `low` | 19 |
| `nitpick` | 14 |
| **Total** | **80** |

The single `critical` is unit coverage at 57.7%, below the repo minimum of 80% and below the 60% threshold at which CLAUDE.md Section 4 floors the dimension at 1.
