# Production Readiness Review — `user-service`

| | |
|---|---|
| **Service** | `user-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/user-service-production-readiness-zlyo7o` |
| **Commit** | `e7e94c9` |
| **Overall score** | **3.3 / 5** |
| **Method** | Six independent expert agents, each reading `CLAUDE.md` + the full service + its `pkg/` dependencies |

## TL;DR

`user-service` is a mature, well-engineered Go service whose *code* is among the best in the
repo — `make lint` is clean across the whole repo, `gosec` reports zero findings, every NATS
subject goes through `pkg/subject` builders, every store interface is consumer-defined and
role-scoped, and all 28 client-facing RPCs are documented in `docs/client-api.md` with matching
schemas and `reason` wire values. What holds it back from a shipping grade is not defect density
but three structural pressures. First, **measured unit coverage is 52.6%**, far below the repo's
80% floor — six client-facing `chatlist.*` RPCs and the entire `roomclient`/`presenceclient`/
`publisher` trio have no unit tests at all (see the material Docker caveat in Chapter 4).
Second, the service layer has consolidated into a **god object**: one `UserService` struct with
66 methods across 9 unrelated domains, built by a 15-parameter positional constructor that is
called twice with a silent divergence. Third — and flagged **independently by three of the six
experts** — the cross-site federation path for status/settings/chatlist does **serial, blocking,
lossy JetStream publishes inside the user's request handler**, the exact failure mode the OUTBOX
lane was introduced to eliminate. None of these is a reason to stop a deploy today; all three are
reasons this service should not absorb its next feature before they are addressed.

## Dimension scores

| # | Dimension | Score |
|---|---|---|
| 1 | Go code quality | 4.5 / 5 |
| 2 | Architecture | 4.0 / 5 |
| 3 | Test coverage | 1.0 / 5 |
| 4 | Maintainability | 3.0 / 5 |
| 5 | Integration | 4.0 / 5 |
| 6 | Performance | 3.5 / 5 |
| | **Average** | **3.3 / 5** |

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 8 |
| `medium` | 20 |
| `low` | 15 |
| `nitpick` | 11 |
| **Total** | **55** |

The single `critical` is the coverage floor breach. Of the 8 `high` findings, 3 are performance
hot paths (badge recompute, badge `$lookup` over-fetch, unbounded `subscription.list` scan),
2 are maintainability structure (god object, 15-param constructor), 2 are test gaps (untested
chatlist RPCs, untested `RegisterHandlers`), and 1 is a cross-service config divergence in the
badge cache.

## Convergent findings

Three experts independently reached the same conclusion from different angles, which raises
confidence well above any single agent's judgment:

- **Synchronous cross-site federation** (`service/status.go:101`, `service/settings.go:138`,
  `service/chatlist.go:202`) was flagged by Architecture, Integration, *and* Performance — as a
  durability gap, a correlation gap, and a latency gap respectively.
- **`GetThreadUnreadSummary`'s unbounded fan-out** (`service/threadunread.go:59`) was flagged by
  Code quality, Maintainability, *and* Performance.
- **Missing Mongo projections** on `GetAppsByAssistants` (`mongorepo/apps.go:140`) were flagged
  by Code quality *and* Performance.
