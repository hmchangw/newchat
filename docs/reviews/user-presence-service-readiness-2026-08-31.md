# user-presence-service — Production Readiness Review

**Service:** `user-presence-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Idiomatic, densely WHY-commented Go with correct `errcode` Tier-1 usage, exemplary DI in the core binary, and a genuinely careful hot path — a single-round-trip Lua recompute, deduped pipelined batch reads, a precise Mongo projection via `pkg/userstore`, and clean sweeper termination.

Three findings dominate. **The bulk-presence RPC that `notification-worker`'s push gate calls has no responder anywhere in the repo** — and the two sides also speak different payload shapes and disagree on chunk size by 5×, so the gate cannot be enabled by configuration alone. It is dead-by-flag today; flipping that flag would fail three ways. **The sweep index is a single untagged cluster key** while every per-account key is hash-tagged, so 100% of hello, heartbeat, activity and bye traffic for the entire site funnels into one Valkey master's single-threaded loop — the service's scaling ceiling, invisible until it is hit. And **`Sweep` drains 100 stale accounts per second**, so a gateway restart dropping 50k connections leaves those users shown as online for about eight minutes, long past the 45-second staleness threshold.

Underneath: the `sync/` sub-binary re-declares the shared Valkey and presence knobs and hand-dials the cluster, against a rule the store's own comments state explicitly. Coverage is 45.1%.

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
| Count | 1 | 14 | 21 | 15 | 10 | **61** |
