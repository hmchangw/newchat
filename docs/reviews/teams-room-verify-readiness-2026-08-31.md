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
