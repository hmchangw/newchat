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
