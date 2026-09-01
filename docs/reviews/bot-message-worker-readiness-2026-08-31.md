# bot-message-worker — Production Readiness Review

**Service:** `bot-message-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.5 / 5

**The lowest-scoring service in the fleet, and the one with the lowest coverage: 13.6%.** The entire Cassandra store — ten INSERTs, the encryption split, the bucket math and the thread-count SET, the whole durability surface of bot messages — is at **0.0%**, with no integration test, no `TestMain`, and no `//go:build integration` file anywhere in the directory.

That absence is not incidental; it is the mechanism by which the service drifted. It is a fork of `message-worker`'s write path that no longer tracks it. `message-worker` gained `writeTS`/`USING TIMESTAMP` pinning and the `stripLegacyPlaintext*` clears; **neither propagated here**, and none of the ten creates pins its write timestamp. CLAUDE.md itself flags this service as unmigrated — and **its stated exposure for it is wrong on both halves**: there *is* a failure point after the Cassandra commit (`countAndSetParentTcount` runs post-`ExecuteBatch` and can NAK a committed create), and the retry window is ~12.6 minutes rather than the ~2.6 the note claims.

Two further structural gaps: the dispatcher goroutine is outside the WaitGroup, so shutdown can close the Cassandra session under an in-flight message; and every bot thread reply triggers an **unbounded partition scan inside the ack window**.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 10 | 8 | 6 | 1 | **26** |
