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

---

## 2. Go code quality — 3 / 5

Idiomatic, well-commented Go with correct `%w` wrapping and slog-only structured logging, undercut by log lines that carry no correlation ID and a permanent-error branch that can never fire in production.

### Findings
- `high` — the `isPermanent` / poison-drop branch is dead code in production: nothing in this service or its dependencies ever returns `errcode.Permanent` (`store_cassandra.go` has no `errcode` import; `grep errcode.Permanent pkg/atrest pkg/threadcount` → 0 hits), so `permanentErrorTotal` only increments in the unit test — `bot-message-worker/handler.go:36-42`, `metrics.go:9`. A genuinely poisonous event NAKs to `MaxDeliver` and vanishes with no poison signal.
- `high` — no event validation after unmarshal: a `{}` payload decodes cleanly and is written straight through with empty `ID`/`RoomID` — `handler.go:27-35`. Contrast `message-worker/handler.go:96-98`, which returns `errcode.Permanent(errcode.BadRequest("malformed message event"))`.
- `medium` — no log line includes `request_id`, violating CLAUDE.md "propagate via `context.Context`, include in all log lines" — `handler.go:29-31, 38-39, 43-44, 49-50`. Peers do (`message-worker/handler.go:113` uses `natsutil.RequestIDFromContext(ctx)`), and `obs.ContextWithIdentity` is never called either.
- `low` — SAST audit coverage gap: gosec + repo-owned semgrep are clean repo-wide; `govulncheck` and semgrep registry packs could not run (blocked egress). Environmental, not a service defect.
- `nitpick` — `write`'s local `threadRoomID := m.RoomID` exists only to name the argument — `handler.go:60`.

### Recommendations
- `high` — return `errcode.Permanent(errcode.BadRequest(...))` for a decoded event missing `ID`/`RoomID`/`CreatedAt`, so the poison path and its metric become reachable.
- `high` — reject non-retryable gocql errors (e.g. `gocql.ErrNotFound`, invalid-query/`ErrorMap` server errors) as permanent rather than NAKing them 6× over 12.6 minutes.
- `medium` — add `"request_id", natsutil.RequestIDFromContext(ctx)` to all four log sites and `obs.ContextWithIdentity` after decode.
- `low` — record the govulncheck gap in the release checklist and run it from an unblocked CI leg.
