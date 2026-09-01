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

---

## 3. Architecture — 3 / 5

Boundaries, DI, bootstrap opt-in and subject/stream builders are all correct, but the dispatcher goroutine is outside the WaitGroup — so shutdown can close Cassandra under an in-flight message — and the service ships without a CI pipeline.

### Findings
- `high` — the consumer loop goroutine is not tracked by `wg`; only the per-message workers are — `main.go:154-167`. A message returned by `iter.Next()` and blocked on `sem <- struct{}{}` (`:160`) is invisible to `wg.Wait()` (`:179-188`), so shutdown can proceed to `nc.Drain` and `cassSess.Close()` (`:189-190`) before that goroutine runs `HandleJetStreamMsg` against a closed session. `message-worker/main.go:281-286` explicitly counts the loop itself and documents exactly this hazard.
- `high` — no `deploy/azure-pipelines.yml`; only `Dockerfile` and `docker-compose.yml` exist (`ls bot-message-worker/deploy/`). CLAUDE.md requires one per service; 29 of 37 service `deploy/` dirs have one.
- `medium` — `store.go` has no `//go:generate mockgen` directive — `store.go:10-16`. ~20 peer services carry it (`room-service`, `message-worker`, `broadcast-worker`, …); its absence is why this service has no `mock_store_test.go`.
- `low` — no `integration_test.go` and therefore no `TestMain`/`testutil.RunTests`, so the entire Cassandra store is unexercised against a real cluster (see D3).
- Compliant and worth noting: `bootstrapStreams` verifies rather than creates when disabled (`bootstrap.go:23-37`), config is a typed `caarlos0/env` struct with `required` on secrets and a mounted `mongoutil.PoolConfig`/`atrest.Config` (`main.go:26-57`), `pkg/subject`/`pkg/stream` builders are used (`main.go:141,208`), `encoding/json` is correct here (this is not one of CLAUDE.md's designated sonic hot-path workers).

### Recommendations
- `high` — `wg.Add(1)` before the dispatcher goroutine with `defer wg.Done()`, matching `message-worker/main.go:281-286`.
- `high` — add `deploy/azure-pipelines.yml` mirroring a peer worker's.
- `medium` — add the `//go:generate mockgen` directive to `store.go` and generate `mock_store_test.go`.
- `low` — start the health server before the consumer loop so readiness cannot report healthy on a half-wired process.

---

## 4. Test coverage — 1 / 5

Coverage is **13.6%** (206 statements) — the lowest in the fleet and far below the 60% critical line, with the entire Cassandra store, `bootstrapStreams` and `run` at 0%.

### Findings
- `critical` — 13.6% statement coverage, well under CLAUDE.md's 80% floor and the 60% critical threshold (`coverage_by_service.txt`). Every function in `store_cassandra.go` is 0.0%: `SaveMessage:64`, `saveEncrypted:100`, `SaveThreadMessage:142`, `saveThreadEncrypted:189`, `countAndSetParentTcount:246`, `toSender:27`, `toMentionSet:36`, `buildCassandraMessage:274` (`covfunc.txt`). That is the entire durability surface — 10 INSERTs, the encryption split, the bucket math and the tcount SET — with zero assertions.
- `high` — zero integration tests: no `integration_test.go`, no `//go:build integration` file, no `TestMain`/`testutil.RunTests`. Peer `message-worker` has `integration_test.go`, `store_cassandra_test.go`, `store_cassandra_batch_test.go` and `store_cassandra_writetime_test.go`; none of those guardrails exist here, which is precisely why the write-timestamp divergence (D5) went unnoticed.
- `high` — `bootstrapStreams` is 0.0% (`bootstrap.go:23`) despite `streamManager` being an injectable interface built for testing — the "verify" branch that must fail fast in production is untested.
- `medium` — the store is mocked with a hand-written `fakeStore` (`handler_test.go:21-48`) rather than a `go.uber.org/mock` mock in `mock_store_test.go`, contrary to CLAUDE.md Section 4.
- `low` — the five handler tests are flat `TestX` functions, not table-driven; the four scenarios (main/thread/malformed/transient/permanent) are near-identical shapes that CLAUDE.md's table-driven rule would collapse.
- Good: tests are `package main`, independent, and snapshot the Prometheus counter as a delta (`handler_test.go:160,169`) so they don't cross-contaminate.

### Recommendations
- `critical` — add `integration_test.go` (`//go:build integration`, `TestMain` → `testutil.RunTests`, `testutil.CassandraKeyspace`) covering: plaintext main-room write, encrypted write, thread reply with and without `TShow`, and `countAndSetParentTcount` idempotency across a simulated redelivery.
- `high` — port `message-worker/store_cassandra_writetime_test.go` to pin the plaintext-pinned / encrypted-unpinned rule here once D5 is fixed.
- `high` — unit-test `bootstrapStreams` both branches with a fake `streamManager`.
- `medium` — replace `fakeStore` with a generated mock and table-drive the handler scenarios.

---

## 5. Maintainability — 3 / 5

Small, readable and well-commented, but `store_cassandra.go` is four near-duplicate hand-written column lists that have already drifted from the `message-worker` original they were copied from.

### Findings
- `medium` — 10 INSERT statements across four methods repeat the same column lists with only encryption/thread variations — `store_cassandra.go:73-92, 114-133, 150-181, 202-236`. Adding one column means editing up to 10 statements; nothing in the package prevents missing one, and no test would catch it (D3).
- `medium` — the handler hand-rolls the permanent/transient split that `jsretry.Settle` already implements — `handler.go:35-51` vs `pkg/jsretry/jsretry.go:93,118-140`. The only reason for the fork is the `permanentErrorTotal` increment, which is itself unreachable (D1).
- `medium` — this service is a fork of `message-worker`'s write path that no longer tracks it: `message-worker` gained `writeTS`/`USING TIMESTAMP` (`message-worker/store_cassandra.go:82-119`) and `stripLegacyPlaintext*` (`:144-146`, `:226,236`); neither propagated here. The duplication is the mechanism of the drift, not a side effect.
- `low` — `metrics.go` holds a single counter that no reachable code path increments (`metrics.go:9`).
- `low` — comments are WHY-shaped and genuinely useful (`main.go:46-49, 104-105`; `store_cassandra.go:99, 244-245`) — comment discipline is not a problem here.

### Recommendations
- `medium` — extract the shared column list/binder into one builder per target table (`messages_by_room`, `messages_by_id`, `thread_messages_by_thread`) taking a plaintext-vs-encrypted variant, collapsing 10 statements to 3.
- `medium` — after making permanent errors reachable, replace the hand-rolled branch with `jsretry.Settle` plus a metric hook, or move the counter into `jsretry`.
- `low` — extract `pkg/`-level shared helpers for `toSender`/`toMentionSet`/`buildCassandraMessage`, which duplicate `message-worker` equivalents.
- `low` — add a README noting this service intentionally mirrors `message-worker`'s write path, so future changes there get mirrored.
