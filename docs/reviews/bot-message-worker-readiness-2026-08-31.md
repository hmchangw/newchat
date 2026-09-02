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

---

## 6. Integration — 2 / 5

Subject, stream and consumer wiring are exactly right, but the Cassandra write-timestamp contract in CLAUDE.md is unimplemented across all ten creates — and the documented rationale for tolerating that is factually wrong.

### Findings
- `high` — **confirmed: none of the 10 create INSERTs pin `USING TIMESTAMP`** (`grep "USING TIMESTAMP" bot-message-worker/` → 0 hits) — `store_cassandra.go:74,84,115,125,151,161,173,203,215,227`. CLAUDE.md requires every *plaintext* create to bind `writeTS`; `message-worker/store_cassandra.go:173,184,265,276,297` does. The five plaintext creates (`:74,84,151,161,173`) are in violation; the encrypted ones (`:115,125,203,215,227`) are correct to remain unpinned per the same rule, but by accident rather than intent.
- `high` — **CLAUDE.md's stated exposure for this service is wrong on both halves.** (a) "no failure point after the Cassandra commit in its handler" is false for the thread path: `countAndSetParentTcount` runs *after* `ExecuteBatch` commits and can fail on the partition scan or either UPDATE, returning a transient error that NAKs and replays the already-committed create — `store_cassandra.go:186,241,246-270`. (b) "repo-default `MaxDeliver=6` (~2.6 min)" understates the window: the handler NAKs with `jsretry.DefaultBackoff` (`handler.go:45`), whose schedule is `{1s,5s,30s,2m,10m}` = **12.6 minutes** across `MaxDeliver=6` (`pkg/jsretry/jsretry.go:52-59`; `pkg/stream/consumer.go:20`). The edit-reverting race window is ~5× larger than documented.
- `medium` — the encrypted creates bind literal `null` for `msg/attachments/card/card_action` inside the INSERT — `store_cassandra.go:120,130,209,220,232` — the exact form CLAUDE.md forbids ("never as NULLs bound into it"). It is harmless *only* because these statements are unpinned; pinning them later (the D5 fix above) would silently stop the legacy-plaintext clear from landing. `message-worker` keeps them as separate unpinned `stripLegacyPlaintext*` UPDATEs (`message-worker/store_cassandra.go:144-146, 226, 236`) precisely to keep the requirement local.
- Compliant: consumes only, publishes nothing (`grep Publish` → 0 hits), registers no `chat.user.…` handler — so no `docs/client-api.md` obligation; `subject.BotCanonicalCreated` and `stream.BotMessagesCanonical` used via builders (`main.go:141,208`); bucket math via `msgbucket.New(MESSAGE_BUCKET_HOURS)` (`main.go:102`) with the fleet default 360 (`deploy/docker-compose.yml`); `inbox-worker`/`outbox-worker` ownership untouched.

### Recommendations
- `high` — pin the five plaintext create INSERTs with `USING TIMESTAMP writeTS(msg.CreatedAt)`, porting `message-worker/store_cassandra.go:82-119` verbatim, and leave the encrypted creates unpinned with the CLAUDE.md rationale as a comment.
- `high` — make `countAndSetParentTcount` non-fatal for the commit (Ack the create and retry the tcount SET separately), or move it ahead of the commit, so a post-commit failure cannot replay the create.
- `high` — correct the CLAUDE.md bot-message-worker paragraph: the retry window is 12.6 min via `jsretry.DefaultBackoff`, and the thread path *does* have a post-commit failure point.
- `medium` — convert the inline encrypted `null` bindings into separate unpinned `stripLegacyPlaintext*` UPDATE statements before pinning anything.

---

## 7. Performance — 3 / 5

Retry discipline, batching and worker-pool sizing are correct, but every bot thread reply triggers a full unbounded partition scan inside the ack window.

### Findings
- `high` — each thread reply runs `threadcount.Count`, a full scan of `thread_messages_by_thread` for that thread with no LIMIT — `store_cassandra.go:250`, `pkg/threadcount/threadcount.go:41-44`. Cost grows linearly with thread length, runs synchronously in the handler, and with `MAX_WORKERS=100` (`main.go:39,152`) up to 100 such scans run concurrently against Cassandra.
- `high` — the scan's own 15s backstop (`pkg/threadcount` `scanTimeout`) plus the batch write can approach the 30s `AckWait` default (`pkg/stream/consumer.go:19`), so a long thread risks un-acked redelivery — which then replays an unpinned create (see D5).
- `medium` — the unbounded dispatcher goroutine has no termination guarantee tied to shutdown (`main.go:154-167`); after `iter.Stop()` a goroutine parked on `sem <-` is neither drained nor counted (see D2).
- `low` — `bucket.Of(msg.CreatedAt)` is recomputed inline per statement in the thread paths (`store_cassandra.go:178,233`) rather than hoisted as in `SaveMessage` (`:70`); trivial, but inconsistent.
- Compliant: no bare `Nak()`/`NakWithDelay(0)` anywhere — the only settle is `jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, …)` (`handler.go:45`); `cc.BackOff` is never hardcoded, it comes from `stream.DurableConsumerDefaults` (`main.go:206`); writes are `UnloggedBatch` so the denormalized pair shares one round trip (`store_cassandra.go:72,113,149,201`); the sole Mongo use is the at-rest DEK collection behind `ATREST_ENABLED` with a justified `primaryPreferred` read preference (`main.go:46-49,130-131`) — no unprojected finds, no `$lookup`.

### Recommendations
- `high` — bound or cache the thread count: maintain `tcount` as a counter/derived value, or cap the scan and fall back, so per-reply cost is O(1) rather than O(thread length).
- `high` — raise `CONSUMER_ACK_WAIT` above the worst-case scan+write, or call `msg.InProgress()` while the scan runs, so a long thread cannot trigger redelivery of a committed create.
- `medium` — track the dispatcher goroutine in the WaitGroup so shutdown cannot close the Cassandra session under it.
- `low` — hoist `s.bucket.Of(msg.CreatedAt)` to a local in both thread paths.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `critical` | **Add integration tests for the Cassandra store** | Test coverage | 13.6% overall; **every** function in `store_cassandra.go` at 0.0% — `SaveMessage:64`, `saveEncrypted:100`, `SaveThreadMessage:142`, `saveThreadEncrypted:189`, `countAndSetParentTcount:246`, `toSender:27`, `toMentionSet:36`, `buildCassandraMessage:274` | The whole durability surface of bot messages — 10 INSERTs, the encryption split, the bucket math, the tcount SET — with **zero assertions**. `message-worker` has `integration_test.go`, `store_cassandra_test.go`, `store_cassandra_batch_test.go` and `store_cassandra_writetime_test.go`; **the absence of that last one is precisely why the write-timestamp divergence went unnoticed.** |
| 2 | `high` | **Pin `USING TIMESTAMP` on the five plaintext creates** | Integration | 0 hits for `USING TIMESTAMP` in the service; plaintext creates at `store_cassandra.go:74`, `:84`, `:151`, `:161`, `:173`; correct form at `message-worker/store_cassandra.go:173`, `:184`, `:265`, `:276`, `:297` | CLAUDE.md requires every plaintext create to bind `writeTS`. Unpinned, a create that commits then NAKs on a later step **outranks an edit made in between and silently restores the original body.** The five encrypted creates are correct to remain unpinned under the same rule — **but by accident, not by intent**, so pin the four and comment the five. |
| 3 | `high` | **Correct CLAUDE.md's stated exposure for this service** | Integration | (a) `countAndSetParentTcount` runs *after* `ExecuteBatch` commits and can fail on the partition scan or either UPDATE — `store_cassandra.go:186`, `:241`, `:246-270`; (b) the handler NAKs on `jsretry.DefaultBackoff`, giving ~12.6 min, not ~2.6 | CLAUDE.md says this service has "no failure point after the Cassandra commit in its handler" and a "~2.6 min" window. **Both halves are wrong**, and they are the stated justification for leaving item 2 undone. The document is binding project law; a wrong risk assessment in it propagates to every future reader. |
| 4 | `high` | **Count the consumer loop goroutine in the WaitGroup** | Architecture | `main.go:154-167`; `wg.Wait()` at `:179-188`; `nc.Drain`/`cassSess.Close()` at `:189-190`; `message-worker/main.go:281-286` **counts the loop and documents this hazard** | A message returned by `iter.Next()` and blocked on `sem <- struct{}{}` is **invisible to `wg.Wait()`**, so shutdown can close the Cassandra session before that goroutine runs the handler against it. Every rolling restart is a chance to lose a bot message. |
| 5 | `high` | **Bound the thread-count scan** | Performance | `threadcount.Count` at `store_cassandra.go:250`; no LIMIT at `pkg/threadcount/threadcount.go:41-44`; `MAX_WORKERS=100` at `main.go:39`, `:152` | **Every bot thread reply runs a full partition scan** whose cost grows linearly with thread length, synchronously in the handler, up to 100 concurrently. Its own 15s backstop plus the batch write can approach the 30s `AckWait`, so a long thread risks un-acked redelivery — **which then replays an unpinned create** (item 2). The three findings are one failure chain. |
| 6 | `high` | **Validate the event after unmarshal** | Code quality | `handler.go:27-35`; the correct form at `message-worker/handler.go:96-98` | A `{}` payload decodes cleanly and is **written straight through with empty `ID`/`RoomID`.** The peer returns `errcode.Permanent(errcode.BadRequest("malformed message event"))`; this service persists it. |
| 7 | `high` | **Make the permanent-error branch reachable, or delete it** | Code quality / Maint | `handler.go:36-42`, `metrics.go:9`; nothing in the service or its deps returns `errcode.Permanent` (no `errcode` import in `store_cassandra.go`; 0 hits in `pkg/atrest`, `pkg/threadcount`) | `permanentErrorTotal` **only ever increments in a unit test.** A genuinely poisonous event NAKs to `MaxDeliver` and vanishes **with no poison signal.** Better still: delete the hand-rolled split and use `jsretry.Settle` (item 9), then reintroduce the metric where it can fire. |
| 8 | `high` | **Add `deploy/azure-pipelines.yml`** | Architecture | `bot-message-worker/deploy/` has only `Dockerfile` and `docker-compose.yml`; 29 of 37 services have the pipeline | No CI gate exists for the service with the fleet's lowest coverage and an unimplemented durability contract. |
| 9 | `medium` | **Replace the hand-rolled settle tree with `jsretry.Settle`; add the `//go:generate mockgen` directive** | Maintainability / Tests | `handler.go:35-51` vs `pkg/jsretry/jsretry.go:93`, `:118-140`; no directive at `store.go:10-16`; hand-written `fakeStore` at `handler_test.go:21-48` | The only reason for the retry fork is the metric from item 7, which cannot fire. And the missing directive is **why this service has no `mock_store_test.go`** — ~20 peers carry it. |
| 10 | `medium` | **Deduplicate the ten INSERTs, and fix the bound-`null` form** | Maintainability / Integration | column lists repeated at `store_cassandra.go:73-92`, `:114-133`, `:150-181`, `:202-236`; literal `null` bound at `:120`, `:130`, `:209`, `:220`, `:232` | Adding one column means editing up to 10 statements with **nothing preventing a miss and no test to catch it.** And CLAUDE.md forbids clearing plaintext columns as NULLs bound into the INSERT — harmless *only* because these are unpinned, so **item 2 done carelessly would silently stop the legacy-plaintext clear from landing.** `message-worker` keeps them as separate unpinned `stripLegacyPlaintext*` UPDATEs. |

**Read items 2, 5 and 10 together.** They are not three independent fixes: pinning the plaintext creates is only safe once the legacy-plaintext clears are separate unpinned UPDATEs, and the redelivery that makes pinning *matter* is made likely by the unbounded thread scan approaching `AckWait`. Sequence it as: integration tests first (item 1), then split the clears out, then pin, then bound the scan — each step verified by the tests the first step adds.

**Also worth doing.** Unit-test `bootstrapStreams` (`bootstrap.go:23`, 0.0%) through the `streamManager` seam that exists for exactly that — the "verify" branch is this service's production startup gate and is untested. Attach `request_id` to every log line via `natsutil.RequestIDFromContext` as `message-worker/handler.go:113` does, and call `obs.ContextWithIdentity`; today **no log line in the service carries a correlation ID**, so a bot message cannot be traced from handler to store. And ensure the dispatcher goroutine parked on `sem <-` is drained after `iter.Stop()` — item 4 makes it visible, but it also needs a termination path.
