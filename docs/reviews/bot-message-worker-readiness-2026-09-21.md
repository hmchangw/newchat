# bot-message-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `fe27c91` (base `main`)  
**Overall score:** 1.8 / 5 (baseline 2026-09-01: 2.5, Δ -0.7)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

bot-message-worker is the weakest service in the fleet on this audit. Every reviewer flagged the same thread-partition defect: bot thread replies bind `thread_room_id` to the parent *room* ID, while message-worker keys that table by a Mongo ThreadRoom UUID and stamps it onto the parent row, and history-service resolves replies only through that stamp. Bot replies therefore land in a partition no reader queries, and the shared thread-count scan then runs over that wrong partition and blind-writes the result onto the parent, clobbering the user pipeline's count. It also decodes a `MessageEvent` envelope that bot-room-service does not send, so system messages arrive as zero-valued rows that fail and NAK until dropped. Four structural protections its twin has are missing: no outage retry budget (a Cassandra outage past roughly six minutes permanently drops bot messages, versus the hour message-worker survives), no `jobguard` panic guard on the worker goroutine, the consume loop is outside the WaitGroup so the Cassandra session can close under an in-flight message, and no per-message deadline or heartbeat. On the storage rules, none of the ten create INSERTs pin `USING TIMESTAMP` as CLAUDE.md requires, and the encrypted paths bind literal nulls inline — the exact shape the rulebook forbids, which will silently stop clearing legacy rows the moment someone adds the pending pin. `buildCassandraMessage` aliases rather than copies the quoted parent, so the at-rest split mutates the caller's message, and it skips the attachment re-encode, silently dropping quoted attachments on encrypted messages. Coverage is 14.1%, the lowest in the fleet: the entire Cassandra store, bootstrap and wiring are at zero, and `SaveMessage` and both encrypted paths have never been executed by any test.

| Dimension | Score |
|---|---|
| Code quality | 2 |
| Architecture | 2 |
| Test coverage | 1 |
| Maintainability | 2 |
| Integration | 2 |
| Performance | 2 |

**Findings by severity:** 1 critical, 17 high, 18 medium, 9 low, 4 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 2

### Evidence

- [high] Bot thread replies are written under a partition key no reader queries — `bot-message-worker/handler.go:65` — `threadRoomID := m.RoomID` is bound as `thread_room_id`, while `message-worker/handler.go:395-396` uses the Mongo `ThreadRoom.ID` and stamps it on the parent (`message-worker/store_cassandra.go:454-475`). `history-service/internal/service/threads.go:115` reads `thread_messages_by_thread WHERE thread_room_id = <parent's stamp>` and returns no replies when the stamp is empty (`:63-72`). bot-message-worker never stamps the parent, so its replies are invisible, and `countAndSetParentTcount` (`store_cassandra.go:276-279`) scans the same wrong partition and overwrites the parent's `tcount` with a bot-only count. Write/read mismatch verified in code; integration reviewer should cross-check.
- [high] Unit coverage is 14.1% (29/206), far under the 80% floor — `store_cassandra.go`, `bootstrapStreams`, `run`, `main` at 0%. `SaveMessage`, `saveEncrypted`, `saveThreadEncrypted` have no test of any kind: all four integration tests use `cipher=nil` and only `SaveThreadMessage` (`integration_test.go:174,218,262,297`), and the test DDL for `thread_messages_by_thread` (`:109-129`) lacks `enc_payload`/`enc_meta`, so the encrypted thread path cannot be exercised there.
- [high] Plaintext create INSERTs do not pin `USING TIMESTAMP` — `store_cassandra.go:97-116`, `:175-207`. CLAUDE.md requires every plaintext create to bind `writeTS(CreatedAt)` (`message-worker/store_cassandra.go:192-207`). The pin precondition holds better here than in message-worker: `toSender`/`toMentionSet` (`:28-51`) are pure projections of the event, no re-resolution. Exposure is real: `countAndSetParentTcount` runs after `ExecuteBatch` (`:211`, `:266`) and a transient failure there NAKs a committed create for replay under `jsretry.DefaultBackoff`.
- [medium] Encrypted creates clear plaintext columns as literal `null` bound into the INSERT — `store_cassandra.go:144,154,234,245,257` — the form CLAUDE.md forbids ("never as NULLs bound into it"). Harmless only while unpinned; pinning must first split these into separate unpinned `stripLegacyPlaintext*` UPDATEs (`message-worker/store_cassandra.go:163-165,245,255`) or the legacy-row clear silently stops landing.
- [medium] Handler hand-rolls the settle tree instead of `jsretry.Settle` — `handler.go:33-56` — and discards `msg.Ack()` errors uncommented at `:36`, `:45`. The only reason for the fork, `permanentErrorTotal` (`metrics.go:9`), is unreachable in production: `store_cassandra.go` imports no `errcode`, and `pkg/threadcount`, `pkg/atrest`, `pkg/cassutil` never return `errcode.Permanent` (grep: 0 hits). A `{}` payload decodes cleanly and is written with empty `ID`/`RoomID` (`:33-40`); message-worker returns `errcode.Permanent(errcode.BadRequest(...))` (`message-worker/handler.go:94-98`).
- [medium] No log line carries a request ID and the consume loop never stamps one — `handler.go:34,43,48,54`; `main.go:162-171`. `logctx.ConsumeContext` (`pkg/logctx/consume.go:37`) exists for this; message-worker uses it (`message-worker/main.go:392`).
- [medium] Dispatcher goroutine is outside the WaitGroup — `main.go:160-173`. A message parked on `sem <-` at shutdown is uncounted, so `wg.Wait()` passes and `cassSess.Close()` (`:196`) runs under it. message-worker counts the loop (`message-worker/main.go:290-295`).
- [medium] `store.go:10` has no `//go:generate mockgen` directive and no `mock_store_test.go`; tests use a hand-written `fakeStore` (`handler_test.go:21-55`). `deploy/azure-pipelines.yml` is also missing (29 of 37 services have it). Both are CLAUDE.md layout requirements.
- [low] `cassParticipant` (`store_cassandra.go:18-26`) duplicates `pkg/model/cassandra.Participant` (`pkg/model/cassandra/message.go:11-19`) field-for-field, and message-worker's own copy; the fork is how this write path drifted from message-worker's. Handler tests are five near-identical functions, not table-driven (`handler_test.go:95-171`); the ack-failed branch (`handler.go:53-56`) and `bootstrapStreams` (seam at `bootstrap.go:17-20`) are untested.
- [nitpick] `fmt.Errorf` with no verbs at `main.go:121`; prefer `errors.New`.

### Recommendations

- [high] Resolve the thread partition-key contract — `handler.go:65` — resolve/create the `ThreadRoom` and stamp the parent as message-worker does (or document why bot replies are intentionally unreadable); add an integration test that writes a bot reply and reads it back via `GetThreadMessages`' query.
- [high] Reach the 80% floor: unit-test `bootstrapStreams` via `streamManager`; add integration tests for `SaveMessage` and both encrypted paths (fix the test DDL to carry `enc_payload`/`enc_meta` on all three tables); port `message-worker/store_cassandra_writetime_test.go`.
- [high] In order: split the inline `null`s into separate unpinned `stripLegacyPlaintext*` UPDATEs (`store_cassandra.go:144,154,234,245,257`), then add `USING TIMESTAMP writeTS(msg.CreatedAt)` to the five plaintext creates (`:97-116,:175-207`), commenting the encrypted ones with the CLAUDE.md rationale.
- [medium] Replace `handler.go:40-56` with `jsretry.Settle` on an error returned from `write`; return `errcode.Permanent(errcode.BadRequest(...))` for empty `ID`/`RoomID`/`CreatedAt` so the poison metric is reachable.
- [medium] Stamp `logctx.ConsumeContext(...)` in the consume loop (`main.go:162`) and add `"request_id", natsutil.RequestIDFromContext(ctx)` to every log line.
- [medium] Count the dispatcher loop in the WaitGroup (`main.go:160`), add the `//go:generate mockgen` directive to `store.go`, and add `deploy/azure-pipelines.yml` mirroring a peer worker.
- [low] Replace `cassParticipant` with `cassandra.Participant` and share `toMentionSet` with message-worker so the two write paths cannot drift again.
