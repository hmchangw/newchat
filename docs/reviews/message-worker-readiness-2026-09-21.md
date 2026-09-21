# message-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `2369149` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The persistence hot path is correct where it matters most: plaintext creates pin `USING TIMESTAMP`, encrypted creates and the legacy-strip UPDATEs do not, `store_cassandra_writetime_test.go` pins both halves, `sonic` and `jsonwarm.Pretouch` are in place, the consumer opts into the outage retry budget, the #484 degraded start gates every document-creating thread write on the index gate, and Mongo is ordered before Cassandra so an outage NAKs before anything persists. Two design gaps carry the medium findings. The enrichment columns (`sender`, `mentions`) are re-resolved fail-open on every delivery yet bound under one pinned timestamp — the per-cell mixed-enrichment window CLAUDE.md itself names as unfixed — and a consume-loop exit is invisible to `/readyz`, so a pod that has silently stopped persisting history keeps reporting healthy; roomlist-worker already carries the readiness pattern to port. Performance is well-sized but leaves round-trips on the table: every subsequent thread reply pays two Paxos LWTs it can skip on non-redeliveries, thread-mention marking costs 2k+1 serial Mongo writes per reply, thread-store calls carry no deadline, and a 20 KB `tshow` reply's three-copy unlogged batch exceeds Cassandra's default fail threshold deterministically and then NAK-loops for the hour-long budget. `thread_reply_added` publishes the reply's own `CreatedAt` instead of the `TLM` the store computed, regressing thread freshness under out-of-order redelivery. Coverage is 56.3% with the parent `thread_room_id` CAS stamp untested at any layer; seven hand-aligned INSERT literals guard the pin rule only by comment and one hand-listed test; the README describes a service that no longer exists.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 1 high, 22 medium, 21 low, 7 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Log-AND-return on the Teams persist failure path — `message-worker/teamsbatch.go:140` — `slog.ErrorContext("teams batch: save message failed", …)` then `return res, err`; the error propagates unchanged through `handleBatch` (`teamsbatch.go:84`) into `jsretry.Settle` (`teamsbatch.go:74`), which logs it again at `pkg/jsretry/jsretry.go:138` ("message failed — retrying"). Every Cassandra outage produces two ERROR lines per message, violating the "never log AND return" rule.
- [medium] Six bare `return err` in the Mongo thread store — `message-worker/store_mongo.go:52,62,65,94,106,122` — each forwards `IndexGate.Ready`'s error without the local frame ("create thread room", "insert thread subscription", …). The callee does wrap (`pkg/mongoutil/indexgate.go:74`), so no context is lost below, but the store op in progress is invisible in the NAK log line. Direct breach of "never return bare err".
- [medium] README describes a service that no longer exists — `message-worker/README.md:3-56` — claims it consumes `MESSAGES-{siteID}` on `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, publishes to a `FANOUT-{siteID}` stream (no such stream in `pkg/stream/stream.go`), writes a `messages` table with `content`/`user_id` columns and touches `rooms.updatedAt`. Actual code binds `MESSAGES-CANONICAL` `.created` (`main.go:380`), writes `messages_by_room`/`messages_by_id`/`thread_messages_by_thread` (`store_cassandra.go:188-295`) and `thread_rooms`/`thread_subscriptions`. Config table lists `NATS_URL` with a default; it is `required` (`main.go:42`). An on-call reader is actively misled.
- [low] Two ERROR logs bypass the context-aware API — `message-worker/store_cassandra.go:465,480` — `slog.Error(...)` instead of `slog.ErrorContext(ctx, ...)` on the thread_room_id-stamp misses. `request_id` is passed by hand but the o11y handler's trace/span correlation (which reads ctx) is lost on exactly the lines that flag a permanently broken thread read. Only two non-ctx call sites in the service.
- [low] `pretouch` list drifted from the sonic call sites — `message-worker/pretouch.go:12-14` — warms `model.InboxEvent`, which this service never sonic-marshals (`outbox.Publish` uses `encoding/json`, `pkg/outbox/outbox.go:96,106`), while `model.ThreadUnreadAddedEvent` (`handler.go:752`) and `model.TeamsBatchRequest` (`teamsbatch.go:68`) are sonic-coded and not warmed. Cold first-call compile lands on the live thread-unread path.
- [low] Dead code — `message-worker/teamstransform.go:113` — `reactionShortcode` is referenced only by a comment (`:71`) and its own test; no production caller.
- [low] Mode magic strings — `main.go:87,241,374`, `bootstrap.go:46` — `"default"`/`"teams"` compared as raw literals in four places with no typed constant; a typo compiles.
- [low] Mixed JSON codecs in one service — `main.go:276`, `teamsbatch.go:163`, `teamstransform.go:33` use `encoding/json` while the same paths' outer envelope uses sonic (`teamsbatch.go:68`). CLAUDE.md names message-worker a sonic worker; the split is undocumented at the call sites.
- [nitpick] Corrupted UTF-8 (`â` for em-dash) in comments — `teamstransform.go:72,94`.
- [nitpick] `"error", failed` binds an int count to the conventional error key — `teamsbatch.go:96` — log pipelines that type the `error` field as string will choke or mis-index.
- [nitpick] Comment cites hard-coded line numbers that have drifted — `main.go:147` ("handler.go:159-201"; the Mongo-before-Cassandra ordering now lives at `handler.go:207-234`).
- [nitpick] `Handler`/`NewHandler`/`CassandraStore`/`NewCassandraStore`/`DefaultTransformer` are exported inside `package main` — `handler.go:34,53`, `store_cassandra.go:60,71`, `teamstransform.go:23` — contrary to "keep handler/store implementations unexported within services" (repo-wide habit, zero runtime effect).

Verified compliant (not findings): every plaintext create INSERT pins `USING TIMESTAMP` (`store_cassandra.go:192,203,284,295,316`); encrypted creates (`:241,251,363,374,391`) and the three legacy-strip UPDATEs (`:163-165`) are unpinned; `UpdateParentMessageThreadRoomID` (`:454`) and `threadcount.Maintain` carry no pin. `errcode.Permanent` used only for poison payloads (`handler.go:98`, `teamsbatch.go:64,71`); `errors.Is` throughout; no bare `Nak()`; no body/token logging; request_id on every WARN/ERROR line; `go vet` and `gofmt -l` clean.

### Recommendations

- [medium] Drop the `slog.ErrorContext` at `teamsbatch.go:140` (keep the metric) or switch `teamsbatch.go:74` to `jsretry.SettleQuiet` — one ERROR per failed message instead of two.
- [medium] Wrap the six `IndexGate.Ready` returns — `store_mongo.go:52,62,65,94,106,122` — e.g. `fmt.Errorf("create thread room: %w", err)`; NAK logs then name the operation.
- [medium] Rewrite `message-worker/README.md` against the current pipeline (stream, subject, three Cassandra tables, thread collections, `MODE`, required env vars) or delete it — a wrong runbook is worse than none.
- [low] `store_cassandra.go:465,480` → `slog.ErrorContext(ctx, …)`; drop the now-redundant hand-passed fields if the handler injects them.
- [low] Align `pretouch.go:12-14` with the actual sonic sites: add `ThreadUnreadAddedEvent`, `TeamsBatchRequest`; remove `InboxEvent`.
- [low] Delete `reactionShortcode` (`teamstransform.go:113`) and its test until reactions migrate; introduce `const modeDefault, modeTeams` and use them at `main.go:87,241,374`, `bootstrap.go:46`.
- [nitpick] Fix the mojibake at `teamstransform.go:72,94` and rename the `error` log key at `teamsbatch.go:96` to `failed`.
