# unread-worker: extracting broadcast-worker's MongoDB writes

**Date:** 2026-08-13
**Status:** Design approved, not implemented

## Problem

`broadcast-worker` both fans out room events and writes room/subscription state to
MongoDB. When MongoDB is unavailable the read path degrades gracefully — `GetRoomMeta`
is served from an L1 LRU and an L2 Valkey tier — but the write path cannot. In
particular `SetSubscriptionMentions` (and `UpdateRoomLastMessage` before it) runs
**before the publish switch** in `handleCreated` and returns its error to the
handler, which NAKs the JetStream message before any broadcast is published. So a
MongoDB write failure does not duplicate a broadcast — it **suppresses** one. And
because `buildConsumerConfig` takes the repo-default `MaxDeliver=5`, a sustained
MongoDB outage does not retry indefinitely either: after five NAKs the message falls
out of the stream and is **dropped, never delivered to any client**. (Verified at the
pre-extraction commit `846e8c5`.)

The fan-out and the writes have different availability requirements. Fan-out should
survive a MongoDB outage; the writes should be retried until they land. One consumer
with one ack cannot express both.

## Scope

Move every MongoDB write derived from a canonical message event at the **room** level
into a new service. Thread-level state stays where it is — `message-worker` already
owns `MarkThreadSubscriptionMention`, `AdvanceThreadSubscriptionLastSeen` and
`UpdateThreadRoomLastMessage`.

Out of scope: hardening broadcast-worker's remaining uncached reads (`GetRoom`,
`ListSubscriptions`, `GetThreadFollowers`, `GetHistorySharedSince`). Those still fail
during a MongoDB outage, so the edit/delete/pin/react paths and all DM fan-out remain
unavailable. This design protects the dominant created-channel path only.

## Approach

A new flat service `unread-worker` at the repo root, `package main`, with its own
durable consumer on MESSAGES-CANONICAL.

Two alternatives were considered and rejected:

- **Fold the writes into `message-worker`.** It already owns the thread-level twins of
  all three writes, and it adds no new consumer. Rejected because one consumer means
  one ack: NAK-ing on a MongoDB failure would stall Cassandra message persistence
  behind a MongoDB outage, trading a badge problem for a durability problem.
- **A second consumer inside the existing broadcast-worker binary.** Fixes the
  duplicate-broadcast bug with no new deployment, but costs the same extra stream
  delivery as a separate service while giving up independent scaling and failure
  isolation — and grows a MongoDB-outage backlog inside the process that is supposed
  to be MongoDB-independent.

The accepted cost of the new service is a fourth consumer on MESSAGES-CANONICAL
(the payload is the full message, so the added delivery bandwidth is real) and one
more service to deploy and operate.

## Boundary

`mention.Parse(content)` is a pure function of the message content, and `meta.ID` /
`room.ID` are `msg.RoomID` re-fetched. The write set is therefore a **pure function of
the canonical event with zero MongoDB reads**. `unread-worker` needs no userstore,
no roomkeystore, no Valkey, and no room-type switch — in broadcast-worker the writes
already run before the room-type switch, so they are room-type agnostic.

### Trigger conditions

Mirrors broadcast-worker's current behaviour exactly.

| Event | `shouldUseThreadFanOut` | Writes |
|---|---|---|
| `created` | false | `lastMsgAt`, `lastMsgId`, `updatedAt` (+ `lastMentionAllAt` when `@all`); sender `$max lastSeenAt`; `hasMention` for parsed mentions |
| `created` | true | none — `message-worker` owns thread state |
| `updated` | false | `hasMention` for parsed mentions, gated on non-nil `EditedAt` |
| `updated` | true | none |
| `deleted`, `pinned`, `unpinned`, `reacted` | — | none |

### Behaviour change

On the created path `SetSubscriptionMentions` currently runs only after `GetRoomMeta`
succeeds, so a nonexistent room skips it. Extracted, it is attempted unconditionally.
The filter matches on `roomId`, so a missing room is a no-op. Benign.

## Service shape

```
MESSAGES-CANONICAL-{site}
  └─ durable "unread-worker" (filter: chat.msg.canonical.{site}.>)
       │
   consume loop (single goroutine — per-message work is parse + map merge, no I/O)
       │  derive intents, merge into batch, hold the jetstream.Msg
       ▼
   batch: rooms map[roomID]lastMsgUpdate
          subs  map[roomID]mentionAccounts, map[roomID+account]lastSeenAt
       │
   flush ticker (FLUSH_INTERVAL, default 250ms)
       ├─ BulkWrite(rooms, ordered=false)
       ├─ BulkWrite(subscriptions, ordered=false)
       ├─ success → Ack every held msg
       └─ failure → settle per the table below, drop the batch
```

Files: `main.go`, `handler.go` (event → write intents), `batch.go` (merge, flush,
settle), `store.go`, `store_mongo.go`, `bootstrap.go`, plus `handler_test.go`,
`batch_test.go`, `integration_test.go`, `mock_store_test.go`.

The consume loop is a single goroutine rather than the `MAX_WORKERS` semaphore pattern
broadcast-worker uses: per-message work is a regex parse and a map merge with no I/O,
so a worker pool would add contention on the batch mutex for no throughput.

`MODE` (`user` | `bot`) drives stream wiring through `stream.Resolve` exactly as in
broadcast-worker, so the binary deploys twice against both canonical streams.
`deploy/user/` and `deploy/bot/` each carry a Dockerfile, docker-compose.yml and
azure-pipelines.yml, matching broadcast-worker's layout.

Stream bootstrap follows the repo convention: `bootstrapConfig` with
`env:"STREAMS" envDefault:"false"`, a `bootstrapStreams` helper that verifies the
stream in production and creates it only when `BOOTSTRAP_STREAMS=true`.

### Replay safety

NAK-and-retry means any message can be redelivered after a newer one has already
landed, so every write must be safe to replay out of order.

- `lastMsgAt` / `lastMsgId` — add the filter `{_id: roomID, lastMsgAt: {$not: {$gte: at}}}`
  so a stale replay cannot regress the pointer. `$not`/`$gte` (not `$lt`) so the filter
  still matches a missing or null `lastMsgAt`. This is a real improvement: today's
  coalescer carries the same hazard silently.
- `lastSeenAt` — already `$max`, safe.
- `hasMention` — `$set: true` filtered on not-already-read, additive, safe.

### Back-pressure is the retry mechanism

During a MongoDB outage held messages accumulate, `MaxAckPending` fills, and JetStream
stops delivering to this consumer alone. broadcast-worker's fan-out is untouched.
`MaxAckPending` must exceed `flush_interval × peak_rate` with headroom, and `AckWait`
must exceed flush interval plus write latency. Both are explicit config, not defaults.

## Error handling

| Failure | Classification | Action |
|---|---|---|
| Malformed event payload | `errcode.Permanent` | Ack-drop with a warning — it will never parse on redelivery (same as broadcast-worker today) |
| MongoDB network error, timeout, not-primary | transient | `NakWithDelay`, backoff by delivery count, `MaxDeliver=-1` |
| `mongo.BulkWriteException` carrying server write errors with no network error | permanent | Log at Error with the room count, then **Ack** |

The last row is load-bearing. With `MaxDeliver=-1` a single server-rejected document
would otherwise loop forever and block the consumer indefinitely — the exact stall this
design exists to avoid. A rejected write never succeeds on retry, so it is dropped
loudly rather than retried.

### Health probe excludes MongoDB

Only the NATS check is registered with `health.ServeWithPprof`. Putting MongoDB in the
readiness probe would make Kubernetes churn the pod during precisely the outage this
service is designed to ride out, discarding the held batch on every restart.

### Shutdown

`iter.Stop()` → drain the consume loop → final flush on a fresh context with timeout →
settle held messages → `nc.Drain()` → MongoDB disconnect → observability flush. A hard
kill loses only acks, and replay safety makes the resulting redelivery a no-op.

## Changes to broadcast-worker

Removed: `UpdateRoomLastMessage`, `BulkUpdateRoomLastMessage`, `SetSubscriptionMentions`,
`AdvanceSubscriptionLastSeen`, all of `coalescer.go` and `coalescer_test.go`, the
`LAST_MSG_FLUSH_INTERVAL` config field, and the coalescer wiring in `main.go`.

`Store` drops to five read methods: `GetRoom`, `GetRoomMeta`, `ListSubscriptions`,
`GetThreadFollowers`, `GetHistorySharedSince`. `NewHandler` takes the cached store
directly instead of the coalescing wrapper. Mocks are regenerated with
`make generate SERVICE=broadcast-worker`, and `handler_test.go` drops its write
expectations.

After this change broadcast-worker never NAKs because of a MongoDB write failure, so
the duplicate-broadcast-on-redelivery path is gone.

## Rollout

Both halves ship in **one PR**. Splitting them across two PRs would leave the write
logic duplicated in the repo for the length of the soak, forcing every change to that
logic to be made twice. The services deploy independently, so a safe rollout needs
only deploy ordering — not a dual-write window in source.

1. Merge the single PR.
2. Deploy `unread-worker` first, with `DeliverPolicy: New` so it does not replay
   stream history. The still-running old broadcast-worker image keeps writing; the
   overlap is harmless because the writes are idempotent and additive.
3. Soak for as long as desired. Merge time and deploy time are decoupled, so soaking
   costs nothing in the repo.
4. Roll broadcast-worker to the new image. From that point only `unread-worker`
   writes. There is no gap in either direction.

Rollback is an ordinary image rollback of broadcast-worker — the previous image still
writes.

The ordering rule — **deploy `unread-worker` before rolling broadcast-worker** —
goes in the PR description and the deploy README.

## Consistency trade-off

The writes become eventually consistent relative to the broadcast. A client can
receive a new-message event a few hundred milliseconds before its own `hasMention`
badge is durable. For `lastMsgAt` this is not new — the coalescer already flushes at
250 ms. For `hasMention` it is new: that write is synchronous-before-broadcast today.
Accepted, because a badge that arrives late is strictly better than a duplicated
message plus a badge that is lost when the flush fails.

A third deferred write has the same shape but a subtler effect: the sender's own
`lastSeenAt` advance. In the base, `AdvanceSubscriptionLastSeen(sender)` ran
synchronously before the broadcast was published, so any client-triggered
`room.read` necessarily observed the sender's already-advanced `lastSeenAt`.
Extracted, that advance now lands up to `FLUSH_INTERVAL` plus write latency after
the broadcast. `room-service`'s `messageRead` handler recomputes the room read-floor
as `MinSubscriptionLastSeenByRoomID` and persists it to `rooms.minUserLastSeenAt`;
if a recipient marks the room read inside the flush window, that `min` is taken over
a subscription set where the sender has not yet advanced, so `minUserLastSeenAt` is
written below the true floor. Nothing recomputes it afterwards — the room's read-floor
stays understated until the next `messageRead` call recomputes it (which, by then, is
usually correct because the sender's advance has long since landed). Accepted for the
same reason as the other two: `room-service` is explicitly out of scope for this
extraction, and understating a read-floor by a sub-second window is a much smaller
defect than a duplicated broadcast.

## Testing

Unit tests are written first, red before green, per the repo's TDD rule.

- `handler_test.go` — table-driven over all six event types crossed with
  thread/non-thread, asserting the derived intent set. Includes "thread reply produces
  no writes" and "updated without `EditedAt`".
- `batch_test.go` — coalescing semantics (max by `createdAt`; `lastMentionAllAt` sticks
  across later non-mention-all messages), empty-flush no-op, and settlement:
  ack-on-success, nak-on-transient, ack-on-permanent-write-error, driven by a fake
  iterator and fake `jetstream.Msg` modelled on broadcast-worker's `consumeloop_test.go`.
- Config parsing.

Integration (`//go:build integration`, `TestMain` → `testutil.RunTests`) uses
`testutil.MongoDB` and `testutil.NATS`: publish canonical events and assert the
resulting `rooms` and `subscriptions` documents, plus an explicit replay-ordering test
that delivers an older message after a newer one and asserts `lastMsgAt` does not
regress.

Coverage: 80% floor, 90%+ target on `handler.go` and `batch.go`.

## Documentation

- CLAUDE.md — add `unread-worker` to the event-flow paragraph.
- `docs/architecture.md` — add the service.
- `docs/client-api.md` is **not** touched: no client-facing handler changes and no
  `pkg/model` struct changes.
