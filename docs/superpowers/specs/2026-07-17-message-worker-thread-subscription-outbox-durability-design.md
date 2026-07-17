# message-worker Thread-Subscription Federation Durability — Adopt OUTBOX

**Status:** Design — approved, not yet implemented.
**Date:** 2026-07-17
**Related:** `docs/design/2026-07-05-membership-federation-durability.md` (PR #410, the OUTBOX relay this extends), `pkg/outbox`, `outbox-worker`, `inbox-worker`.

## 1. Problem

`message-worker` federates **thread subscriptions** cross-site: when a thread reply
involves a participant (the parent-message author or the replier) whose home site
differs from the local site, it must tell that remote site to create/update the
user's `ThreadSubscription` so the thread surfaces in their thread list and badge
feed.

Today `message-worker` does this with a **direct `InboxExternal` publish** to the
remote site's INBOX (`publishThreadSubInboxIfRemote` → `subject.InboxExternal`,
`message-worker/handler.go`), riding the MESSAGES_CANONICAL consumer's default
`MaxDeliver=5`. This is **structurally the pre-#410 room-worker failure mode**: on
a destination outage longer than ~5 redeliveries, the cross-site event is **dropped**
— the local thread-subscription write already committed, so local and remote silently
diverge with no repair.

PR #410 fixed exactly this class of loss for **membership** and room-service's
subscription-state events by routing them through a local **OUTBOX** stream that
`outbox-worker` forwards to the destination INBOX with retry-forever
(`MaxDeliver=-1`). `thread_subscription_upserted` was **out of scope** for #410 —
it is in neither `outbox.ConcurrentEventTypes` nor `outbox.OrderedEventTypes`, so
it never got the durability upgrade.

**Goal:** give `message-worker`'s cross-site `thread_subscription_upserted` the same
**delay-not-drop within retention** guarantee membership has today — a destination
down for an hour (or a day, up to OUTBOX `MaxAge`) delays the event, never loses it.

## 2. Scope

**In scope:** route `thread_subscription_upserted` through the local OUTBOX so
`outbox-worker` forwards it durably. Durability **parity with #410 as shipped**.

**Explicitly out of scope (unchanged from #410's own state):**
- **Reconciliation / anti-entropy** (beyond-retention re-derivation). §3.4 of the
  membership design is itself unbuilt; this change does not add it for thread subs.
  The guarantee is delay-not-drop *within* `MaxAge`, not unconditional.
- The **thread-reply badge event** (`publishThreadReplyEvent` →
  `chat.server.broadcast.{siteID}.thread.tcount`). It is a same-site, best-effort,
  core-NATS badge update — deliberately not durable, and not federated. Untouched.
- Any change to `inbox-worker` apply semantics or `outbox-worker` consumer topology.

## 3. Decision: concurrent lane, reuse `outbox.Publish`

### 3.1 Lane placement — concurrent (order-insensitive)

`thread_subscription_upserted` joins `outbox.ConcurrentEventTypes`, **not** the FIFO
`OrderedEventTypes` lane. Justification — the destination apply is genuinely
order-insensitive: `inbox-worker.UpsertThreadSubscription` is

- `$setOnInsert` for the immutable identity fields (`_id`, ids, `createdAt`,
  `lastSeenAt`) — commutative; first writer wins and the values are identical across
  events for the same `(threadRoomId, userAccount)`,
- `$max hasMention` — monotonic; a `true` can never be regressed to `false`
  regardless of arrival order,
- `$set updatedAt` — a cosmetic last-write timestamp with no correctness meaning.

This is also **exactly the ordering behavior the event has today** (direct
`InboxExternal` publish is unordered/concurrent), so riding the concurrent OUTBOX
lane is **not a regression** — it is the same delivery semantics with durable retry
added.

### 3.2 Producer — `message-worker` reuses `outbox.Publish`

`publishThreadSubInboxIfRemote` stops building the `InboxEvent` envelope and
publishing to `subject.InboxExternal`. Instead it calls the shared
`outbox.Publish(ctx, h.publish, h.siteID, roomID, destSiteID, eventType, payload,
dedupID, ts)`, which builds the byte-identical `InboxEvent` envelope, wraps it in an
`OutboxEvent`, and publishes to the **local** OUTBOX at
`chat.outbox.{origin}.{dest}.thread_subscription_upserted`.

`message-worker` already holds the exact `publish` closure `outbox.Publish` needs
(`func(ctx, subj, data, msgID)` — JetStream publish with `Nats-Msg-Id` when `msgID`
is non-empty, core NATS otherwise), the same one `room-worker` passes to its
`federate` helper. No new wiring in `main.go`.

**Preserved unchanged:**
- **Inner payload:** `sonic.Marshal(sub)` of the `ThreadSubscription`. `ThreadSubscription`
  has no map fields, so sonic output is semantically equivalent to `encoding/json`,
  and `inbox-worker` decodes it with `json.Unmarshal` regardless. The envelope itself
  is built by `outbox.Publish` with `encoding/json` — identical to the OUTBOX
  envelopes room-worker/room-service already send and `inbox-worker` already decodes,
  so the sonic→shared-path switch is inherently wire-compatible.
- **Dedup ID:** the existing seed
  `thread-sub-inbox:{threadRoomID}:{userID}:{msgID}:{hasMention}:{destSiteID}` stays.
  `outbox.Publish` uses it as the OUTBOX publish's `Nats-Msg-Id` (a MESSAGES_CANONICAL
  redelivery can't double-enqueue) **and** as the forward's `Nats-Msg-Id` at the
  destination (idempotent apply). `hasMention` stays in the seed so a
  `HasMention=false` upsert and a later `HasMention=true` update get distinct dedup
  IDs — otherwise stream-level dedup would swallow the mention update.
- **Guards:** the empty-`ownerSiteID` warning (caller-bug signal) stays in
  `message-worker` before the call. `outbox.Publish` independently no-ops on a blank
  or local destination, so the same-site short-circuit is retained too. The
  `isMigration` skip and the per-participant call sites (first reply, subsequent
  reply, parent-not-found) are unchanged — only the publish primitive changes.

### 3.3 Transport — `outbox-worker` (no change)

`outbox-worker`'s per-destination **concurrent** consumer
(`outbox-worker-concurrent-{dest}`, `MaxDeliver=-1`) already forwards every type in
`outbox.ConcurrentEventTypes` by filter subject. Adding
`thread_subscription_upserted` to that set is picked up automatically — **no code
change in `outbox-worker`**. A down peer stalls only its own per-destination lane;
healthy peers keep flowing.

### 3.4 Destination — `inbox-worker` (no change)

`handleThreadSubscriptionUpserted` already handles the type. Because forwarding is
at-least-once and the apply is idempotent (§3.1), duplicates from a retried forward
are absorbed by both the destination `Nats-Msg-Id` dedup and the idempotent upsert.

### 3.5 Contract bookkeeping

- Add `model.InboxThreadSubscriptionUpserted` to `outbox.ConcurrentEventTypes`
  (`pkg/outbox/outbox.go`). This is the single required contract edit; `Publish`
  rejects types in neither partition set, so this is what makes the producer call
  legal.
- Update the `pkg/outbox` package doc comment (and the `ConcurrentEventTypes` doc)
  to list `message-worker` as a third producer alongside `room-service`/`room-worker`.

## 4. No-loss guarantee (remote down) — inherited from #410

The change places `thread_subscription_upserted` on the identical rails that give
membership its guarantee, so the same vector table applies:

| # | Loss vector | Mitigation | Status |
|---|---|---|---|
| 1 | Remote unreachable (target case) | Producer commits to local OUTBOX; forward Naks with `MaxDeliver=-1` → retained, delivered on recovery. | closed by this change |
| 2 | Local OUTBOX node loss | OUTBOX is `R3 + file` (IaC precondition, shared with #410). | IaC precondition |
| 3 | Outage longer than retention | Bounded by OUTBOX `MaxAge`; reconciliation would be needed for the unconditional guarantee — **out of scope**, same as membership. | partial (accepted) |
| 4 | Producer crash before durable enqueue | Publish returns an error → MESSAGES_CANONICAL Naks and redelivers; local thread work re-runs idempotently and re-enqueues; stable `Nats-Msg-Id` makes it a dedup no-op. | closed by this change |
| 5 | Poison / malformed event | Producer emits well-formed events; `outbox-worker` Ack-drops permanent with a warning (defensive). | inherited |

**End-to-end invariant:** within OUTBOX retention, a remote thread subscription is
never silently dropped when its home site is temporarily unreachable. Beyond
retention remains an accepted gap (matching membership) until reconciliation is
built.

## 5. Failure-path improvement (bonus)

Today a failed cross-site publish Naks the **whole** MESSAGES_CANONICAL message,
re-running all local thread work (idempotent) up to `MaxDeliver=5`, then dropping it.
After this change the publish targets the **local** OUTBOX (same-site, highly
available), so it effectively never fails for a *remote* outage — the canonical
message acks promptly and the cross-site concern is owned by `outbox-worker`. The
hot path trades one cross-gateway INBOX publish for one local OUTBOX publish (same
count, lower latency, higher availability).

## 6. Testing

TDD, per CLAUDE.md. New/changed coverage:

- **`message-worker` unit tests** (`handler_test.go`): assert `publishThreadSubInboxIfRemote`
  now publishes to the **OUTBOX** subject `chat.outbox.{origin}.{dest}.thread_subscription_upserted`
  (via the captured publish closure) with the correct `Nats-Msg-Id` (dedup seed),
  an `OutboxEvent` whose decoded `Envelope` is the expected `InboxEvent`
  (`Type=thread_subscription_upserted`, `SiteID=origin`, `DestSiteID=dest`, payload
  = the `ThreadSubscription`). Update the existing `CarriesAttachments`-style
  assertions and any test asserting the old `InboxExternal` subject.
  - Keep coverage for: same-site → no publish; empty `ownerSiteID` → warn + no
    publish; `isMigration` → no publish; `HasMention` flip → distinct dedup IDs.
- **`pkg/outbox` unit tests** (`outbox_test.go`): `thread_subscription_upserted` is a
  known/accepted type (no longer rejected); it partitions into the **concurrent** set.
- **`outbox-worker` / `inbox-worker` integration:** existing federation tests already
  exercise the concurrent lane end-to-end; extend a table/case to include
  `thread_subscription_upserted` so a forwarded envelope lands and upserts. No new
  container topology.
- **Coverage floor:** keep changed packages ≥ 80% (target 90% for handlers).

## 7. Out-of-scope / follow-ups

1. **Reconciliation for thread subscriptions** (unconditional, beyond-retention) —
   tracks membership's own §3.4 follow-up.
2. **Dead-letter + alert** for permanent poison — shared with #410's follow-up.
3. **Client API docs:** none. `thread_subscription_upserted` is an internal
   federation event, not a client-facing RPC or a `pkg/model` client/event struct —
   `docs/client-api.md` and its derived views are untouched.

## 8. Files touched (anticipated)

- `pkg/outbox/outbox.go` — add the type to `ConcurrentEventTypes`; update producer docs.
- `message-worker/handler.go` — rewrite `publishThreadSubInboxIfRemote` to call
  `outbox.Publish`; import `pkg/outbox`.
- `message-worker/handler_test.go` — retarget assertions to the OUTBOX subject/envelope.
- `pkg/outbox/outbox_test.go` — accept + partition assertion for the new type.
- `outbox-worker` / `inbox-worker` integration tests — add the new type to an existing case.
- No changes to `main.go` (publish closure already present), `bootstrap.go` (OUTBOX
  owned by `outbox-worker`, mirroring room-worker), `outbox-worker`, or `inbox-worker`
  production code.
