# oplog-transformer — migration stream → new-stack message pipeline (Design)

> **Status:** DESIGN — stage 2 of the data-migration suite. Consumes `MIGRATION_OPLOG_{site}` (produced by stage-1 `oplog-connector`) and re-injects migrated RocketChat **messages** into the existing message pipeline. Depends on the stage-1 contract (`model.OplogEvent`, `chat.migration.oplog.*` subjects). Built on branch `claude/oplog-transformer` (stacked on the stage-1 branch).

*A JetStream consumer — the counterpart to the connector's producer. One durable consumer on `MIGRATION_OPLOG_{site}`, routing each change event by collection. This spec covers the `rocketchat_message` path only; other collections (e.g. `users` → inbox-worker) are deferred to a later spec.*

---

## 0. Where this sits

```text
  oplog-connector ──▶ MIGRATION_OPLOG_{site} ──▶ oplog-transformer ──┬─▶ chat.msg.canonical.{site}.created   (insert)
  (stage 1)                                       (stage 2, THIS)     │      └─ message-worker (Cassandra) + search-sync (ES)
                                                                      │         broadcast/notification SKIP (X-Migration: live)
                                                                      │
                                                                      └─▶ chat.migration.internal.{site}.msg.{edit,delete}  (update / delete)
                                                                               └─ history-service migration handlers
                                                                                    └─ delete resolves the row by message_id (GetMessageByID); edit by source-doc locator
                                                                                    └─ Cassandra edit/soft-delete (retry until insert lands)
                                                                                    └─ chat.msg.canonical.{site}.{updated,deleted} (X-Migration: live)
```

The connector deliberately did no enrichment ("the transformer's job"). The transformer does that enrichment here: it formats the opaque oplog payload into new-stack events, doing **source-Mongo lookups** where the oplog delta is insufficient.

---

## 1. Goal & scope

**Goal.** Re-inject migrated RocketChat messages into the existing new-stack pipeline so they are persisted (Cassandra), indexed (ES), and editable/deletable — **without** re-delivering historical messages to online users (the legacy system already delivered them).

**In scope (this spec):**
- The `oplog-transformer` service: durable consumer + route-by-collection.
- The `rocketchat_message` path: insert, update (edit / soft-delete).
- **Message text + core fields only** — `id`, `roomId`, sender identity, `content`, timestamps, thread-parent linkage. Reactions, file attachments, pins, and RocketChat system-message types (`t`) are **explicitly deferred** (see below).
- Small edits to three existing services: `broadcast-worker`, `notification-worker`, `history-service`.

**Out of scope (deferred / later specs):**
- **Message reactions, file attachments, pins, and system-message types (`t`: `room_changed_*`/`user_added`/…)** — named here so the omission is **intentional, not silent fidelity loss**. Migrated messages carry text + core fields; these enrichments are a later stage. Source-E2E messages (`t:"msg_encrypted"`, ciphertext in `msg`) are also skipped — we can't decrypt them.
- All non-message collections (`users`, `rocketchat_room`, `rocketchat_subscription`, `company_*`, …). The router logs-and-skips them for now.
- The bulk `history-migrator` (sibling, separate).

## 1.1 Migration context — cold cutover, not parallel-live

This is **not** a parallel/federated run with users split across old and new sites. The **new stack has no user activity for ~a month** while the migration runs; users stay on the legacy system until a one-shot cutover. The **source stays live** during this window (that is why CDC exists), but the **new stack is dormant**. Implications baked into this design:

- **`notification-worker` is the real reason for `X-Migration: live`** — it would otherwise push to users' **devices** (which exist regardless of which stack a user is "on") for every migrated message. **`broadcast-worker`** has no connected clients on the dormant new stack, so suppressing it is mostly avoiding wasted work + future-proofing.
- We **intentionally do NOT maintain broadcast-worker's other side-effects** (thread reply counts, room last-message / unread aggregates) for migrated messages, **and we do NOT recompute them at cutover** — accepted as a known limitation. Migrated threads/rooms may show stale or zero counts; this is a deliberate trade for a temporary migration.
- No ordering races against live *new-stack* traffic (there is none). Races remain only against the **live source** mutating mid-migration — handled by the current-state lookup + retry-until-present convergence (§4–§5).

---

## 2. Service shape

- **Consumer:** one **durable** consumer on `MIGRATION_OPLOG_{siteID}`, **sequential** (`cons.Consume()` — lower-volume, ordering-sensitive). Ack-after-success.
- **Single active consumer (operational invariant):** ordering depends on one sequential durable — the transformer runs **one active replica** (like the connector's `replicas=1`). Scaling it horizontally breaks per-message order.
- **Routing:** dispatch on the event's `Collection` field. Today: `rocketchat_message` → message handler; everything else → `default` (log-and-skip + metric).
- **Dependencies:** NATS (consume + publish + request/reply) and **source-Mongo read access** for update lookups, read with **`primaryPreferred`** — a lagging secondary could return *pre-edit* content and we'd apply stale text with no self-correction (§4). No transformer-owned database.
- **No stream bootstrap:** it consumes a stream owned by the connector/ops and publishes to the ops-owned `MESSAGES_CANONICAL`. It creates only its own durable consumer.

---

## 3. Insert path (`op = insert`)

The oplog event carries the **full document** (insert is native in the oplog), so **no lookup**. (`replace` also carries a full doc but is an edit of an *existing* message — it goes through the update path in §4, not here; routing it to `.created` would hit message-worker's write-once LWT and silently drop the change.)

1. `messagemap` translates the RocketChat doc → `model.Message`:

   | RocketChat | `model.Message` |
   |---|---|
   | `_id` | `ID` (kept verbatim — 17-char) |
   | `rid` | `RoomID` |
   | `u._id` | `UserID` |
   | `u.username` | `UserAccount` |
   | `u.name` | `UserDisplayName` |
   | `msg` | `Content` |
   | `ts` | `CreatedAt` |
   | `editedAt` | `EditedAt` |
   | `tmid` | `ThreadParentMessageID` |
   | … | … (full mapping pinned during impl against the source schema) |

2. Wrap in `model.MessageEvent{Event: EventCreated, SiteID, Timestamp}`. The event-level **`Timestamp` is publish-time** (`time.Now().UTC().UnixMilli()`) per CLAUDE.md — distinct from the domain `CreatedAt`/`EditedAt` carried inside `Message`. (Same for the `.updated`/`.deleted` events history republishes in §4/§4.5.)
3. Publish to the **existing** `chat.msg.canonical.{site}.created` with header **`X-Migration: live`** and dedup id = the message `_id` (`Nats-Msg-Id`).
4. **Confirmed publish (required):** the transformer **awaits the JetStream pub-ack** before acking the oplog message. For inserts the canonical publish is the *only* durability handoff — message-worker reads it off the stream — so a fire-and-forget publish + oplog ack would **lose the message**. This is distinct from history-service's republish in §4, which is best-effort *because Cassandra is already written*.

**Downstream:**
- `message-worker` (filters `.created`) → Cassandra persist (write-once LWT = idempotent on replay).
- `search-sync-worker` (filters `.>`) → ES index.
- `broadcast-worker` + `notification-worker` → **skip** when `X-Migration: live` is present.

**ID reuse / idempotency.** Migrated messages keep their RocketChat **17-char `_id`** as the message ID — `idgen.IsValidMessageID` already accepts 17-char (the legacy length exists for exactly this). Re-running migration → same ID → LWT no-op.

---

## 4. Update path (`op = update`, and `replace`)

`update` and `replace` are handled **identically** — both are edits of an existing message. The only difference: `update` carries just a delta (needs the lookup), while `replace` carries the **full new doc** in the event (skips the lookup; map directly).

1. **Resolve the full doc:** for `update`, **lookup** by `_id` from source Mongo — read **`primaryPreferred`** so a lagging secondary can't hand back pre-edit content (recovers `roomId`, `createdAt`, current content/flags); for `replace`, use the event's full document. An `update` lookup miss → log-and-skip + metric.
2. **Classify** edit vs soft-delete by the source's deletion marker.
   > **Pin during impl (verify FIRST):** confirm the exact soft-delete signal. The reference (`SOURCE_DATA.md`) lists no `rm` `t`-type and describes deletes as hard `op=delete` — if so the soft-delete branch never fires and *all* deletes flow through §4.5. If the source *does* soft-delete (sets `t:"rm"` on an `update`), that branch routes into the same delete-by-id path.
3. **Route — edit** via **sync request/reply** to `chat.migration.internal.{site}.msg.edit` with `MigrationEditRequest{messageId, roomId, createdAt, content, editedAt}` (the source lookup supplies `roomId`+`createdAt`, which the edit's Cassandra UPDATE needs). **`messageId` = the event's `documentKey._id`** (the already-resolved id), not the looked-up doc's `_id` — so a projection that ever omits `_id` can't produce an empty-id request that Nak-loops to `MaxDeliver`.
4. **history-service `MigrationEditMessage`** (registered via `natsrouter`) → edit content in Cassandra → best-effort publish `chat.msg.canonical.{site}.updated` (with the **`X-Migration: live`** header, via `publishCanonicalBestEffort`) → reply ok. The Cassandra UPDATE does **not** create the row; if the insert hasn't been persisted yet the writer returns `ErrMessageNotFound`, a retryable error (the transformer Naks and retries — see §5).
5. Transformer **acks the oplog message only after a successful reply**; failure → Nak/retry.

### 4.5 Delete path (`op = delete`, and soft-delete `update` with `t:"rm"`)

Both delete shapes collapse to **delete-by-id** — the target resolves everything from the message id, so no source doc is needed:

- **Hard delete** (`op = delete`): the event carries only `documentKey._id`. Because **ids map 1:1** (`_id` *is* the target `message_id`) and the message was already migrated, no source lookup is possible *or needed*.
- **Soft delete** (`op = update`, looked-up doc has `t:"rm"`): classified in §4.2; routed into the same delete-by-id path.

Route via **sync request/reply** to `chat.migration.internal.{site}.msg.delete` with **`MigrationDeleteRequest{messageId, deletedAt}`** only (`deletedAt` = the event's `clusterTime`). **history-service `MigrationDeleteMessage`:**
1. `GetMessageByID(messageId)` — `messages_by_id` is `PRIMARY KEY (message_id, created_at)`, so this resolves `roomId`+`createdAt` **from the id alone** (`cassrepo/messages_by_id.go`).
   - **not found** (insert not yet persisted, or a message never migrated) → **retryable error** → transformer Naks/retries until the insert lands (bounded by `MaxDeliver`, then `Term`-skips a never-migrated message).
   - **already deleted** → reply ok (idempotent), no CAS, no publish.
2. else `SoftDeleteMessage(msg, deletedAt)` → best-effort publish `…deleted` with `X-Migration: live` → reply ok.

Best-effort publish is deliberate (Cassandra is truth, migration is temporary — see §5 + Footprint).

### 4.6 Degraded events (the connector's lossless-degrade contract)

The connector never drops an event whose opaque field fails to encode — it publishes it with **`Degraded=true`** + `DegradedReason`, the failed field left `nil` (connector spec §2.4). The transformer **must not re-drop these** (doing so would defeat the connector's lossless design), but it also must not ingest a silently-missing field as if clean.

- The transformer **decodes** `Degraded`/`DegradedReason` from the `OplogEvent`.
- A degraded event is **not** poison. When the dropped field is one the path *needs*:
  - degraded **insert** (`fullDocument` nil) → **recover via a source lookup on `_id`** (the same lookup §4 uses for updates), then map + publish. A lookup miss → **Nak-retry** (the source is live; bounded by `MaxDeliver`), **never** `Term`.
  - any op whose `documentKey` is nil (so `_id` can't be resolved) → **Nak-retry**, not `Term`.
- A degraded event whose **non-required** field was dropped (e.g. `updateDescription` on an `update`) needs no special handling — the update lookup already re-reads the live doc, so it self-heals.
- Degradation is logged with `DegradedReason` and counted by a distinct **`degraded`/recovered** metric — never folded into the generic poison/`Term` path.

Genuine poison (a field that is *present* but un-decodable, i.e. a real malformed doc) is still `Term`-ed (§5). The distinction: *missing-because-degraded* ⇒ recover/Nak; *present-but-corrupt* ⇒ Term.

---

## 5. Ordering, idempotency, at-least-once

- **In-order, sequential:** one durable consumer, one event at a time, ack-after-success → per-collection oplog order preserved end to end.
- **Order-independent across paths via retry-until-present (key property):** the Cassandra writer CAS-updates and does **not** create the row — there is no upsert. Insert-via-canonical (`message-worker`, async) and edit/delete-via-history (sync) converge by **retry, not upsert**: an edit/delete that arrives before message-worker has persisted the insert finds no row, so the handler returns a **retryable error**, the transformer **Naks and retries** until the insert lands (bounded by the consumer's `MaxDeliver`). A late `.created` still hits message-worker's write-once LWT and no-ops; an early delete that *did* land would never resurrect on a later `.created` LWT — but it can't land early in the first place, because absent ⇒ retry. Convergence is **retry-until-present**, not upsert.
- **Idempotent on replay/redelivery:** message `_id` reuse + Cassandra write-once LWT + the delete handler's already-deleted short-circuit + canonical dedup id.
- **At-least-once on the write (truth), best-effort on the publish.** The transformer's ack covers the **Cassandra write** — history replies ok after the edit/soft-delete, and the transformer acks only then; transient write/lookup/RTT failures **and not-yet-persisted rows** → Nak + capped backoff (the latter retries until the insert lands, hence the large `MaxDeliver`), permanent (a *present-but-corrupt* doc) → `Term`-and-skip + metric so one bad event never wedges the stream. The **canonical publish stays best-effort** ("as usual"), so `search-sync` convergence is not guaranteed per-op. This is an intentional, temporary-migration trade: no standing retry/await machinery. Residual ES drift (a missed re-index / soft-delete) is reconciled by an **optional end-of-migration reindex sweep** (a one-off job, not standing infrastructure).
- **History reply classification.** The reply from `chat.migration.internal.{site}.msg.{edit,delete}` is either `MigrationAck{OK:true}` (success) or an `errcode` error envelope. The transformer **classifies it** (via `errcode.Parse`): `NotFound` (the not-yet-persisted / retry-until-present case) and infra/timeout → **Nak** (retryable); a **non-retryable** category (e.g. a malformed/`BadRequest`) → **`Term`** + metric. This stops a genuinely permanent history rejection from silently exhausting against `MaxDeliver` with no signal. (In practice history rejections are `NotFound`/infra — the migration constructs valid, server-side requests — so the `Term` branch is a safety net, not a hot path.)

---

## 6. Cross-service changes

| Service | Change |
|---|---|
| **broadcast-worker** | Skip canonical events carrying `X-Migration: live` (before fan-out). It currently consumes all canonical subjects with no filter, so this is an explicit header check in its handler. |
| **notification-worker** | Same `X-Migration: live` skip (it already filters `.created`). |
| **history-service** | Two new internal handlers (`chat.migration.internal.{site}.msg.{edit,delete}`) → `MigrationEditMessage` / `MigrationDeleteMessage` (edit/soft-delete + canonical republish; absent row ⇒ retryable error so the transformer retries until the insert lands); the canonical publish path gains the ability to **set the `X-Migration` header**. |
| **message-worker** | **Thread replies only.** For `X-Migration: live` events, skip the thread-**subscription** writes (insert/upsert + cross-site outbox), the `hasMention` mark, and the live tcount badge. Still persists the reply and materializes `thread_rooms` + `replyAccounts`. Non-thread migrated messages are unchanged (persist only). See §6.2. |

`X-Migration: live` semantics: *persist & index this message, but do NOT re-deliver it — the source system already delivered it to users.* Read by the two live-delivery services; ignored by the persistence/index services.

**Security (required):** the `chat.migration.internal.{site}.msg.{edit,delete}` subjects bypass the auth that the user-facing edit/delete RPCs enforce (owner/admin only). They MUST be restricted to **server identities** in the NATS account permissions — a client able to publish there would have an unauthenticated edit/delete bypass.

**Not maintained (by design):** suppressing broadcast-worker also suppresses its aggregates (room last-message/unread) for migrated messages — **accepted limitation, not recomputed** (§1.1). (Thread-reply specifics, including who owns thread-subscription state, are in §6.2.)

### 6.1 Footprint & teardown (the migration is temporary)

The migration loses traffic after cutover and must not leave standing cost or permanent coupling:

- **Transformer lives in the deletable `data-migration/` folder** and idles to ~zero when source traffic stops (scale to zero / delete at cutover). No standing per-op machinery: best-effort publish means no awaited pub-acks, no retry queues, no held resources.
- **Permanent core-service edits are minimal and removable:** the `X-Migration: live` skip in broadcast/notification is a few lines that become harmless dead code once no event carries the header; the two history-service migration handlers are unused when idle and are **removed in the cleanup PR** that deletes the suite.
- **No new permanent infrastructure:** no transformer DB, no new stream (reuses `MIGRATION_OPLOG` + `MESSAGES_CANONICAL`), only a durable consumer that is deleted with the service. ES drift is reconciled by a one-off end-of-migration reindex job, not a standing reconciler.

### 6.2 message-worker thread-reply handling (the third canonical consumer)

message-worker is the **third** `MESSAGES_CANONICAL` consumer and must process migrated `.created` events — it *is* the Cassandra history writer. A **non-thread** message is persisted and that's all, so migrated events need no special handling there. A **thread reply** additionally runs derived effects, split by owner:

| Effect | Owner | Migrated `.created` |
|---|---|---|
| Persist reply (Cassandra) + durable tcount (`SaveThreadMessage`) | message-worker (history) | **run** |
| `thread_rooms` create + `replyAccounts` + last-message pointer + parent `thread_room_id` stamp | message-worker (`thread_rooms` — no source doc; collections only FK-references it) | **run** |
| `thread_subscriptions` insert/upsert + cross-site outbox | **collections migration** (`company_thread_subscriptions`) | **skip** |
| `hasMention` mark (+ outbox) | **collections migration** | **skip** |
| live tcount badge (`chat.server.broadcast.{site}.thread.tcount`, no header) | nobody — transient notify | **skip** |

**Invariant that makes the skip safe — DO NOT REVERT:** the collections transformer migrates `rocketchat_subscriptions` and `company_thread_subscriptions` **without** a federation-origin filter — every subscription / thread-subscription doc, every origin, is carried. So the thread-subscription state message-worker skips here is owned and reproduced by collections; suppressing it is **not** data loss. Re-deriving it here is actively harmful: `InsertThreadSubscription` would dup-key the unique `(threadRoomId, userAccount)` index (poison-Nak the reply), and `MarkThreadSubscriptionMention` would `$set hasMention=true`, clobbering the source's read state. The tcount badge carries **no** `X-Migration` header, so without this skip broadcast-worker would push it live — defeating the §6 skip for thread replies.

**Rollout coupling:** because message-worker no longer creates thread subscriptions for migrated replies, a migrated thread has no subscription rows until the collections migration runs. This is eventual-consistent but makes the message + collections migrations a **joint** unit — deploy together (or collections-first). message-worker keeps owning `thread_rooms`, which the collections thread-sub FK resolver depends on.

---

## 7. Components & config

**`data-migration/oplog-transformer/`** (flat layout):
- `main.go` — config, connect NATS + source Mongo, create durable consumer, consume loop, graceful shutdown. Holds **`processOne`** — the per-message **Ack/Term/Nak disposition** (nil→Ack, `errPoison`→Term, transient/degraded-recoverable→Nak); this is core behavior and is **unit-tested** with a fake `jetstream.Msg`.
- `handler.go` — route-by-collection; op dispatch (`insert` → §3, `update`/`replace` → §4, `delete` → §4.5); the `Degraded` recovery branch (§4.6); the `errPoison` sentinel (`errors.go`).
- `messagemap.go` — RocketChat doc → `model.Message`; classification: `t==null` → message, `t=="rm"` → soft-delete, any other `t` → system message (skip, §1).
- `sourcelookup.go` — source-Mongo `FindByID` (`primaryPreferred`).
- `canonical.go` — confirmed canonical `.created` publish (awaits pub-ack; sets `X-Migration: live`).
- `historyclient.go` — sync request/reply to the history internal subjects; **classifies the reply** (§5: `OK`→ack, `NotFound`/infra→Nak, non-retryable→Term).
- `metrics.go` — Prometheus instruments (incl. the `degraded`/recovered counter, §9).
- `config.go`, tests, `deploy/`. **No transformer DB. No `//go:generate mockgen` directives** — unit tests use hand-written fakes (`inserter`/`historyClient`/`sourceLookup`), so no dangling mock targets.

**Config (env):** `SITE_ID`, `NATS_URL` (+ `NATS_CREDS_FILE`), `SOURCE_MONGO_URI` (+ `SOURCE_MONGO_USERNAME`/`PASSWORD`), `SOURCE_DB`, `SOURCE_MESSAGE_COLLECTION` (default `rocketchat_message`), `SOURCE_READ_PREFERENCE` (default `primaryPreferred`), `HISTORY_REQUEST_TIMEOUT`, consumer durable name, retry/backoff, `METRICS_ADDR`, `LOG_LEVEL`.

---

## 8. Testing

- **`processOne` disposition (required)** — table-driven over a fake `jetstream.Msg`: handler returns nil → **Ack**; `errPoison`-wrapped → **Term**; transient error → **Nak**. This is the heart of stage-2 and must be unit-tested directly (the integration test calls `handle` directly and bypasses it).
- **Transformer handler unit** (table-driven, mocked publisher + history requester + fake source lookup): insert mapping, edit-vs-soft-delete classification, system-message skip (`t!=null`), `op=delete` → delete-by-id, update lookup-miss / unknown-collection skips, header set, subject routing, ack-only-after-success.
- **Error/poison branches (required, §4 §4.6)** — insert-poison (un-decodable present doc), `documentKeyID` malformed/empty, replace-empty-doc, `handleUpdate` lookup-*error* (vs the covered miss), `applyUpdate` decode-poison.
- **Degraded recovery (§4.6)** — degraded insert → source-lookup recovers → published; degraded insert + lookup-miss → **Nak** (not Term); degraded event with nil `documentKey` → **Nak**.
- **`historyclient` reply classification (§5)** — `OK` → success; `NotFound`/infra reply → error→Nak; non-retryable category → Term; ack-decode-error.
- **`messagemap`** round-trips (RocketChat doc fixtures in `testdata/`, incl. `system.json`).
- **history-service unit** — the two migration handlers: edit/soft-delete semantics, absent-row ⇒ retryable error, already-deleted ⇒ idempotent ack, + canonical republish carrying `X-Migration: live`.
- **broadcast-worker / notification-worker unit** — skip-on-`X-Migration: live`.
- **Integration** (`//go:build integration`, testcontainers source Mongo + NATS): insert event → assert canonical `.created` lands with the header; update event → a fake history responder receives the mapped edit/delete request on the internal subject.
- **Coverage** — meet the §4 80% floor; the azure-pipelines gate runs `-tags=integration` (unit+integration combined). The wiring funcs (`main`, `createConsumerWithRetry`, the Mongo/NATS adapters) are covered by integration; `processOne` + the handler/poison branches must be covered by **unit** tests (above).

---

## 9. Observability

- `log/slog` JSON; correlation field = the oplog `EventID` carried through.
- Metrics: events processed by `collection`+`op`, source-lookup latency + misses, history request RTT, skips (unknown-collection / lookup-miss / system-message), **degraded/recovered** (§4.6, distinct from skips/Terms), Naks/Terms, errors. `/metrics` + `/healthz`.
- **Degraded events** log `DegradedReason` at `WARN` (not the generic poison `Term` log), so a recovered-degraded event is distinguishable in logs and metrics.
- **No `docs/client-api.md` change** — the new handlers are `chat.migration.internal.*` server subjects, not `chat.user.*`.

---

## 10. Open / deferred

> **Post-rebase work (added after the deep review of the rebased branch):**
> - **A — Degraded handling (§4.6): DONE.** Decodes `Degraded`/`DegradedReason`; recovers a degraded insert/replace via source-lookup; a degraded-nil `documentKey` ⇒ Nak (not Term); distinct `oplog_transformer_degraded_recovered_total` metric + `WARN` log. *(Without this, the rebase left the transformer Term-dropping the connector's degraded events — silent loss.)*
> - **B — History reply classification (§5): DONE.** `classifyHistoryReply` runs `errcode.Parse` on the reply: a permanent category (`bad_request`/`unauthenticated`/`forbidden`/`conflict`) ⇒ `errPoison`→Term + `oplog_transformer_history_rejected_total{code}` metric; `NotFound`/infra/`too_many_requests`/unknown code/not-ok/undecodable ⇒ plain error→Nak. Nothing is Term-dropped on uncertainty.
> - **C — Test coverage (§8): DONE.** `processOne` Ack/Term/Nak disposition (C1), `historyclient` reply classification (`TestClassifyHistoryReply`), and the handler poison/error branches (unknown op, update lookup-error→Nak, present-but-corrupt looked-up doc→Term, delete bad/nil `documentKey`, degraded-replace miss→Nak) + canonical pub-ack-error propagation. `handle`/`handleInsert`/`handleUpdate`/`handleDelete`/`applyUpdate`/`resolveDocumentKeyID`/`recoverDegradedDoc` all ~100%.
> - **D — Small fixes: DONE.** edit/delete `messageId` = the event's `documentKey._id` (the resolved key), never the looked-up doc's `_id` (§4.3); migration `.updated`/`.deleted` event `Timestamp` = publish-time, not the historical domain editedAt/deletedAt (§3.2); dropped the dangling `//go:generate mockgen` directives (§7); history request reuses the timeout-scoped context. *(Also fixed: the history-service integration-test publisher fake now implements `PublishMigration`.)*

- **Verify the source deletion mode FIRST (§4.2)** — the delete path's correctness depends on it being soft-delete. Plus the full `messagemap` field list — pinned against the source schema during implementation.
- **Thread/mention/unread aggregates — accepted limitation (no recompute).** Thread reply counts (`tcount`/`tlm`) and mention/unread aggregates are not maintained for migrated messages and are **not** recomputed at cutover (explicit decision). Migrated threads/rooms may show stale/zero counts.
- **Reactions / attachments / pins / system messages** — deferred enrichment stage (§1).
- All non-message collections — later specs.
- **Initial gap catch-up throughput:** inserts use confirmed publish on a single sequential consumer (~1k/s ceiling). Steady-state edits over the month are trivial, but the first catch-up from the history cut to "now" may be a large insert burst — acceptable given the month-long window; the `_id`-keyed concurrency lever is the escape hatch.
- Soak/retry tuning (Nak backoff, `Term` threshold).
- **End-of-migration reindex sweep** — a one-off job to reconcile any ES drift from best-effort canonical publishes (scoped, temporary; not built until cutover).
- **Throughput lever (if ever needed):** bounded `_id`-keyed concurrency on the transformer (parallel across messages, ordered per message) — not built now; the sequential consumer is sufficient for live-sync edit/delete volume.
- **Cleanup PR** — at source retirement, delete `data-migration/` and remove the broadcast/notification header checks + history-service migration handlers.
