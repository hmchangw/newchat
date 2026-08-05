# Room Preview Denormalization (Phase 2) — Design

**Date:** 2026-08-05
**Status:** Draft — awaiting user review
**Predecessor:** `2026-08-03-room-preview-read-performance-design.md` (Phase 1 landed in
#173/#175; this spec is the concrete Phase 2, pursued by explicit user decision rather
than the metrics gate).

## Goal

1. **One single query:** a local `subscription.list` serves room metadata **and**
   `previewMessage` entirely from the existing subscriptions→rooms `$lookup`
   aggregation — no `rooms.get` RPC to history-service for local rooms.
2. **Reduce write frequency:** the **message hot path** must add **zero new write
   operations** — a created message's preview rides the per-room coalesced `$set` that
   broadcast-worker already performs, so a hot room still writes its room doc once per
   flush interval, not once per message. The rare paths do add writes, bounded and
   off the hot path: one guarded update per **edit/delete** (§3), and one best-effort
   **warm-back** per dormant room the read path resolves by walking, which stops once
   the room is warmed (§5).

## Current state (post-Phase 1)

- `user-service.enrichLastMessage` (`user-service/service/subscriptions.go:331`) issues
  one `rooms.get` per site **including local**; history-service resolves each preview
  with a single-query walk (Phase 1a) fronted by a short-TTL LRU cache (Phase 1b).
- `broadcast-worker` is the single writer of `rooms.lastMsgAt/lastMsgId/lastMentionAllAt`,
  buffered through `coalescingStore` (`broadcast-worker/coalescer.go`) and flushed via
  one unordered `BulkWrite` per interval (`store_mongo.go:104`).
- `handleUpdated`/`handleDeleted` already receive the post-mutation preview on the
  canonical event (`MessageEvent.PreviewMessage`, computed by history-service
  `previewAfterMutation`) and relay it to clients (`broadcast-worker/handler.go:717,737`)
  — but do not persist it.
- `model.Room` has no persisted preview; `SubscriptionRoom.PreviewMessage` is wire-only
  (`bson:"-"`).

## Approaches considered

- **A. Room-doc denormalization written by broadcast-worker (chosen).** O(1) per
  message, piggybacks an existing write, already the approved Phase 2 shape.
- **B. Separate `room_previews` collection.** Second query on the read path (defeats
  goal 1) and a second write target; no isolation benefit since the room doc is already
  written per flush. Rejected.
- **C. Cache-only (extend Phase 1b TTL / move to Valkey).** Still an RPC per list, cold
  after restarts, per-instance; cannot reach "one query". Rejected.
- **Anti-shape to avoid:** per-subscription fan-out (O(members) writes/message) — never
  considered; everything below is per-room.

## Design

### 1. Data model

- `pkg/model/room.go` `Room` gains:
  ```go
  PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"previewMessage,omitempty"`
  // PreviewAsOf is the ordering watermark for previewMessage writes: the createdAt
  // (epoch ms) of the message event that produced the stored preview. Guard key only;
  // never serialized to clients.
  PreviewAsOf int64 `json:"-" bson:"previewAsOf,omitempty"`
  ```
  (`PreviewAsOf` holds the canonical event `Timestamp` of the write that produced the
  stored preview — see §2.)
- `model.PreviewMessage` (and any embedded types lacking them: `cassandra.Attachment`,
  reused `Participant` fields) gain **camelCase `bson` tags** so the stored shape
  matches the JSON shape. JSON output is unchanged.
- `pkg/model` `roundTrip` covers the new field; `TestPreviewMessageOmitempty` extends to
  `Room`.

### 2. Ordering watermark (why not `previewMessage.createdAt`)

Guarding on the stored preview's own `createdAt` breaks on **delete**: deleting the
newest message legitimately replaces the preview with an *older* message. Guarding on
the *mutated message's* `createdAt` breaks on **sequential deletes** (delete m3 →
preview=m2 with watermark t3; deleting m2 then carries t2 < t3 and is wrongly
rejected). Instead the watermark is the **canonical event's `Timestamp`** (publish-time
epoch ms, already on every `MessageEvent`): create, edit, and delete writes all carry
`asOf = evt.Timestamp`, which is monotonically assigned along a room's mutation stream
even when broadcast-worker's concurrent workers process the events out of order.
Warm-back (no event) conservatively uses the resolved preview's `createdAt` — always ≤
any event timestamp that observed that message, so a warm-back can fill an empty doc
but never regress a newer event-driven write. All preview writes go through a Mongo
**aggregation-pipeline update** that applies `previewMessage`+`previewAsOf` only when
`incoming asOf ≥ stored previewAsOf` (ties: last writer wins). This makes
create/edit/delete/warm-back writers and JetStream redeliveries commutative. Known residual race (accepted): a create-event
redelivery arriving after that same message's delete can briefly restore a deleted
preview until the next room activity; today's read-time walk has no such window, but
the window is bounded by the JetStream ack window and self-heals.

### 3. Write path — broadcast-worker (zero new ops)

- **Shared builder:** extract history-service's `toPreviewMessage` +
  `botAwareDisplayName` into `pkg/preview`, used by history-service (walk path) and
  broadcast-worker (create path) so previews are identical regardless of writer.
- **Create** (`handleCreated`): for an **eligible** message (non-system; thread replies
  are already diverted to `handleThreadCreated` and never touch the room preview),
  build the preview from the already-constructed `clientMsg` and hand it to
  `UpdateRoomLastMessage` alongside the existing args. `coalescingStore` buffers it:
  `roomLastMsgUpdate` gains `preview *model.PreviewMessage` + `previewAsOf`, merged by
  max-`asOf` **independently** of `msgID/at` (a system message advances `lastMsgAt` but
  leaves the buffered preview intact). `BulkUpdateRoomLastMessage` folds
  `previewMessage`/`previewAsOf` into the same per-room `$set`, now expressed as a
  pipeline update with the watermark guard. **Write frequency is unchanged from
  today** — same op count, same coalescing cadence, slightly larger payload.
- **Edit / Delete** (`handleUpdated`/`handleDeleted`): when `evt.PreviewMessage != nil`,
  persist it with a direct guarded update (asOf = `evt.Timestamp`).
  These are rare, non-hot-path ops; `nil` never overwrites. Best-effort: a failed
  preview persist logs and continues (clients still got the relayed preview; the store
  self-heals on next activity or warm-back).
- **Snippet truncation:** `pkg/preview` caps `Content` at **500 runes** (constant in
  `pkg/preview`). Applied in the shared builder, so the denormalized doc, `rooms.get`,
  and the edit/delete relay all serve the same truncated shape — no path-dependent wire
  inconsistency. This is the one client-visible change (see §7).

### 4. Read path — user-service

- The subscription `$lookup` baseline (`user-service/mongorepo/subscriptions.go`)
  projects `previewMessage` (this pipeline only — other room-doc consumers keep their
  precise projections).
- `buildLocalRoom` fills `room.PreviewMessage` from the baseline.
- `enrichLastMessage` sends the **local** site's `rooms.get` only the residual roomIDs
  that lack a baseline preview (dormant rooms not yet warmed); rooms with a preview
  skip the RPC entirely. When no residual remains, no local RPC is made at all — the
  local list is one Mongo aggregation. Cross-site sites keep the existing per-site
  fan-out (out of scope, as in the predecessor design).
- Staleness bound: the preview can lag `lastMsgAt` by up to one coalescer flush
  interval — the same bound `lastMsgAt` itself already has; clients receive live
  messages in real time, so this is acceptable (unchanged trade-off).

### 5. Rollout, backfill, flag

- **Lazy warm-back (no migration job):** when history-service `RoomsGet` resolves a
  preview for a room by walking, it best-effort writes it back to the room doc through
  the same watermark-guarded pipeline update (`internal/mongorepo/room.go` already
  holds the rooms collection). Dormant rooms heal on their first list load, so
  residual `rooms.get` traffic decays to zero. Failures log and never fail the RPC.
- **Flag:** one env flag on **user-service only** —
  `SUBSCRIPTION_PREVIEW_FROM_DOC` (`envDefault:"true"`) — gating the read-path skip.
  Flipping it off restores today's behavior instantly (writes are harmless when
  ignored). The write paths ship unflagged: they add no ops and the guard makes them
  idempotent. *(Deviation from the predecessor's "ship dark" stance, justified by the
  explicit user decision to denormalize now; default-on, instant rollback.)*
- Phase 1b's preview read-cache stays as-is: it now serves only residual/cross-site
  `rooms.get` calls, which shrink over time. No invalidation coupling.

### 6. Testing (TDD, per CLAUDE.md)

- `pkg/preview`: builder parity with history-service's current output (golden cases:
  bot sender display name, mentions, attachments, truncation boundary at 500 runes,
  multi-byte runes).
- `broadcast-worker`: coalescer merges preview by max-asOf independently of
  `lastMsgAt` (system-message case); bulk flush writes guarded pipeline update;
  integration: older redelivery cannot regress a newer stored preview; delete-path
  write replaces with an older-createdAt preview (watermark passes); **sequential
  deletes** (delete preview, then delete its replacement) both land; system message
  advances `lastMsgAt` without touching `previewMessage`.
- `user-service`: local rooms with baseline preview skip the local `rooms.get`;
  residual rooms still resolve via RPC; flag off restores full RPC path; degraded RPC
  leaves preview nil.
- `history-service`: warm-back writes guarded and best-effort (write failure doesn't
  fail `RoomsGet`).
- `pkg/model`: roundTrip + omitempty coverage for `Room.PreviewMessage`.
- Coverage ≥80% on touched packages, ≥90% target for handlers/stores; `-race`.

### 7. Documentation

- `docs/client-api.md` (+ derived views if their sections are touched):
  `previewMessage.content` is now a snippet capped at 500 runes (was full body) —
  the only wire-visible change; confirm with frontend before shipping.
- Update `model.PreviewMessage` doc comment (no longer "full message body").

## Out of scope

- Cross-site preview denormalization (`GetRoomsInfo` extension) — later follow-up.
- Proactive backfill migration — lazy warm-back covers it.
- Thread-room previews — thread fan-out paths never touch the room preview.

## Decisions taken without interactive input (review these)

1. **Flag default-on, read-side only** (`SUBSCRIPTION_PREVIEW_FROM_DOC`,
   `envDefault:"true"`), write path unflagged — vs the predecessor's dark launch.
2. **Truncation cap 500 runes**, applied in the shared builder — truncates `rooms.get`
   / cross-site previews too, for path-consistent wire behavior.
3. **`previewAsOf` watermark keyed on the canonical event `Timestamp`** instead of the
   predecessor's `previewMessage.createdAt` guard (which rejects legitimate delete-path
   replacements — including sequential deletes).
4. **Warm-back lives in history-service** (it holds the walk result and the rooms
   collection handle), best-effort, guarded.

## Amendment (2026-08-05, post-review)

**Supersedes** §3's "`nil` never overwrites" wording for the edit/delete path.

### Defect

Deleting a room's *last* eligible message left the deleted message's preview on the
Mongo room doc forever. `history-service.previewAfterMutation` returned `nil` when the
walk found no eligible survivor, `broadcast-worker.persistPostMutationPreview` skipped
`nil` ("nil never overwrites"), so the stored preview was never cleared. `user-service`
then served the deleted content from the doc *and* skipped the `rooms.get` fallback (a
room with a preview is not residual), contradicting `docs/client-api/events.md`'s promise
that `previewMessage` is omitted when the deleted message was the room's last eligible one.

The root cause is that a single `nil` conflated two different outcomes: **"definitively
none"** (safe to clear) and **"unknown"** (a degraded walk — clearing would lose data).

### Fix — three-state walk + explicit clear

1. **`history-service` three-state walk.** `roomLastPreviewMessageState` returns a
   `previewResolveState`: `previewFound` / `previewEmpty` (walk completed, no eligible
   message) / `previewDegraded` (a read failed, or the `lastMsgWalkMaxScan` budget was
   exhausted — a survivor may exist beyond it). `roomLastPreviewMessage` stays as a
   two-value wrapper so every other caller is untouched. Mapping: room-times error →
   degraded; `GetMessagesBefore` error → degraded; empty page → **empty**; whole page
   ineligible with `!HasNext` → **empty**; budget exhausted → **degraded**.
2. **Canonical marker.** `model.MessageEvent.PreviewGone bool` (`bson:"-"`, canonical
   stream only) is set by `previewAfterMutation` when and only when the walk completed
   empty. `PreviewMessage != nil` ⇒ found; `nil` + `PreviewGone` ⇒ empty; `nil` +
   `!PreviewGone` ⇒ degraded, change nothing. Hidden thread replies keep skipping the
   walk entirely, so they set neither.
3. **Guarded clear.** `pkg/preview.GuardedClearFields(asOf)` mirrors `GuardedSetFields`:
   `$$REMOVE`s `previewMessage` when `asOf >= $ifNull($previewAsOf, 0)` while still
   advancing `previewAsOf`, so a redelivered older create cannot resurrect the cleared
   preview.
4. **`broadcast-worker` clear write.** `Store.ClearRoomPreviewMessage(ctx, roomID, asOf)`
   (mongo impl = pipeline update with `GuardedClearFields`).
   `persistPostMutationPreview` becomes three-way: Set on preview, Clear on `PreviewGone`,
   no-op otherwise — all still best-effort warn-and-continue. Thread branches unchanged.
5. **`user-service` unchanged.** A cleared doc makes the room residual again, so the
   `rooms.get` fallback runs, its own walk skips the deleted message, and `previewMessage`
   is omitted — exactly what the client docs already promise. No wire-schema change, so
   `docs/client-api.md` and its derived views need no edit.
