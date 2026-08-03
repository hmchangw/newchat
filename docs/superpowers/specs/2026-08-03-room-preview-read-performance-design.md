# Room-list Preview Read Performance — Design

**Date:** 2026-08-03
**Status:** Approved (design); implementation pending
**Symptom (measured by the user):** `subscription.list` on the local site is slow.

## Problem

`user-service.ListSubscriptions` → `enrichWithRoomInfoAndLastMsg(withLastMsg=true)`
→ `enrichLastMessage` issues one `rooms.get` per site **including the local site**
(`user-service/service/subscriptions.go:327`). Local room metadata otherwise comes
entirely from the Mongo `$lookup` baseline with no RPC; the **last message is the
sole reason a local list touches history-service at all**.

Inside history-service, `RoomsGet` (`history-service/internal/service/rooms.go:26`)
fans out to ≤16 concurrent goroutines, each calling `roomLastPreviewMessage`, which
resolves room times (Mongo, cached) and then **walks `messages_by_room` in Cassandra**
to find the last *eligible* message (non-deleted, non-system).

### The cost is not uniform — and not what "walk" first suggests

With an accurate `lastMsgAt`, the walk starts in the right partition and the **first**
query already returns the newest message. The slowness comes from two narrower causes:

1. **Over-fetch → over-walk.** `roomLastPreviewMessage` requests a **50-row page**
   (`lastMsgWalkPageSize = 50`) but uses only the first eligible row. `fillPage`
   (`walker.go:134`) loops `for len(out) < pageSize`, so when the newest bucket returns
   fewer than 50 rows it **steps into older (often empty) buckets** issuing one query
   each, trying to reach 50. A busy room (≥50 msgs in the newest 72h bucket) costs **1
   query**; a **quiet/sparse room** (DM, low-traffic channel, dormant room) walks many
   buckets for an answer that was in row 1 of query 1.
2. **Ineligible tail.** The preview must skip deleted/system messages; the 50-row page
   is sized to batch-skip such a run (up to `lastMsgWalkMaxPages=5` × 50 = 250 before
   giving up). A long trailing run of deleted/system messages extends the walk.

**Why 50 at all:** the page size exists only because eligibility is computed by
*scanning* — the newest message by time (`lastMsgId`) may itself be a system event or
deleted, so the reader scans back to the newest message that represents room content.

### Where the pain actually is

- Not every room — **busy rooms are already ~1 query.** The cost concentrates in
  **quiet/sparse rooms** (over-walk) and **deleted-tail rooms**.
- **The aggregate.** Every list load does N × (1 Mongo room-times read + ≥1 Cassandra
  query), N = the whole page, **recomputed on every load** with no caching of the
  resolved preview.

## Constraint

A prior decision avoided **denormalizing** the preview out of concern for **write
traffic**. That concern was **assumed, not measured**. This design therefore leads with
zero-write read-side wins + instrumentation, and treats denormalization as a
flag-gated, measurement-triggered escalation — not the default.

Note for context: the heavy-write shape to avoid is **per-subscription fan-out**
(O(members) writes/message). Denormalizing onto the **single room doc** is O(1) and
piggybacks the `lastMsgAt` write broadcast-worker already performs — a different, far
cheaper shape. Phase 2 below only ever considers the room-doc shape.

---

## Phase 1 — zero-write read-side wins (do first)

No message-hot-path writes. Directly attacks the two cost drivers above.

### 1a. Fix the preview-walk over-fetch

`roomLastPreviewMessage` needs the **first eligible** message, not a 50-row page.
Fetch a **small first page and escalate** only when the newest rows are ineligible:

- Start the walk at page size 1. If the newest row is eligible → **single query**,
  done. This collapses the accurate-`lastMsgAt` + eligible-newest case (the common
  case, including all sparse rooms) to one Cassandra round-trip, eliminating the
  older-empty-bucket over-walk.
- On a fully-ineligible page, **grow the page geometrically** (e.g. 1 → 8 → 64) and
  continue, preserving the existing ≤250-message ineligible-skip budget without
  1-row-at-a-time round-trips for deep deleted tails.
- Behavior is unchanged (same eligible message returned, same 250 cap); only the
  round-trip count for quiet/sparse rooms drops.

Scope: `history-service/internal/service/rooms.go` (walk driver) — the change is local
to the preview path; `LoadHistory` et al. keep their own page sizes.

### 1b. Short-TTL read cache of the resolved preview

Add a `PreviewMessage`-per-room cache to history-service's existing `readcache`
(LRU+TTL+singleflight, already fronting `GetRoomTimes` / `GetMinUserLastSeenAt`).
`RoomsGet` serves cached previews and only walks on miss; singleflight dedups
concurrent misses for the same room.

- **Zero write-path impact** — purely a read cache.
- TTL bounds staleness (the newest message may lag by up to the TTL); acceptable
  because the list also carries `lastMsgAt` and clients receive new messages in real
  time. Pair with a short TTL (align with the existing room-times TTL).
- Invalidation is TTL-only (per-instance), matching the existing readcache precedent.

### 1c. Instrument (prerequisite for deciding on Phase 2)

The service emits no DB-level signal today (noted in the 2026-05-28 perf spec). Add:
- `rooms.get` per-room latency + Cassandra queries-per-preview (buckets walked),
- preview cache hit/miss,
- `rooms.get` batch size (N) distribution.

This turns the Phase-2 decision into an evidence-based one and quantifies whether the
assumed write concern is even relevant.

**Phase 1 exit criteria:** if the walk fix + cache bring `subscription.list` latency
and history-service load within target, **stop here** — no write-path change.

---

## Phase 2 — room-doc denormalization (flag-gated, only if measured need)

Pursue only if Phase 1 metrics show the read-side is insufficient (e.g. cache hit rate
too low because rooms churn faster than TTL, or first-load latency still dominates).

### Shape

Denormalize the last **eligible** preview onto the Mongo `rooms` doc, written by
**broadcast-worker** — already the single writer of `rooms.lastMsgAt`/`lastMsgId`
(`store_mongo.go:78`), already handling create/edit/delete (`handler.go:94`), already
building the enriched `clientMsg` on create (`handler.go:195`). history-service already
computes the post-mutation preview via `previewAfterMutation` and ships it on the
canonical edit/delete events (`messages.go:647`). So the write is wiring, not new
computation.

**Phase 2 extraction point:** the `PreviewMessage` construction logic
(`toPreviewMessage` + `botAwareDisplayName`, currently in history-service
`rooms.go`/`reactions.go`) is the one piece of logic Phase 1 and Phase 2 share.
Phase 2 extracts it to a shared package (e.g. `pkg/preview`) so broadcast-worker
builds byte-identical previews on the create path. Phase 1 deliberately does NOT
pre-extract it (YAGNI — Phase 2 is metrics-gated and may not land). Otherwise the
phases are one-directional: Phase 2 *calls* Phase 1's `roomLastPreviewMessage`
(warm-back + delete-path) but does not edit it, and the Phase 1b cache coexists
without invalidation because user-service selects the denormalized field vs
`rooms.get` per room.

Add to `pkg/model/room.go` `Room`:
```go
PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"previewMessage,omitempty"`
```

### Write-cost mitigations (address the original concern head-on)

- **No new write op** — fold `previewMessage` into the existing per-message
  `UpdateRoomLastMessage`/`BulkUpdateRoomLastMessage` `$set`. The room doc is already
  written on every message.
- **Coalescer** (`broadcast-worker/coalescer.go`) already collapses same-room updates in
  a window, so a hot room writes the preview once per flush, not once per message —
  write amplification is sublinear exactly under heavy traffic.
- **Truncated snippet** — store a capped preview `content` (a few hundred bytes) rather
  than the full ≤20KB body; the frontend truncates for display anyway. Keeps the room
  doc small for both the write and the `$lookup` read. (Tradeoff: the list's `content`
  becomes a snippet, not the full body — a minor wire-behavior change to confirm.)
- **Precise projections** — only the subscription `$lookup` baseline reads
  `previewMessage`; other room-doc consumers keep their existing projections, so no
  read bloat elsewhere (per CLAUDE.md's projection rule).

### Write path

Ordering guard on all writes via a Mongo **aggregation-pipeline update**: set
`previewMessage` only when the incoming message time ≥ stored `previewMessage.createdAt`,
making every writer commutative (redeliveries / warm-backs cannot regress a newer
preview).

- **Create** (`handleCreated`): write the preview from `clientMsg`, **eligible messages
  only** (system messages advance `lastMsgAt` but leave the preview; hidden thread
  replies are already routed to `handleThreadCreated`).
- **Edit / Delete** (`handleUpdated`/`handleDeleted`): persist `evt.PreviewMessage`
  (already computed upstream); a `nil` value leaves the stored preview untouched.

### Read path

`buildLocalRoom` (`subscriptions.go:384`) fills `room.PreviewMessage` from the baseline;
`enrichLastMessage` **skips the local-site `rooms.get`** for rooms that carry a preview,
falling back to `rooms.get` only for the residual (self-healing during rollout).

### Rollout

- **Backfill:** lazy / self-healing — a `rooms.get` fallback warms the room doc via the
  same guarded pipeline write (a narrow, idempotent, order-safe history-service →
  `rooms` write; broadcast-worker stays the authoritative writer). No migration job.
- **Cross-site:** unchanged — cross-site rooms keep the per-site `rooms.get` fan-out
  (a `GetRoomsInfo` extension is a later follow-up). This design targets the local-site
  symptom.
- **Flag:** the read-path skip and the write are behind a config flag so Phase 2 ships
  dark and enables on evidence.

## Testing

- **Phase 1a** (`rooms_test.go` + integration): eligible-newest room → single Cassandra
  query; sparse room no longer over-walks; deleted-tail still resolves within the 250
  budget; identical preview to today.
- **Phase 1b:** cache hit skips the walk; singleflight dedups concurrent misses; TTL
  expiry re-walks; miss/hit metrics recorded.
- **Phase 2** (if pursued): create writes an eligible/truncated preview; system message
  leaves preview; edit/delete persist `evt.PreviewMessage`; ordering guard rejects an
  older write; coalescer carries the newest; user-service skips `rooms.get` when a
  baseline preview exists and falls back otherwise; warm-back is guarded + best-effort;
  `pkg/model` `roundTrip` covers `Room.PreviewMessage`.
- Coverage ≥ 80% on touched packages; `-race` throughout.

## Out of scope / follow-ups

- Cross-site preview denormalization via `GetRoomsInfo` (room-service).
- Proactive backfill migration (lazy warm-back covers correctness if Phase 2 ships).
- No client-facing request/response schema change in Phase 1; Phase 2's truncated
  `content` is the only wire-visible change and is confirmed before shipping.
