# Room Preview Denormalization (Phase 2) — Design

**Date:** 2026-08-05 (consolidated 2026-08-06)
**Status:** Design agreed, not implemented — see [Implementation status](#implementation-status).
**Predecessor:** `2026-08-03-room-preview-read-performance-design.md` (Phase 1 landed in
`#173`/`#175`; this is the concrete Phase 2, pursued by explicit user decision rather
than the metrics gate).

This document describes the design as it now stands. It replaces four append-only
amendments whose chronology had made the current shape hard to read; the reasoning
they carried is preserved in the [Decision log](#decision-log).

## Goal

Make a room-list preview cheap to resolve by memoizing it on the room doc, so the
common case stops walking Cassandra.

The unit of cost being attacked is **the walk**, not the RPC. Resolving a preview
today scans `messages_by_room` backwards from the tail across bucketed partitions,
skipping ineligible messages (system type, deleted, hidden thread replies) up to a
250-message budget — per room, for every room in a list. One NATS round-trip
carrying a batch is cheap by comparison. A room list of 1000 rooms therefore costs
~1000 walks and one RPC; this design removes the walks and keeps the RPC.

## Current state

- `user-service.enrichLastMessage` issues one `rooms.get` per site, including local.
- `history-service.resolvePreview` walks Cassandra per room, fronted by an in-process
  `previewCache`, and `warmBackPreview` persists the result to the room doc — but
  **nothing reads that field back.** The walk runs on every cache miss regardless.
- `broadcast-worker` is the sole writer of `rooms.lastMsgAt/lastMsgId/lastMentionAllAt`,
  buffered through `coalescingStore` and flushed as one unordered `BulkWrite` per
  `LAST_MSG_FLUSH_INTERVAL` (250ms).
- `handleUpdated`/`handleDeleted` receive the post-mutation preview on the canonical
  event (`MessageEvent.PreviewMessage`, computed by `previewAfterMutation`) and relay
  it to clients. That relay is pre-existing and stays.

## Design

history-service is the **sole writer and sole reader** of the stored preview.
broadcast-worker and user-service are untouched by this feature.

### 1. Room document shape

| Field | Type | Notes |
|---|---|---|
| `previewMeta` | subdocument | Plaintext: `messageId`, `sender`, `createdAt`, `mentions`, `visibleTo` |
| `previewCiphertext` | binary | Seals `Content` + `Attachments` |
| `previewNonce` | binary | AES-GCM nonce |
| `previewKeyEpoch` | int | Preview DEK epoch that produced the ciphertext |
| `previewForMsgId` | string | Freshness key — see §3 |
| `previewAsOf` | int64 | Write-ordering watermark (epoch ms) |

All are `json:"-"`: clients receive a preview through the `rooms.get` reply and
message events, never through `Room`. `previewAsOf` stays plaintext because it
carries no body content and the guarded update pipeline must compare it server-side.

### 2. Encryption — seal the body, leave the metadata clear

Encrypt exactly the two fields `atrest.SplitForEncryption` classifies as sensitive,
through the existing `atrest.EncryptedFields`:

- **Sealed:** `Content` → `Msg`; `Attachments` → `Attachments`, marshalled back to
  the `[][]byte` form Cassandra stores natively, so this is a round-trip rather than
  a re-encoding.
- **Clear (`previewMeta`):** `messageId`, `sender`, `createdAt`, `mentions`,
  `visibleTo` — precisely what Cassandra leaves unencrypted. message-worker's
  `toMentionSet` already writes sender and mentions to plaintext columns, so this
  introduces no classification the system does not already make.

`pkg/preview.Seal`/`Open` implement the split. `pkg/atrest` is unchanged.

**Why this matters:** the room doc previously held up to 500 runes of message body
in cleartext, bypassing the encryption that nulls those columns in Cassandra. atrest
distributes trust across three stores — Cassandra holds ciphertext, Mongo holds
Vault-wrapped DEKs, Vault holds the KEK — so read access to any one alone yields
nothing. Plaintext previews broke that: Mongo access alone would have yielded the
latest 500 runes of every room, no key required.

### 3. Freshness — identity, not timestamps

A stored preview is current iff `previewForMsgId == lastMsgId`.

`previewForMsgId` is **the newest message id the resolving walk OBSERVED in
Cassandra**. It is *not* `previewMeta.messageId` — the two differ whenever the newest
message is ineligible and skipped, which is the ordinary case for a room whose last
activity was a system message.

It must be stamped from what the walk saw, never from the `lastMsgId` read out of
Mongo. broadcast-worker and message-worker are unordered consumers of
MESSAGES-CANONICAL, so the room doc can name a message Cassandra does not hold yet;
trusting it would claim freshness for a state never observed, and because the ID
equality would then hold, the error would not self-heal until the next message.

Timestamps were rejected for this: millisecond granularity cannot distinguish two
messages in the same millisecond, and an ineligible newest message makes
`previewAsOf < lastMsgAt` permanently true, leaving the room forever residual.

### 4. Write path

history-service writes the preview in two places, both sealing first:

- **Warm-back**, after a walk resolves a preview on the read path. Best-effort:
  failures log and continue.
- **Post-mutation**, directly in the edit/delete handlers. `previewAfterMutation`
  already computes the preview for the client event; the write is added alongside.
  This replaces routing the outcome through the canonical event for broadcast-worker
  to persist, so `MessageEvent.PreviewGone` is removed.

Both apply `pkg/preview.GuardedSetFields`, an aggregation-pipeline `$set` that lands
only when `asOf >= $ifNull($previewAsOf, 0)` and advances `previewAsOf` with it.
`GuardedClearFields` mirrors it with `$$REMOVE`, still advancing the watermark so a
redelivered older write cannot resurrect a cleared preview. Both iterate one
`previewDocFields` list so they cannot drift apart and strand a fragment — a nonce
and epoch describing a ciphertext that no longer exists.

Mutations need this explicit write because `lastMsgId` changes only on message
creation: editing or deleting the current preview message is invisible to the
freshness check in §3.

### 5. Read path

`resolvePreview` consults the room doc before walking. The preview fields join the
projection of the `GetRoomTimesByIDs` batch read that `resolveRoomMetaHints` already
performs, so the doc read costs no extra round-trip.

```
fresh (previewForMsgId == lastMsgId AND previewKeyEpoch == configured) → Open, return
otherwise                                                              → walk, warm-back, return
```

`previewCache` stays in front. Epoch mismatch and decrypt failure both degrade to a
walk — previews are derived data, so a failure to decrypt is a cache miss, not data
loss — but must log distinctly: an epoch mismatch is expected during rotation, a
same-epoch decrypt failure is not.

Because the reader is history-service, **cross-site rooms benefit equally**: a remote
room's doc lives in the remote site's Mongo, which user-service can never read but
that site's history-service can.

### 6. Key management

**Scope: one preview DEK per site**, id `preview:{siteID}:{epoch}`. Per-room DEKs are
the wrong granularity here — preview reads are driven by membership enumeration
(`subscription.list` touches up to `MAX_SUBSCRIPTION_LIMIT` = 1000 rooms, dormant ones
included), which is exactly the one-shot cold-tail scan `dekCache`'s 2Q algorithm is
built to resist rather than serve. Each dormant room would miss, costing a Mongo read
plus a single-key Vault `Unwrap` (`KeyWrapper.Unwrap` has no batch form), and would
never promote to 2Q's frequent segment, so the next list pays again. One site key is
unwrapped once at process start and cached permanently.

Accepted trade-off: previews lose per-room cryptographic isolation. Compromise of the
preview DEK exposes every room's 500-rune preview in that site. Full message history
keeps its per-room DEKs.

**Provisioning: no human step.** Reuse the existing atrest DEK lifecycle with the
sentinel id in place of a roomID. `GenerateDataKey` mints the DEK inside Vault;
`DEKStore.Upsert` uses `$setOnInsert` so the first writer wins atomically; `createDEK`
re-reads and adopts the winner's key on a lost race; `singleflight` collapses the
per-process stampede. That protocol is already correct for a shared key as written.
The colon in the sentinel cannot collide with a room id (base62 / hex / concatenations
thereof) — pin with a test.

**Storage: separate collection, shared KEK.** The wrapped preview DEK lives in a
dedicated `preview_deks` collection, not `atrest.CollectionName`; wiring is
`db.Collection("preview_deks")` at the call site with `NewMongoDEKStore` unchanged.
Both are wrapped by the existing `chat-kek`. The split collection is operational
rather than cryptographic: losing a preview DEK is a cache miss, losing a room DEK is
permanent history loss, and co-locating them invites identical backup and retention
treatment for records that deserve different handling.

**Rotation: lazy, no migration.** `PREVIEW_KEY_EPOCH` (int, `envDefault:"1"`) is set
identically across history-service instances and selects the sentinel id. Rotation is
an ops action — bump the value, redeploy — and process restart is what invalidates the
cached DEK, which deliberately avoids inventing an invalidation protocol. Readers on a
different epoch treat the preview as absent and re-resolve, so no reader ever needs a
retired DEK. During a rolling deploy both epochs are live and previews churn
self-correctingly, bounded by the rollout window; rotate at low traffic.

**Nonce budget.** `atrest.Encrypt` uses random 96-bit GCM nonces, so NIST SP 800-38D's
2³²-invocations-per-key guidance applies. A per-room DEK never approaches it because
usage is partitioned; one site-wide key reaches it sooner. Collision probability is
`~q²/2⁹⁷` and degrades quadratically (≈1.2e-10 at 2³², ≈7.6e-6 at 2⁴⁰) — a margin to
preserve, not a cliff. Rotation handles it, which is why the key is not bucketed.

A GCM nonce collision is silent: both messages decrypt and authenticate normally. It
leaks `P₁ ⊕ P₂` — recoverable, since the payload is structured and an attacker can
plant a known preview by posting in any room they belong to — and permits recovery of
the GHASH subkey `H = E_K(0¹²⁸)`, extending forgery to every nonce under that key.
**The preview DEK must therefore never be reused for message bodies.**

## Known hazards

- **Best-effort writes can be lost.** Warm-back and post-mutation writes are
  warn-and-continue with the event acked regardless, so a Mongo failure leaves a stale
  preview until the next message changes `lastMsgId`.
- **`visibleTo` is unresolved.** One preview is stored per room but consumed per
  member. When the `visibleTo` write path lands, a partially-visible last message will
  be served to every member, and no freshness check can detect it — the value is not
  stale, merely not per-user. Decide before that path ships.
- **Cold-start batch cap.** `maxRoomsGetBatch` is 100 while `MAX_SUBSCRIPTION_LIMIT`
  is 1000, so a large list can exceed it. Pre-existing on `main` and tracked
  separately, but a cold cache makes it easier to hit.

## Out of scope

- Chunking `rooms.get` at the 100-room batch limit (pre-existing; its own PR).
- Populating `PreviewMessage.VisibleTo`'s write path.
- `forwardSource` on the preview (TODO #106).

## Decision log

Alternatives considered and rejected, with the reason each failed.

**Where the preview is read.** Four shapes were compared: status quo; eager write by
broadcast-worker with user-service reading the doc; warm-back-only with user-service
reading; and the chosen shape, history-service reading its own doc.

Reading in user-service was rejected because it puts decryption in the front-line
service, which would need Vault unwrap capability on the same `chat-kek` that protects
room DEKs — making a Mongo collection grant the only barrier against full history
disclosure from a user-service compromise. It also cannot help cross-site rooms, whose
docs are remote. Eager write additionally requires new atrest wiring in
broadcast-worker, on its highest-throughput path.

The cost of the chosen shape is that `rooms.get` still fires on every list, so the
original "zero RPCs for a local list" goal is not met — the RPC becomes cheap rather
than absent. Given the walk dominates, that was judged the right trade. Eager write
remains the answer if a NATS round-trip ever cannot fit the latency budget.

**Encrypting the preview as one opaque blob** was specified first and proved
unbuildable: `atrest.Cipher.Encrypt` accepts only the fixed, message-shaped
`EncryptedFields`, so a single blob would mean adding a generic seal to a shared
security package for one caller. The hybrid in §2 uses atrest as designed.

**A separate `room_previews` collection** adds a second read on the read path and a
second write target, with no isolation benefit since the room doc is already written
per flush.

**Cache-only** (longer Phase 1b TTL, or Valkey) still costs an RPC per list, is cold
after restarts, and is per-instance.

**Per-subscription fan-out** — writing the preview onto every member's subscription —
was never considered: O(members) writes per message. Everything here is per-room, so a
10,000-member room costs the same single write as a two-person DM.

## Implementation status

**Not implemented.** The branch still carries the superseded shape: broadcast-worker
writes previews, user-service reads them, and nothing is encrypted. A green CI run
does not mean the code matches this document.

Remaining work, in order:

1. Revert `broadcast-worker/` and `user-service/` to the **merge-base** (not
   `origin/main`, which has moved — reverting to it shows main's own changes as ours).
2. Reshape `pkg/model`: the §1 room fields, add `PreviewMeta`, drop
   `MessageEvent.PreviewGone`.
3. Rebuild `pkg/preview` guards around `Sealed`; add `Seal`/`Open` **with tests** —
   round-trip, wrong-key failure, attachment-marshal error.
4. Wire the preview DEK in history-service: `PREVIEW_KEY_EPOCH`, the `preview_deks`
   collection off the existing Vault wrapper, cipher threaded into `NewRoomRepo`.
5. Make history-service the sole writer (§4).
6. Add the doc read to `resolvePreview` (§5).
7. Verify: `make generate`, `lint`, `test`, `sast`, integration for history-service and
   `pkg/model`; confirm the 80% coverage floor.
