# Room Preview Denormalization (Phase 2) — Design

**Date:** 2026-08-05
**Status:** Draft — awaiting user review
**Predecessor:** `2026-08-03-room-preview-read-performance-design.md` (Phase 1 landed
in `#173`/`#175`; this spec is the concrete Phase 2, pursued by explicit user decision
rather than the metrics gate).

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

## Amendment 2 (2026-08-06, security review)

**Supersedes** §1's plaintext `previewMessage` subdocument on the room doc.

**Status: design decision only — not yet implemented.** As of this commit the branch
still writes the plaintext shape: `pkg/preview.GuardedSetFields` builds
`previewMessage`, `ClearRoomPreviewMessage` `$$REMOVE`s it, and
`model.PreviewMessage` still carries cleartext `Content` and `Attachments`. That
divergence is expected and must be closed before merge. A green CI run on this
branch does not mean the code matches this spec.

### Defect

`Room.PreviewMessage` persists `Content` (≤500 runes of the message body) and
`Attachments` to Mongo in cleartext. `atrest.SplitForEncryption` classifies exactly
those two fields — plus `Card`/`CardAction` — as sensitive, and
`StripEncryptedFields` nulls them in Cassandra so plaintext never lands there.
The denormalization therefore writes, in the clear, the field categories the
project's own threat model requires to be encrypted at rest.

This is new exposure, not inherited: on `main` the room doc carries only
`lastMsgAt` and `lastMsgId`. Previews were resolved at read time by walking
Cassandra and decrypting, so Mongo held zero message body.

What breaks is the trust split. atrest distributes trust across three stores —
Cassandra holds ciphertext, Mongo holds Vault-wrapped DEKs, Vault holds the KEK —
so read access to any one alone yields nothing. After this change, Mongo read
access alone yields the latest 500 runes of every room, no key required. The
plaintext lands in the same store as the wrapped keys that were supposed to make
that store safe to lose. `ATREST_ENABLED=true` is the deployed posture.

### Fix — encrypt the preview under a dedicated preview DEK

1. **Room doc shape.** `previewMessage` (subdocument) is replaced by
   `previewCiphertext []byte`, `previewNonce []byte`, and `previewKeyEpoch int`.
   `previewAsOf` stays plaintext — it is a timestamp, carries no body content, and
   the guarded-write pipeline must compare it server-side.
   `GuardedSetFields`/`GuardedClearFields` set and `$$REMOVE` the new triple; the
   watermark semantics of Amendment 1 are unchanged.

2. **Key scope — one preview DEK per site, not per room.** The per-room DEK is the
   wrong granularity for this data. Preview reads are driven by membership
   enumeration (`subscription.list` touches up to `MAX_SUBSCRIPTION_LIMIT` = 1000
   rooms, dormant ones included), which is precisely the one-shot cold-tail scan
   `dekCache`'s 2Q algorithm is designed to *resist*, not serve. Each dormant room
   would miss, costing a Mongo read plus a single-key Vault `Unwrap`
   (`KeyWrapper.Unwrap` has no batch form), and would never promote to 2Q's
   frequent segment, so the next list pays again. A single per-site key is
   unwrapped once at process start and cached permanently.

   Trade-off accepted: previews lose per-room cryptographic isolation. Compromise
   of the preview DEK exposes every room's 500-rune preview in that site. Full
   message history keeps its per-room DEKs and is unaffected.

3. **Provisioning — no human step.** Reuse the existing atrest DEK lifecycle with
   the sentinel id `preview:{siteID}:{epoch}` in place of a roomID.
   `KeyWrapper.GenerateDataKey` mints the DEK inside Vault (plaintext never
   originates outside the KEK provider); `DEKStore.Upsert` uses `$setOnInsert` so
   the first writer wins atomically; `createDEK` re-reads and adopts the winner's
   key on a lost race; `singleflight` collapses the per-process stampede. That
   protocol is already correct for a globally shared key as written — no change to
   `pkg/atrest`. The colon in the sentinel cannot collide with a room id (base62 /
   hex / concatenations thereof); pin this with a test.

4. **Storage — separate collection, shared KEK.** The wrapped preview DEK lives in
   a dedicated `preview_deks` collection, NOT `atrest.CollectionName`. Wiring is
   `db.Collection("preview_deks")` at the call site; `NewMongoDEKStore` is unchanged.
   Both collections are wrapped by the existing `chat-kek` — one KEK, decided
   knowingly (see Residual risk).

   Rationale for the split collection is operational rather than cryptographic:
   the two key classes have opposite criticality. Losing a preview DEK is a cache
   miss (previews are derived — see 6). Losing a room DEK is permanent,
   unrecoverable history loss. Co-locating them invites identical backup, retention,
   and restore-test treatment for records that deserve different handling.

5. **Nonce budget.** `atrest.Encrypt` uses random 96-bit GCM nonces, so NIST
   SP 800-38D's 2^32-invocations-per-key guidance applies. A per-room DEK never
   approaches it because usage is naturally partitioned; one site-wide key
   encrypting every preview write does so far sooner. Collision probability is
   `~q²/2^97` and degrades quadratically (≈1.2e-10 at 2^32, ≈7.6e-6 at 2^40), so
   this is a margin to preserve, not a cliff. It is handled by rotation (6) rather
   than by bucketing the key, since rotation here is free.

   A GCM nonce collision is silent — both messages decrypt and authenticate
   normally. It leaks `P₁ ⊕ P₂` (recoverable: the payload is JSON with known
   structure, and an attacker can plant a known preview by posting in any room they
   belong to), and permits recovery of the GHASH subkey `H = E_K(0¹²⁸)`, which
   extends forgery to every nonce under that key. This is why the preview DEK must
   never be reused for message bodies.

6. **Rotation — lazy, no migration.** Previews are derived data, always
   reconstructible from Cassandra, so a decryption failure is a cache miss rather
   than data loss. No re-encryption pass, no backfill.

   *Epoch is configuration, not discovered state.* `PREVIEW_KEY_EPOCH` (int,
   `envDefault:"1"`) is set identically across broadcast-worker, history-service and
   user-service, and selects the sentinel id `preview:{siteID}:{epoch}`. Rotation is
   therefore an ops action — bump the value, redeploy — and process restart is what
   invalidates the cached DEK. This deliberately avoids inventing an invalidation
   protocol: caching the key permanently is safe precisely because the only thing
   that changes it also restarts the process holding it.

   *Writers* encrypt under their configured epoch. *Readers* compare the doc's
   `previewKeyEpoch` against their configured epoch; on mismatch they treat the
   preview as absent, which makes the room residual, so the `rooms.get` walk resolves
   it and warm-back rewrites it under the current epoch. No reader ever needs a
   retired DEK, so an old `preview_deks` row may be deleted once no doc references
   its epoch.

   *During a rolling deploy* both epochs are live and previews churn: a pod on epoch
   N treats an N+1 doc as absent and rewrites it as N, and vice versa. This is
   self-correcting and bounded by the rollout window, but it costs extra walks — so
   rotate during low traffic, and no more often than the nonce budget in 5 requires.

7. **Access control.** user-service receives Mongo read on `preview_deks` only and
   MUST NOT hold `find` on `atrest.CollectionName`. Because a single KEK is shared,
   this grant is the sole boundary preventing a compromised user-service from
   unwrapping room DEKs, so it is asserted at startup rather than assumed.

   The assertion **fails closed**. user-service issues a projected, non-data-bearing
   `find` against `atrest.CollectionName` (`limit 1`, projection `{_id: 0}`) and
   startup proceeds *only* on an explicit authorization error (MongoDB code 13,
   `Unauthorized`). Every other outcome is a startup failure: success, timeout,
   network error, namespace-not-found, or any unrecognised error. A check that
   passed merely because the query failed would prove nothing — "cannot tell" must
   never be read as "not permitted". The cursor is never decoded or logged, so no
   DEK record can reach the log even on a misconfigured deployment.

8. **Service impact.** broadcast-worker and history-service both write previews and
   so both need the preview cipher; broadcast-worker has no atrest wiring today and
   gains it. user-service gains atrest plus Vault unwrap capability, scoped per 7.
   The read path still costs one aggregation — decryption happens in-process against
   an always-cached key, with no Vault traffic and no per-room DEK access.

### Residual risk (accepted)

One KEK means user-service holds unwrap capability on the same `chat-kek` that
protects room DEKs, so the Mongo collection grant in 7 is the only control
preventing full history disclosure from a user-service compromise. Any path that
leaks wrapped room DEKs to such an attacker — a Mongo backup, a snapshot, a
read-replica credential, an over-broad grant added later — converts that compromise
into history disclosure. A distinct `chat-preview-kek` would make the two boundaries
independent; it was declined to avoid a second transit key to provision, rotate, and
monitor across every service's Vault policy.

## Amendment 3 (2026-08-06, options review)

**Status: decision pending — no option below is implemented.** The branch implements
Option B minus encryption. This amendment exists to choose between four shapes before
the Amendment 2 rework starts, because that rework's size depends entirely on which
one is picked.

### What prompted it

Two findings from review of Amendments 1–2:

1. **`resolvePreview` never reads the field `warmBackPreview` writes.** In
   `history-service/internal/service/rooms.go`, the load path is
   `roomLastPreviewMessage` (the Cassandra walk) → on `previewFound`,
   `warmBackPreview` persists to the room doc. Nothing reads `previewMessage` back.
   The only consumer anywhere is user-service, for LOCAL rooms. So a cross-site
   room walks Cassandra on every read that misses the in-process `previewCache`,
   then redundantly rewrites the identical preview. **Cross-site rooms get no
   benefit from this design at all**, and `enrichLastMessage`'s own comment
   confirms it: cross-site room docs live remotely and carry no local baseline.

2. **The Amendment 2 rework's cost is concentrated in Vault wiring**, not in the
   preview logic. Encrypting a preview requires the atrest cipher, a Vault client
   with its token-renewal goroutine and shutdown handling, the DEK store handle,
   and config. history-service has all of it. broadcast-worker and user-service
   have none. Which services write and read previews therefore determines most of
   the diff.

### The four options

**A — Status quo (`main`).** No preview stored. Every `subscription.list` issues one
`rooms.get` per site; history-service walks Cassandra per room, bounded by
`previewCache`. No encryption question, because no message content reaches Mongo.

**B — Eager write (the branch as built).** broadcast-worker builds the preview from
the canonical event and folds it into its existing coalesced room `$set`;
history-service writes on mutation and warm-back; user-service reads and decrypts
from the doc. Local rooms are served with zero RPCs when warm.

**C — Warm-back only.** broadcast-worker reverts to `main` — it writes
`lastMsgAt`/`lastMsgId` and nothing else. history-service becomes the sole preview
writer (warm-back + mutation). user-service still reads and decrypts from the doc.
Staleness is implicit: a room is fresh iff `previewForMsgId == lastMsgId`, so a new
message invalidates the preview as a side effect of a write that already happens.

**D — Doc read inside history-service.** Writers as in C, but the *reader* moves:
`resolvePreview` consults the room doc before walking. user-service reverts entirely
to `main` and always calls `rooms.get`. The preview fields join the projection of the
`GetRoomTimesByIDs` batch read that `resolveRoomMetaHints` already performs, so the
doc read costs no extra round-trip.

### Comparison

| | A: status quo | B: eager write | C: warm-back only | D: doc read in history |
|---|---|---|---|---|
| Preview writers | — | broadcast-worker + history-service | history-service | history-service |
| Decrypts preview | — | **user-service** | **user-service** | history-service |
| New Vault/atrest wiring | none | **broadcast-worker + user-service** | **user-service** | **none** |
| RPCs per local list | 1/site | 0 when warm | 1/site if any stale | 1/site always |
| Walk runs | every room | residual only | stale only | doc miss only |
| Cross-site rooms benefit | — | no | no | **yes** |
| Freshness mechanism | n/a | eager write + watermark | `previewForMsgId == lastMsgId` | same, evaluated remotely |
| user-service holds preview DEK | no | **yes** | **yes** | **no** |
| Relative diff size | zero | largest | medium | smallest |

### Cross-cutting facts (true under B, C and D)

- **Encryption is still required.** The Amendment 2 defect is about what is *stored*
  in Mongo, not about who reads it. Any option that persists preview content must
  encrypt it. What changes across options is only *which services hold the key*.
- **The preview-scoped key still earns its place.** Even under D, a doc hit on a
  dormant room needs that preview decrypted without having walked Cassandra, so a
  per-room DEK would re-introduce the cold-tail miss (Mongo read + Vault unwrap per
  dormant room) inside history-service instead of user-service. The site-scoped
  preview key in Amendment 2 §2 stands; under D its consumer set shrinks to services
  that already have atrest, which is what removes the §7 access-control problem.
- **Mutations need an explicit write under every option.** `lastMsgId` changes only
  on message creation, so editing or deleting the current preview message is
  invisible to any implicit freshness check. Amendment 1's `PreviewGone` path stays.
- **Best-effort writes can be lost.** Warm-back and post-mutation writes are
  warn-and-continue with the event acked regardless, so a Mongo failure leaves a
  stale preview until the next message. Design-independent.
- **`visibleTo` is unresolved.** One preview is stored per room but consumed per
  member. When the `visibleTo` write path lands, a partially-visible last message
  will be served to every member, and no freshness check can detect it — the value
  is not stale, merely not per-user. Decide before that path ships.

### Hazards specific to C and D

Both resolve previews by *reading Cassandra*, which introduces a race B does not
have. broadcast-worker and message-worker are independent consumers of
MESSAGES-CANONICAL with no ordering between them, so the room doc can advertise
`lastMsgId = M2` before message-worker has persisted M2. A warm-back that trusts
Mongo's `lastMsgId` would stamp `previewForMsgId = M2` while the walk resolved M1,
claiming freshness for a state it never saw — and it would not self-heal, because
the ID equality holds until M3 arrives.

**Required mitigation:** stamp `previewForMsgId` with the newest message id the walk
actually observed in Cassandra, never the `lastMsgId` read from Mongo. The
ineligible-newest-message case still works, because there the walk did observe the
message and merely skipped it. B avoids this entirely by building the preview from
the event payload it already holds.

**Cold start under C.** With nothing warmed, every room is residual, so the first
list per user sends the full room set to `rooms.get` — straight into the
`maxRoomsGetBatch = 100` cap, which rejects the request outright. C therefore makes
the batch-chunking fix a prerequisite rather than a deferrable follow-up. D shares
the exposure only if a large fraction of a list misses simultaneously.

### Recommendation

**D**, unless zero-RPC is a hard latency requirement.

It is the only option that helps cross-site rooms, the only one that needs no new
Vault wiring, and the smallest diff — user-service reverts to `main`, taking the
`SubscriptionPreviewFromDoc` flag, the aggregation projection, and the residual-set
logic with it. It also keeps the preview DEK confined to services that already
handle message plaintext, which retires the Amendment 2 §7 startup assertion and the
shared-KEK residual risk rather than mitigating them.

Its cost is honest: `rooms.get` fires on every list, so the original "zero RPCs for
a local list" goal is not met — the RPC becomes cheap rather than absent. And because
`previewCache` already absorbs hot rooms in-process, D's measurable gain concentrates
in the cold dormant tail. That is where a large room list's cost actually lives, but
it should be measured against the cache's real hit rate before committing.

Pick B only if a NATS round-trip cannot fit the latency budget; the price is Vault in
two more services, the preview DEK in the front-line service, and two writers whose
divergence has already produced defects.
