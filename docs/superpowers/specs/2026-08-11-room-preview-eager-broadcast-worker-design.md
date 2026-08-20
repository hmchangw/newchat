# Room-list Preview — Eager Persistence, with the Lazy Path Underneath

**Date:** 2026-08-11
**Status:** Implemented. This is the whole design: the read-time walk and its warm-back
are described here alongside the eager writer, not inherited from a predecessor.
**History:** an earlier revision (PR #193) answered "who writes the preview" with *the
reader, when it misses*. That answer survives here as the fallback; what this design adds
is a second writer on the insert path. The earlier document has been dropped rather than
kept as a superseded copy — everything still true of it is stated below.

## The one decision

The 2026-08-05 design memoizes the preview lazily: history-service walks Cassandra when
it must, then warm-backs what it found. This design adds an **eager** write from
broadcast-worker, on the message it is already fanning out, and keeps the lazy path as
the fallback that makes every miss self-healing.

Everything below is downstream of that addition:

| | 2026-08-05 (lazy only) | This design (eager + lazy) |
|---|---|---|
| Writer on insert | none — the first read pays | broadcast-worker, from the message in hand |
| Writer on edit/delete | history-service | history-service (unchanged) |
| Writer on a read miss | history-service (warm-back) | history-service (warm-back, unchanged) |
| Read path | stored preview, else walk + warm-back | identical |
| Cassandra on `rooms.get` | on every first read of a room | only on a miss, which the eager writer makes rare |
| Cold rooms (pre-rollout) | resolved on first read | resolved on first read |
| Backfill | not needed (reads self-heal) | not needed (reads self-heal) |
| `pkg/preview` write shapes | set, clear | set, clear, **advance-key**, **update-body** |
| Preview cache in history-service | required (fronts the walk) | retained, fronting the fallback only |

### Who writes what, and why that split

| Path | Writer | Why it, and not the other |
|---|---|---|
| Insert | broadcast-worker | It already holds the message, sender and mentions for the fan-out, so composing a preview costs no read. history-service is not on this path at all. |
| Edit / delete | history-service | It already walks Cassandra here to recompute the preview. broadcast-worker holds no Cassandra client and could only replay a decision made in history-service — so the decision and the write stay together. |
| Read miss (warm-back) | history-service | It is the reader; the walk it just paid for is the thing worth storing. |

The two writers never coordinate. Every write on both sides goes through a guarded
update keyed on the `previewAsOf` watermark, so the newer write wins whichever lands
second, and a losing write is a no-op rather than an error.

**Eager is an optimization, not a replacement.** The lazy path stays intact underneath:
`rooms.get` serves the stored preview when there is one and walks when there is not. That
is what makes the eager writer optional — a site that does not run it, or runs it with
`ATREST_ENABLED=false`, still returns previews at the old price. It is also what keeps
every miss self-healing rather than permanent, which is why this design needs no backfill
and carries no "blank until the next message" gap.

The miss is the only thing the eager writer is trying to make rare. It does not have to
make it impossible, and the read path must not assume it did.

## Why eager is cheap here, and why that was not obvious

The objection to writing on every insert is write amplification: a preview write per
message, against a warm-back that writes only on a miss.

It does not survive coalescing. broadcast-worker already buffers
`rooms.lastMsgAt`/`lastMsgId` per room and drains them through one unordered `BulkWrite`
every `LAST_MSG_FLUSH_INTERVAL` (250ms). The preview rides **that same buffered
document update** — same room, same write, same flush. A room receiving 40 messages a
second still costs 4 room-doc writes a second, with or without this feature. The marginal
cost of the preview is the extra bytes in an update that was already being issued.

So the real cost of eager persistence is not writes. It is that the writer no longer has
the information the walk gave it for free, which is what the next two sections are about.

## Consequence 1: the freshness key has to be maintained by hand

A stored preview is served only when all four hold: a body is present,
`previewForMsgId == lastMsgId`, `previewKeyEpoch` matches the reader's configured epoch,
and the ciphertext authenticates. The identity check is the interesting one — the other
three are availability conditions — but "current" below is always shorthand for all four. Under the lazy design the
walk supplies that id directly — it is the newest message the walk *observed*. The eager
writer has no walk, so it must reconstruct the same invariant from the event stream:

- **Eligible insert** → write the body *and* set `previewForMsgId` to this message.
- **Ineligible insert** (a system message) → the stored body is still the room's last
  eligible message and must not move, but `lastMsgId` has advanced, so
  `previewForMsgId` must advance with it. This is `GuardedAdvanceKeyFields`, and it is
  load-bearing: rooms whose latest activity is a join or rename notice are common, and
  skipping it would blank their previews.
- **Mutation** → a new body, but `lastMsgId` has not moved, so the stored key is already
  correct and must be left alone. This is `GuardedUpdateBodyFields`, which additionally
  refuses to write when no key is stored — under eager persistence an insert is the only
  thing that may *create* a preview, because only an insert knows the id that makes one
  readable.
- **Eligible insert that fails to seal** → the stored body is *cleared*. This case looks
  like an ineligible insert (no body to write) but must not be treated as one: the stored
  body is *not* the room's last eligible message, so leaving it in place risks presenting
  the previous message's content as the current one's. `ForMsgID` is not part of the
  AEAD's authenticated data — `Seal`/`Open` bind only site+epoch — so a stale ciphertext
  opens cleanly under any key that later points at it.

  An earlier revision only *withheld* both fields, relying on `previewForMsgId !=
  lastMsgId` to make the reader miss. That is not durable. The failure flag is
  per-flush-window in-memory state while the key advance is a durable write, so the next
  **ineligible** insert takes the advance-key branch and restamps `previewForMsgId` onto
  `lastMsgId` — over the body the withholding existed to suppress. The ids match again,
  the reader serves it as current, and because it reads as current the lazy walk is never
  asked to repair it. Clearing leaves nothing to revalidate; it costs a walk on the next
  read, which the warm-back then repays.

Within a flush window the coalescer tracks the newest message and the newest *eligible*
preview on separate clocks, and the flush stamps the freshness key from the former. That
divergence is the induction the whole design rests on. A seal failure rides the preview
clock rather than being recorded flatly, so an older successful seal arriving later in the
same window cannot overwrite it and restore the write it exists to withhold.

## Consequence 2: "no preview" needs to cross the wire

history-service still computes the post-mutation preview — a delete of the current
preview must find its replacement, and only a Cassandra walk can. It publishes that on the
canonical event exactly as before. But under this design broadcast-worker *acts* on the
value, and a nil `previewMessage` is three things at once:

1. not a room-preview event (a hidden thread reply),
2. the walk gave up (read failure, or the 250-row budget),
3. the room genuinely has no eligible message left.

Only (3) authorises a destructive write, and (2) and (3) are opposites: clear on a
degraded walk and a transient Cassandra hiccup wipes a good preview; ignore (3) and a
deleted message stays in the room list forever, because a delete does not move
`lastMsgId` and the freshness check keeps reporting the stored preview as current.

The walk's outcome is three-valued internally (`previewFound` / `previewEmpty` /
`previewDegraded`) for exactly this reason, and history-service acts on all three itself:
found reseals the body, empty clears, degraded touches nothing.

An earlier revision of this design put the mutation write in broadcast-worker, which
forced the distinction onto the wire as a `MessageEvent.PreviewGone` flag — because a nil
`PreviewMessage` is all three states at once, and only one of them authorises a
destructive write. Moving the write to the service that already owns the walk removes the
flag entirely: nothing has to encode "the room is empty" for a second service to act on,
because the service that established it is the one acting. `PreviewMessage` still rides
the event, but purely so broadcast-worker can relay it to clients — it carries no storage
instruction.

### PREVIEW_KEY_EPOCH must match across the two writers

`broadcast-worker` and `history-service` must run the same `PREVIEW_KEY_EPOCH` in a given
site. A mismatch is safe but silently expensive: the reader treats every eagerly written
preview as a retired-epoch miss and falls back to the Cassandra walk, so the optimization
is forfeited while looking healthy.

There is no startup validation, and adding one is not straightforward: neither service can
see the other's configuration, so a real gate would need a shared source of truth (a
sentinel document in Mongo, or the deployment system asserting it). Today the constraint
is carried by the local compose files, which pin the same `${PREVIEW_KEY_EPOCH:-1}` on
both sides, and by the rollout section of the PR. Worth revisiting if a mismatch ever
happens in practice — the symptom is a rise in walk rate with no error.

## Operational limits the design does not enforce

**The preview DEK has a nonce budget, and nothing counts it.** One site-wide key seals
every preview, `atrest` uses random 96-bit nonces, and rotation is a manual
`PREVIEW_KEY_EPOCH` bump. The random-nonce birthday bound is ~2^32 invocations, which at
~500 messages/second is roughly 100 days.

A separate DEK from the message keys was chosen precisely because of invocation count —
so the gap is not in the design but in the operational half of it: nothing measures the
count, warns as it approaches the bound, or forces the epoch forward. Until it does, the
rotation cadence is a deployment obligation rather than a property of the system.

The durable fix is an invocation counter exported as a metric, with an alert well below
the bound. Rotation itself already works: a reader on a retired epoch treats the stored
preview as a miss and the warm-back reseals it under the new one.

## What the read path becomes

Preview content on the wire is capped at 500 Unicode runes, not bytes: `preview.Build`
truncates on a rune boundary before the body is ever sealed, so the stored ciphertext and
the served payload carry the same snippet and neither can split a multi-byte character.
The cap therefore applies identically to a stored preview and to one the walk resolves.

`RoomsGet` becomes one batched `$in` room-doc read that partitions the request in two.
Rooms whose document already carries a current preview are served from it — a decrypt per
hit, no Cassandra. Rooms without one fall back to the same read-time walk as before, in
bounded parallel, fronted by the same positives-only preview cache, and a walk that
resolves warm-backs what it found so the next read is served from the document.

The warm-back is keyed on the newest message the walk *observed* in Cassandra, never on
the preview's own id and never on the room doc's `lastMsgId`. Not the preview's id,
because the two differ whenever the newest message is ineligible and gets skipped — the
ordinary case for a room whose last activity was a join notice — and keying on it would
re-walk forever. Not `lastMsgId`, because broadcast-worker and message-worker consume
MESSAGES-CANONICAL unordered, so the doc can name a message Cassandra does not hold yet,
and stamping that would claim freshness for a state nothing ever observed.

The batched read pays for both halves: `RoomTimes` carries `lastMsgAt`/`createdAt`
alongside the preview, so the fallback's walk bounds come from a read the request already
made. That is also why `req.Hints` stays ignored — the row supplies the same bounds
first-hand, so a client hint could only be staler. The field remains for wire
compatibility; user-service should stop sending it in a follow-up.

Two properties worth stating:

- **A batched-read failure is an error, not an empty map.** The fallback is no escape
  here: it reads the same Mongo for its bounds. Returning an empty map would be
  indistinguishable at the client from "none of these rooms has a preview", and would
  blank every row. Per-*room* failures still degrade to no entry, as before.
- **The preview cache is retained, but demoted.** It now fronts only the fallback — a room
  served from its document never reaches the walk the cache exists to spare. On a site
  with the eager writer running it is nearly idle; on a site without one it is doing
  exactly the job it always did. `HISTORY_PREVIEW_CACHE_SIZE` / `_TTL` keep their meaning.

## Accepted gaps

**Out-of-order inserts can blank a preview transiently.** `lastMsgAt`/`lastMsgId` stay
unguarded, exactly as before this change — making them monotonic would let a single bad
future timestamp freeze a room's ordering permanently, where today the next message
corrects it. The preview fields *are* watermark-guarded, so an insert processed out of
order can leave `previewForMsgId` naming a message that is no longer `lastMsgId`, which
reads as a miss. With the lazy fallback in place that miss is not a blank row — it costs
a walk, and the room's next message restores the cheap path. Bounded, self-healing, and
rare: the coalescer already collapses reordering inside a flush window, so only
reordering across windows can reach this.

**The cross-site limitation is unchanged from the 2026-08-05 analysis and does not apply
here.** A room's document lives at the room's site, and so does the broadcast-worker
writing it. Reads still come from history-service at the room's site. Writer and reader
remain co-located, which is the property the cross-site objection was about.

**`visibleTo` is still unresolved.** It is per-user, one preview is stored per room, and
the insert path cannot populate it at all (the canonical message carries no such column).
Same open question as the 2026-08-05 design, no worse.

## What broadcast-worker gains

It now holds an `atrest.Cipher` over a second DEK collection (`preview_deks`), a Vault
wrapper, and a projected `apps` lookup for bot senders' display names. That last one is
the only read the preview path performs, and only for accounts `model.IsBot` recognizes;
everything else is composed from the message already in hand.

`ATREST_ENABLED=false` disables preview persistence outright rather than falling back to
plaintext. The room document must never hold an unencrypted message body — that is the
same rule the 2026-08-05 design states, and the reason the preview is split into clear
metadata and a sealed body at all.

## Decision log

**Guarding `lastMsgAt`/`lastMsgId` on the same watermark — rejected.** It would make the
identity check unbreakable, but a single future-dated timestamp would then freeze the
room's ordering with no path back. The transient-blank failure above was judged the
cheaper of the two.

**Dropping the identity check entirely — rejected.** Under this design broadcast-worker
writes both fields in one document update and is the sole writer of `rooms.lastMsgId`
(verified: the only other `lastMsgId` writer in the tree is message-worker's, on
`thread_rooms`), so the check is *nearly* redundant. Keeping it costs one string compare
and preserves the one signal that a write landed half-applied.

**Letting a seal failure fall through to the ineligible path — rejected (#224).** It is
the cheaper branch, and it was the original behaviour: `previewForInserted` returned a
bare `nil` for "off", "ineligible" and "seal failed" alike. But the three do not share a
write. Collapsing the third into the second advances the freshness key past a body that
was never replaced, and because the reader's only staleness signal is
`previewForMsgId == lastMsgId`, the stale body then reads as current — a silent wrong
answer rather than the blank row the degrade was reasoned about. The tri-state costs one
bool on the coalescer; serving one message's content under another's is not a cost this
design can absorb.

**Sealing the preview at flush time rather than in the handler — rejected.** The flush
runs off the request goroutine, without the user map or the mention resolution; sealing
there would mean re-resolving both. The handler seals, the flush stamps the freshness key.
