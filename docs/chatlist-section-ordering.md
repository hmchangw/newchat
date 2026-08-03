# Within-section room ordering — fractional index + rebalance

How a chat's manual position inside a custom section is stored, computed, and kept
stable. Applies to the per-room order **within** a section; the section list order
itself is a separate string-id permutation (`ChatlistState.sectionOrder`) and is not
affected by anything here.

## Model

Each subscription carries a `sectionOrder float64` — its manual position inside its
section (`pkg/model/subscription.go`). Positions use **fractional (midpoint) indexing**:
a chat placed between two neighbors gets a value strictly between theirs, so a single
move rewrites one row instead of renumbering the whole section.

- **Append** (no anchor): `max(section) + 1`.
- **After `afterRoomId`**: midpoint of that room's order and the next-higher order; if it's
  the tail, `prev + 1`.
- **Before `beforeRoomId`** (top-insertion): midpoint of that room's order and the
  next-lower order; if it's the head, `next - 1`.

Placement is computed by `ComputeSectionOrder` (`room-service/store_mongo.go`), two small
indexed reads in the midpoint case. `afterRoomId`/`beforeRoomId` are mutually exclusive;
a stale anchor (room not in this section) falls back to append.

## Precision limit (and why size is not the axis)

`float64` has a ~52-bit mantissa. Each midpoint insert into the **same gap** halves that
gap; after ~50 consecutive inserts into the identical slot the gap is too small to hold a
distinct midpoint. This depends only on repeated same-slot inserts — **not** on how many
rooms the section holds. 10k rooms appended are 10k clean integers with zero precision
pressure; only an adversarial "insert at the exact same spot over and over" pattern
approaches the limit.

## Exhaustion detection

`sectionMidpoint(prev, next)` is the pure core:

```
mid := (prev + next) / 2
return mid, mid <= prev || mid >= next   // second value = exhausted
```

`exhausted` is true the instant the computed midpoint rounds onto a neighbor — exact
detection, evaluated **before** the value is ever written. It is unit-tested at the
boundary.

## Rebalance

When a move computes `exhausted`, the handler rebalances the affected section **first,
then recomputes** the position (`room-service/handler.go`, `chat.move` path), so no
collapsed value is ever stored — there is no bad-order window.

`RebalanceSection` (`room-service/store_mongo.go`):

1. Load every subscription in `(account, sectionId)`, sorted by current `sectionOrder`
   ascending (projected to `roomId` only) — the rooms in their current display order.
2. Renumber: room at index `i` → `sectionOrder = i+1`. Relative order is preserved (we
   sorted first); only the gaps reset to integer spacing. `[1, 2, 2.5, 2.625, 3]` → `[1,
   2, 3, 4, 5]`.
3. Apply as one **unordered `BulkWrite`** — a single round trip.
4. The handler recomputes the move against the freshly spaced section; the target gap is
   now a clean integer gap with ~50 inserts of fresh headroom.

**Bounds:** scoped to one `(account, sectionId)` pair — never touches other sections,
other users, or the section string-order. Cost O(section size), triggered ~once per 50
same-slot inserts into a given section. Invisible to the user: the visible order is
unchanged, the triggering move just carries one extra write of latency.

## Client contract

- The client sends only the **anchor** (`afterRoomId` / `beforeRoomId`); the backend owns
  the float and the rebalance. Clients never compute order values.
- **Do not treat a cached `sectionOrder` float as stable across a move.** A rebalance
  renumbers a section's siblings but emits a move event only for the moved room, so a
  client's cached sibling values can go stale. Relative order is preserved, so display
  stays correct; for an authoritative refresh, re-read the section (the keyset-paginated
  read returns current values). Order by the sequence, not by comparing cached floats.
- **Concurrent inserts** into the same gap from two devices can momentarily produce two
  equal floats; the section read's keyset cursor tiebreaks on `(sortKey, _id)`, so pages
  stay stable and the pair gets a deterministic order.

## Why fractional + rebalance (vs. string/lexorank)

A string/lexorank scheme is unbounded (keys grow, never rebalance) but heavier keys and a
larger change. Float + bounded rebalance handles the only failure mode (repeated same-slot
inserts) at this scale for a much smaller footprint, with the sort/pagination path staying
numeric. Widening the initial spacing would buy a few more inserts per gap but isn't worth
the added complexity given the rebalance already covers the edge.
