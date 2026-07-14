# Bounded Thread Reply Count (tcount)

## Problem

`tcount` is the thread-reply-count badge on a parent message. On `main` (PR #245,
"COUNT-based approach") it is maintained by re-counting the thread's Cassandra
partition on every reply and every delete, then blind-`SET`ting the result on the
parent row:

```sql
SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ?
```

This streams **every row** in the partition over the wire and counts the
non-deleted ones in Go (`message-worker/store_cassandra.go:331`,
`history-service/internal/cassrepo/write.go:351`). Cost is **O(thread-size) per
write**; building a thread of N replies costs O(N²) cumulatively, and each count
sits on the synchronous JetStream-ack path of `message-worker`, holding a worker
slot for its full duration.

The unbounded scan is the cost #245 knowingly accepted (one stateless source of
truth, no second store, idempotent, soft-delete-aware) over the MongoDB-counter
alternative in PR #276. This change keeps #245's architecture and only removes
the unbounded part.

## Goals

- Bound the per-write count cost to a constant, independent of thread size.
- Preserve the two invariants #245 deliberately protects:
  - **Idempotent under JetStream redelivery** — recompute-then-blind-`SET`
    yields the same value on a redelivered reply (no over/under count).
  - **Soft-delete-aware** — soft-deleted replies (`deleted = true`) are excluded.
- Keep a single source of truth in Cassandra; do **not** introduce a second
  counter store (that is PR #276 / Approach C, explicitly not revived here).

## Non-Goals

- No change to the read path or to how `tcount` is read back.
- No backfill of existing `tcount` values; they self-heal on the next reply or
  delete to the thread.
- The ghost-row blind-`UPDATE` on an absent parent and the `tcount == 0`
  `GetThreadMessages` short-circuit (issue #286) are pre-existing concerns the
  cap neither causes nor worsens (a non-empty thread always counts ≥ 1). Out of
  scope.

## Relationship to #245's documented future work

PR #245 (merged to `main` 2026-06-10) is the COUNT-based approach that produced
the current code. It flagged this O(N) scan as a known limitation and committed
to a *specific* follow-up in `docs/superpowers/plans/2026-06-04-tcount-count-based.md`
§ "Known Trade-offs and Future Work": a dedicated Cassandra **COUNTER table**
(`thread_reply_counts`) incremented/decremented on add/delete, made crash-safe by
a **periodic reconciliation job** that overwrites the counter with the true
`COUNT(*)`.

**This design supersedes that COUNTER-table plan.** The COUNTER table is O(1) and
exact, but a Cassandra `counter` increment/decrement is **not idempotent under
JetStream redelivery** — the very property #245 (and #276) worked to guarantee —
so it requires a new scheduled reconciliation job to bound the resulting drift,
plus new DDL and an operational component. Approach A instead stays entirely
inside #245's idempotent, stateless model (recompute-then-blind-`SET`), adds no
table and no job, and bounds the per-write cost to a constant — accepting a
display cap as the only trade-off. #245's own note that threads under ~500
replies scan in sub-millisecond time confirms a cap of 99 sits comfortably in the
safe zone.

When the implementation lands, the COUNTER-table future-work item in
`2026-06-04-tcount-count-based.md` is updated to point here as the chosen
resolution.

## Design

### Bounded count

Replace the unbounded scan with an early-terminating bounded count: iterate the
partition in clustering order, increment the tally for each non-deleted row, and
**stop as soon as the non-deleted tally reaches the cap**. With gocql paging
(page size ≈ cap) and an early `break`, the coordinator only ever materializes
~cap rows.

No `LIMIT` clause in the CQL: soft-deleted rows are interspersed with live rows,
so a hard `LIMIT cap` could return cap rows of which some are deleted and
undercount the live total. Early-break on the *non-deleted* tally is the correct
bound. Because soft-delete is `UPDATE … SET deleted = true` (not a CQL `DELETE`),
deleted rows are live rows, not tombstones — no tombstone-scan amplification.

Result semantics: the count returned (and persisted to `tcount`) is
`min(trueNonDeletedCount, Cap)`.

- Below the cap: exact.
- At/above the cap: returns `Cap`.

Per-write cost drops from O(thread-size) to **O(cap)**; cumulative thread-build
cost drops from O(N²) to O(N·cap).

### Cap value & frontend contract

`Cap = 99`. Virtually all real threads are far below this, so the badge stays
exact in practice; threads at/above show "99+". The persisted `tcount` is
`min(trueCount, 99)`; the frontend renders `tcount >= 99` as "99+".

This is a client-visible semantics change (previously `tcount` was unbounded),
documented in:
- `docs/cassandra_message_model.md` — the `tcount` column comment in
  `messages_by_room` and `messages_by_id`.
- `docs/client-api.md` — if `tcount` appears in any `chat.user.` read response
  (history-service message reads), note the cap on that field.

### Two write sites — shared helper

`tcount` has two authoritative writers, one per mutation direction:

- **Reply added (count up):** `message-worker` — `SaveThreadMessage` →
  `countAndSetParentTcount` (`store_cassandra.go:226`). The only service that
  sees new replies (consumes MESSAGES_CANONICAL).
- **Reply soft-deleted (count down):** `history-service` — `SoftDeleteMessage` →
  `countAndSetParentTcount` (`write.go:343`). Owns the delete path;
  `message-worker` never sees deletes.

Both recompute-from-source and blind-overwrite the same field, so they **must
use the same cap** — otherwise an add (capped) and a delete (differently capped)
would write different values and the badge would flip-flop. To guarantee they
cannot drift, the bounded count is extracted into one shared unit:

```go
// pkg/threadcount
const Cap = 99 // single source of truth for the display cap

// Count returns min(non-deleted replies in the thread partition, Cap).
func Count(ctx context.Context, session *gocql.Session, threadRoomID string) (int, error)

// CountAndLatest additionally returns the latest surviving reply's created_at
// (tlm), from the same bounded scan — used by the delete path, which must
// recompute tlm when the removed reply may have been the newest.
func CountAndLatest(ctx context.Context, session *gocql.Session, threadRoomID string) (int, *time.Time, error)
```

message-worker's add-path `countThreadReplies` delegates to `threadcount.Count`;
history-service's delete-path `countThreadReplies` delegates to
`threadcount.CountAndLatest` (the delete may remove the newest reply, so tlm is
recomputed alongside the count). Both share the same `Cap`, and their
`setParentTcount` / `setParentTcountAndTlm` blind-write (both tables) is
unchanged. The cap lives in exactly one place.

### Federation

Cross-site replays go through the same `message-worker` store path, so they
recompute via `threadcount.Count` and stay consistent automatically — no
separate handling.

## Testing (TDD)

`tcount` is Cassandra-backed, so coverage is primarily integration
(testcontainers), at the helper and at both sites:

- **`pkg/threadcount`** integration test:
  - under cap → exact count
  - over cap → returns `Cap`
  - deleted rows interspersed → excluded from the tally **and** the read stays
    bounded
  - empty / single-reply partitions → 0 / 1
- **`message-worker`** integration: a reply burst past the cap persists `Cap`;
  redelivery of a reply yields the same `tcount` (idempotent).
- **`history-service`** integration: deleting a reply re-counts via the shared
  helper and writes a consistent capped value; deleting below the cap restores
  an exact count.
- Existing mocked-store handler/service unit tests assert the returned value
  threads through unchanged.

## Rollout & Risk

- No schema/DDL change; `tcount` stays `INT`.
- No migration: existing values converge to the capped value on the next
  add/delete to each thread.
- Risk: a pathological thread that is almost entirely soft-deleted forces the
  bounded read to scan past many deleted rows to reach `Cap` live ones — but
  this is strictly ≤ today's cost (which always scans the whole partition), so
  it is a regression-free edge.
