# `company_room_members` migration — direct-write handler in oplog-collections-transformer (Design)

> **Status:** DESIGN. Branch: `claude/oplog-room-members`, off `main`. Adds a fifth collection —
> `company_room_members` — to `oplog-collections-transformer`, mapped and written **directly into the
> target per-site Mongo `room_members` collection**. Decisions fixed by the migration owner:
> no room-worker involvement, no inbox events; source facts per `data-migration/SOURCE_DATA.md` §7.

---

## 0. Where this sits

```text
 source Mongo ──change stream──▶ oplog-connector (collections pod)          [already watching it]
                                       │  chat.migration.oplog.{site}.company_room_members.{op}
                                       ▼
                             MIGRATION_OPLOG_{site}
                                       │  5th FilterSubject on the existing durable
                                       ▼
                           oplog-collections-transformer
                                       │  map → model.RoomMember
                                       ▼
                           target Mongo (chat DB) · room_members          [direct upsert/delete]
```

Scope boundary unchanged from the suite: live CDC tail at/after the checkpoint; a separate owner
bulk-migrates state ≤ checkpoint. The mapping below is documented precisely so it can serve as the
reference mapping; keeping the bulk owner's mapping identical is a coordination requirement, not
this service's code.

## 1. Source contract (confirmed, SOURCE_DATA §7)

- One document per (room, member): `{_id, rid, member: {type, id, username?}, ts, federation: {origin?}}`.
- `member.type ∈ org | individual | app | user` (four values; only the first two have confirmed semantics).
- `member.id`: legacy user `_id` for individuals; HR org id for orgs (identical to
  `company_hr_acct_org.orgs[].id`, no transformation). `member.username`: individuals only.
- `_id` is **opaque** (`RandomID()`, both legacy code paths) — the natural triple is NOT derivable
  from it.
- Mutation pattern: **insert + hard delete only.** No in-place updates, no soft-delete flag, no TTL
  or out-of-band cleanup. Change stream shows insert and delete events only.
- `ts` = insertion time; no separate updated timestamp.

## 2. Target contract (verified in code)

- `room_members` docs: `model.RoomMember{_id, rid, ts, member{id, type: individual|org, account}}`
  (`pkg/model/member.go`); display fields are `bson:"-"`, never persisted.
- Natural key `(rid, member.type, member.id)` — room-worker deletes by exactly that triple.
- Reader (`room-service.ListRoomMembers`): serves only `room_members` once a room has any doc;
  subscriptions fallback applies only to rooms with zero docs. Source holds **both** orgs and
  individuals (§1), so a faithful copy keeps rooms complete — no synthesis needed.
- Enrichment joins individuals on `member.account` and orgs on `member.id`; room-worker's dedup
  logic matches `member.id` against **new-stack** user ids.
- Indexes on `room_members` are **room-worker's property**; this transformer creates none
  (same principle as thread_rooms in `targetstore.go`).

## 3. Mapping

| Target field | Source | Notes |
|---|---|---|
| `_id` | **source `_id`, adopted verbatim** | see §4 — deletes are addressable only via source `_id` |
| `rid` | `rid` | legacy room ids are the migrated rooms' ids |
| `ts` | `ts` | insertion time drives the reader's sort |
| `member.type` | `member.type` | `org`→`org`, `individual`→`individual`; **anything else → error-log + skip** (catch-all, §5) |
| `member.id` (org) | `member.id` | HR org id passes through unchanged |
| `member.id` (individual) | resolved | **new-stack user `_id`** via `targetStore.FindUserID(member.username)`; not found ⇒ **Nak-retry** until the user is seeded (thread-subs precedent) |
| `member.account` (individual) | `member.username` | the reader's enrichment join key |
| `member.account` (org) | — | unset (org entries carry no account) |
| `federation.origin` | dropped | informational; the collections lane migrates every doc in this site's source DB (same treatment as rooms/subscriptions — no origin filter) |

## 4. `_id` strategy — adopt the source `_id` (decision, flagged for review)

The source `_id` is opaque and a change-stream `delete` carries only `documentKey._id`, so the
target doc must be addressable by source `_id`. Chosen: **the migrated doc's `_id` IS the source
`_id`** (direct-transfer precedent). Consequences:

- Delete = `DeleteOne({_id})` — direct, index-free (`_id` is always indexed). Missing row = no-op.
- Upsert = `ReplaceOne({_id}, doc, upsert)` — idempotent under JetStream redelivery.
- **Skip-then-delete coherence is automatic:** a skipped `app`/`user` entry was never written, so
  its later delete no-ops — no bookkeeping needed.
- Deviation, documented: migrated docs carry 17-char legacy ids while room-worker-created docs use
  32-char UUIDv7. Both coexist harmlessly (no code depends on `_id` shape in this collection); the
  UUIDv7 convention applies to app-generated ids, and these ids are source-adopted, not generated.

Rejected alternative: UUIDv7 `_id` + a `sourceId` field — preserves the id convention but delete
routing then needs a `sourceId` lookup whose index we don't own (room-worker owns this collection's
indexes), and it adds a field the target schema doesn't define.

## 5. Op handling

| Op | Action |
|---|---|
| `insert` | decode doc → type gate (§3) → map → upsert by `_id`. Degraded insert (connector couldn't encode the doc) → re-read source by `_id` via `SourceLookup`, then map+upsert — the service's existing degraded-recovery convention. |
| `delete` | `DeleteOne({_id: documentKey._id})`; absent row (incl. never-migrated skipped types) = no-op Ack. |
| `update` | **Contract violation** per §1 (legacy never updates). Handled defensively anyway: re-read by `_id` + map + upsert, with a `Warn` log — the `SourceLookup` already exists for degraded recovery, so defence is free. Doc vanished on re-read ⇒ skip. |
| other (`drop`/`rename`/…) | skip, `unknown_op` metric — service convention. |

**Type gate (the §7-finding decision):** `member.type ∉ {org, individual}` ⇒ `slog.Error` (fields:
`rid`, `member_type`, `eventId` — no doc bodies) + Ack + `skipped` metric with reason
`room_member_type_unmapped`. Catch-all rule — no enumerated skip-list. Skipped entries remain in
the source collection and are re-migratable once `app`/`user` semantics are decided.

**Dispositions** (unchanged service model): decode/contract failures → Term (poison); target/source
Mongo unavailable → Nak; unresolved individual user → Nak (waits for user seeding); deliberate
skips → Ack + metric + **Warn log** (visible at the default info level — no metric-only skips).

## 6. Changes, concretely (all inside `data-migration/oplog-collections-transformer/`)

1. `config.go` — `RoomMembersCollection string` env `ROOM_MEMBERS_COLLECTION`, default
   `company_room_members` (pattern of the four existing collection envs).
2. `main.go` — 5th `FilterSubject` + a `SourceLookup` for the collection.
3. `handler.go` — route the collection in the `handle()` dispatch switch (classify.go is room-type classification, not routing).
4. `roommembers.go` (new) — source struct, op dispatch per §5, mapping per §3.
5. `targetstore.go` — `UpsertRoomMember(ctx, model.RoomMember) error` (ReplaceOne by `_id`,
   upsert), `DeleteRoomMember(ctx, id string) error` (DeleteOne by `_id`); added to the
   `targetStore` interface in `handler.go`.
6. `metrics.go` — no new instruments; existing per-collection/reason labels cover it.
7. `deploy/docker-compose.yml` — `ROOM_MEMBERS_COLLECTION` env (default suffices; listed for
   discoverability).

No connector change (already watching). No stream/subject change. No new service.

## 7. Deployment & backlog

Nothing is in production; all migration services deploy together. The durable consumer is created
fresh at first deploy with all five filter subjects and `DeliverAll` — it consumes the stream's
retained `company_room_members` events from the beginning. (Only caveat, for long-lived dev
environments: a durable that pre-exists from before this change won't re-deliver events behind its
acknowledged position — recreate the durable there.)

## 8. Testing (CLAUDE.md §4 — TDD, 80% floor)

- **Unit** (fake targetStore + fake SourceLookup): org insert maps+upserts; individual insert
  resolves user id and account; individual with unseeded user → Nak-class error; `app`/`user`/novel
  type → skip + reason metric + error log; delete → DeleteByID; delete of never-migrated id →
  no-op Ack; degraded insert → lookup recovery; update → warn + re-read + upsert; vanished on
  re-read → skip; malformed doc/documentKey → poison.
- **Integration** (testutil Mongo + the service's existing harness): insert→doc lands with adopted
  `_id`, mapped fields, copied `ts`; delete→doc gone; redelivery idempotent; unmapped type leaves
  no doc.
- Docs in same PR: `CDC_COVERAGE.md` rows for the collection (insert/delete ✅, update n/a-defensive,
  unmapped-type policy); `SOURCE_DATA.md` §7 already carries the schema + finding.

## 9. Explicitly deferred

- **Fail-stop hardening (owner decision, recorded 2026-07-13):** once all scenarios are verified,
  the move-on-with-error posture (error-log + skip) will be replaced by stopping the process on
  error. Until then, every skip is log-visible at Warn/Error (never Debug-only, never metric-only)
  so nothing is silent at the default info log level.

- `app` / `user` member-type mapping (skipped loudly; revisit with source engineers).
- Bulk-migration mapping alignment (owner's job; §3 is the reference).
- Volume-based consumer tuning (sequential consumer, like the rest of the service).
- Fail-stop posture: once all scenarios are verified, escalate from log-and-continue to halting
  the process on error (owner decision, 2026-07-13); until then every skip is Warn/Error-logged.

## 10. Accepted risks

- **Nak-reorder resurrection window.** An insert for a not-yet-seeded individual member Naks
  (`FindUserID` miss) and redelivers later. If the same `_id` is deleted at the source in the
  meantime, the delete Acks as a no-op (the row was never written), and the still-pending insert
  then lands *after* the delete once the user is seeded — the member persists in `room_members`
  as a phantom until the source retires. This is a migration-window-only race (bounded by legacy
  system retirement) with member-listing-only blast radius (no message/notification-path impact).
  Accepted; not fixed by this design — a durable per-`_id` ordering guard is out of scope for a
  short-lived CDC tail.
- **Room-member deletes use the full `MaxDeliver`, not the short `DeleteMaxDeliver`.** The other
  four collections' deletes are non-actionable Ack-skips, so a short cap only bounds churn from
  unrecognisable foreign-origin deletes. `room_members` deletes perform a real target write
  (`DeleteRoomMember`); capping them short would Term-and-drop a delete that's still transiently
  failing (e.g. a >~2min target-Mongo outage) instead of retrying it, leaving a member listed
  after they left. `room_members` is therefore the one collection whose deletes are exempted from
  `DeleteMaxDeliver` in `processOne`'s cap selection (`deliverCapFor` in `main.go`).
