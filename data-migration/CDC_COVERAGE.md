# Collections CDC — coverage matrix

> Companion to `README.md` (component overview) and `SOURCE_DATA.md` (source schema).
> This doc pins **exactly which source change events the collections migration covers, and which it does not** — the reference for the team building the `oplog-collections-transformer`.
> Design: `docs/superpowers/specs/2026-06-16-oplog-transformer-collections-design.md`.
>
> Scope: the **live CDC tail** of the operational collections (rooms, subscriptions, thread_subscriptions, users). The bulk/initial state sync ≤ checkpoint is a separate owner's job; we tail from the handed-off checkpoint.

## CDC payload facts (all collections)

The connector forwards raw change-stream events with **no `updateLookup`** and **no `fullDocumentBeforeChange`**:

> **Deployment note:** the connector runs as two deployments — `oplog-connector-messages`
> (only `rocketchat_message`) and `oplog-connector-collections` (all other watched
> collections) — with disjoint `WATCH_COLLECTIONS`, so a collection-side fault cannot stall
> message CDC. Coverage below is unchanged by the split.

| Op | Payload carried | Source lookup by `_id` |
|---|---|---|
| `insert` | full `fullDocument` | in payload |
| `replace` | full `fullDocument` | in payload (lookup not needed) |
| `update` | only `updateDescription` (changed fields, no post-image) | **full current doc** (doc still exists) |
| `delete` | only `documentKey._id` | **nothing** — doc already gone |

→ A source lookup resolves the full doc for any op **except `delete`**.

## Event coverage matrix

**Legend:** ✅ migrated · ❌ intentionally not migrated · ⚠️ deferred / later work.

| # | Source event | Op + payload | Source lookup (by `_id`) | Current-system facts | Handling / impact |
|---|---|---|---|---|---|
| **Rooms** |
| 1 | Room create | `insert` — full doc | in payload | `t` ∈ `c,p,d,l,v`; `prid`⇒discussion; `teamId`/`teamMain`; `d` can have >2 users | ✅ map → `room_sync` (skip `l`,`v`,group-DM, any `Del-` room) |
| 2 | Room replace | `replace` — full doc | not needed | whole-doc rewrite; can cross type/exclusion boundary; **no delta** to tell which fields changed | ✅ re-classify → `room_renamed` + `room_restricted` + `room_sync` (skip any `Del-` room; conservative — field events are idempotent + guarded; subs' denormalized name/visibility must not go stale) |
| 3 | Room change | `update` — changed fields only | full current doc | — | ✅ re-read doc → `room_renamed` / `room_restricted` / `room_sync`; skipped entirely once the doc is `Del-` prefixed, including the rename that deleted it |
| 4 | Room delete | `delete` — `_id` only | nothing — doc gone | app has no room-delete operation | ❌ skip (no app deletion; un-actionable) |
| **Subscriptions** |
| 5 | Sub create | `insert` — full doc | in payload | `u`, `rid`, `roles[]`, `open`, `f`, `disableNotifications`, `ls`/`lr`, `alert` | ✅ `member_added` + state events (skip subs to soft-deleted rooms) |
| 6 | Sub replace | `replace` — full doc | not needed | whole-doc rewrite | ✅ re-classify → `member_added` + state (skip subs to soft-deleted rooms) |
| 7 | Sub change (incl. leave/rejoin) | `update` — changed fields only | full current doc | leaving sets `open:false` (not a row delete) | ✅ re-read doc → `open`-toggle → `member_added`/`member_removed`; mute/fav/role/read → matching event (skip subs to soft-deleted rooms) |
| 8 | Sub delete (true row removal) | `delete` — `_id` only | nothing — doc gone | destination subs key by generated `UUIDv7`, not source `_id`; removal needs `(roomID, account)` | ❌ skip (un-actionable; rare — leave is `open:false`) |
| **Thread subscriptions** |
| 9 | Follow / first reply | `insert` — full doc | in payload | keyed `(u._id, parentMessage._id)`; carries `rid`, `lastSeenAt`, `unreadMention` | ✅ resolve thread-room+user → `thread_subscription_upserted` |
| 10 | Thread-sub replace | `replace` — full doc | not needed | whole-doc rewrite | ✅ re-resolve → upsert |
| 11 | Thread read / mention change | `update` — changed fields only | full current doc | — | ✅ re-read doc → re-upsert |
| 12 | Thread unfollow | `delete` — `_id` only | nothing — doc gone | destination thread-subs key by `(threadRoomId, userId)`; inbox-worker has no thread-sub removal handler; live stack emits no thread-unfollow federation event | ❌ skip (un-actionable **and** no handler) → stale follow lingers |
| **Users** |
| 13 | User create | `insert` — full doc | in payload | `_id`, `username` (mutable), `type`, `customFields.*`, `roles[]`, `federation.origin` | ✅ insert-if-absent by account |
| 14 | User replace | `replace` — full doc | not needed | whole-doc rewrite | ✅ insert-if-absent (re-classify) |
| 15 | User **HR-field** change (engName, companyName, dept/sect, roles, …) after first seed | `update` — changed fields only | full current doc | company-wide user sync owns these; insert-if-absent leaves existing untouched | ❌ not propagated (other sync keeps it current) |
| 15a | User **`statusText`** change | `update` — changed fields only | full current doc | chat-originated (set by the user inside legacy chat), **not** in the HR dataset — no other sync carries it | ✅ fan `user_status_updated` to all sites (global-visibility) |
| 16 | User deactivate / delete | `update` (`active:false`) or `delete` | `update`: full doc · `delete`: nothing | source sets `active:false` (no row deletion); no destination apply-path wired | ❌ deferred (out of scope) |
| **All collections** |
| 17 | Collection drop / rename | collection-level (`drop`/`rename`/`invalidate`) | n/a | terminates/invalidates the per-collection change stream | ⚠️ out of scope, deferred — connector re-point, not migration logic |

### Soft-deleted rooms (`Del-` prefix)

**Invariant: a room or subscription carrying the `Del-` marker must never appear in the destination
MongoDB — no doc, no rename, no denormalized copy.**

The source has no room-delete operation and no delete flag: "deleting" a room renames it — and the
denormalized name on every subscription to it — to `Del-`+name. Those records are legacy debris and
are simply not migrated.

Every name this transformer can write into Mongo comes from a published event, so the guard sits on
the publish side of all three:

| Event | What it writes | Guarded by |
|---|---|---|
| `room_sync` | `rooms.name` | room lane (`rooms.go`) |
| `room_renamed` | `subscriptions.name` for every sub in the room | room lane (`rooms.go`) |
| `member_added` | the new subscription's name | subscription lane (`subscriptions.go`) |

`company_room_members` and `company_thread_subscriptions` carry no name field, so those lanes cannot
violate the invariant and need no guard.

| Source event | Handling |
|---|---|
| Any op (`insert` / `replace` / `update`) on a room whose `name` **or** `fname` is `Del-` prefixed | ❌ skip (`room_soft_deleted`) |
| Any op on a subscription whose `name` **or** `fname` is `Del-` prefixed | ❌ skip (`subscription_soft_deleted`) |

Only the exact prefix counts — `delta`, `del-general` and `team-Del-old` are live rooms. The check
is case-sensitive; `SOURCE_DATA.md` §3 carries the open question of whether the source ever writes
another casing.

**Rooms deleted after the CDC checkpoint keep their live name.** A room imported while live and
soft-deleted later arrives as a rename into `Del-`, which is skipped like everything else about a
dead room — so the destination keeps the room under its **original, unprefixed** name. The invariant
holds (no `Del-` doc exists), but that room stays visible to its members. Applying the rename would
hide it via user-service's `^Del-` filter, at the cost of writing exactly the doc this invariant
forbids. **Removing such rooms is an ops backfill, not a CDC behaviour** — the destination has no
room-delete path, and the affected ids are exactly those counted by
`…_events_skipped_total{reason=room_soft_deleted}`.

**DMs are not exempt.** For `t:"d"` the name fields hold the peer's username and display name rather
than a room name, so a user literally named `Del-…` costs their DM. That is the deliberate price of
a total invariant: type-scoping the check would let a `Del-` named DM doc into Mongo.

## Direct-transfer collections (oplog-direct-transfer)

Copied **verbatim** by source `_id` into the same-named new-stack collection — no mapping. Because
the destination adopts the source `_id`, **delete is actionable** (unlike the re-keyed collections above).

| Op | Handling |
|---|---|
| `insert` / `replace` | upsert the full doc verbatim by `_id` |
| `update` | re-read the full current source doc by `_id`, upsert; vanished → skip |
| `delete` | delete by `_id` (idempotent) |
| collection-level (`drop`/`rename`/`invalidate`) | ⚠️ out of scope, deferred |

Collections: `rocketchat_avatar`, `company_apps_v`, `company_bot_cmd_men`, `company_tsso_tokens`,
`rocketchat_uploads`, `company_bot_authorization`, `ufsTokens`, `user_devices`.

**Metadata only** — file/blob bytes (UFS/GridFS) are out of scope. No destination indexes or TTL
(removal is CDC-driven). Design: `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md`.

## `company_room_members` coverage (oplog-collections-transformer)

Migrated by **oplog-collections-transformer** (`roommembers.go`) — field-**mapped**, not a verbatim
copy. Target `_id` is adopted from the source `_id`, but the document itself is remapped (member type
narrowed, `member.id` re-keyed for individuals — see below), so this collection is covered by the
collections-transformer, not `oplog-direct-transfer`.

**member.type mapping:** source `member.type` has four values (`org` | `individual` | `app` | `user`);
target schema admits exactly two (`org` | `individual`). Per SOURCE_DATA §7 decision:

- `org` and `individual` entries → mapped and upserted into target `room_members`: `_id` adopted from
  source `_id`; `member.account` ← source `member.username`; `ts` copied
- **Individual member resolution:** target `member.id` = new-stack user id, **re-keyed** via
  `FindUserID(account)` — not copied verbatim; unresolved → Nak-retry (thread-subs precedent)
- **Org member:** target `member.id` = legacy org id (identical to HR org data; no transformation)
- `app` / `user` / unexpected values → error-logged and **skipped** with metric reason
  `room_member_type_unmapped`, Ack (not Nak) — skipped ≠ lost; re-migration is possible once
  semantics are confirmed

| Op | Handling |
|---|---|
| `insert` / `replace` | map + upsert by adopted `_id` (see mapping above) |
| `update` | n/a per source contract — source is insert + hard-delete only; handled defensively: re-read full current doc + upsert, `Warn`-logged as unexpected |
| `delete` | ✅ actionable — delete by `_id` (destination adopts source `_id`); no-op for entries whose type was never migrated (skipped-type, e.g. `app`/`user`) |
| collection-level (`drop`/`rename`/`invalidate`) | ⚠️ out of scope, deferred |

## inbox-worker handler coverage

Every apply-handler the inbox-worker exposes is either produced by the migration or intentionally not:

| Inbox handler | Emitted? | From |
|---|---|---|
| `member_added` | ✅ | sub `insert`/`replace`; `open` false→true |
| `member_removed` | ✅ | sub `open` true→false |
| `room_sync` | ✅ | room `insert`/`replace`/other-field `update` |
| `role_updated` | ✅ | sub `roles[]` |
| `subscription_read` | ✅ | sub `max(ls,lr)` + `alert` |
| `subscription_mute_toggled` | ✅ | sub `disableNotifications` |
| `subscription_favorite_toggled` | ✅ | sub `f` |
| `thread_subscription_upserted` | ✅ | thread-sub `insert`/`replace`/`update` |
| `room_renamed` | ✅ | room `name`/`fname` change |
| `room_restricted` | ✅ | room `restricted`/`externalAccess` change |
| `user_status_updated` | ✅ | user `statusText` change (chat-owned; fanned to all sites) |
| `thread_read` | ⚠️ not emitted | redundant — thread-sub `lastSeenAt` rides `thread_subscription_upserted`; `Subscription.ThreadUnread` is message-pipeline-owned (producer: message-worker `thread_unread_added`) |

## Open confirmations (source engineers)

- Which room field(s) back **`Restricted`** (read-only) and **`ExternalAccess`** — see `SOURCE_DATA.md`.
- Does the source emit whole-doc **`replace`** for these collections, or only field-level `update`? (If never, rows 2/6/10/14 are moot.)
- Where does a user **employee id** live (if at all).
