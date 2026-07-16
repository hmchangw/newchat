# Collections CDC — coverage matrix

> Companion to `README.md` (component overview) and `SOURCE_DATA.md` (source schema).
> This doc pins **exactly which source change events the collections migration covers, and which it does not** — the reference for the team building the `oplog-collections-transformer`.
> Design: `docs/superpowers/specs/2026-06-16-oplog-transformer-collections-design.md`.
>
> Scope: the **live CDC tail** of the operational collections (rooms, subscriptions, thread_subscriptions). The bulk/initial state sync ≤ checkpoint is a separate owner's job; we tail from the handed-off checkpoint.

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
| 1 | Room create | `insert` — full doc | in payload | `t` ∈ `c,p,d,l,v`; `prid`⇒discussion; `teamId`/`teamMain`; `d` can have >2 users | ✅ map → `room_sync` (skip `l`,`v`,group-DM) |
| 2 | Room replace | `replace` — full doc | not needed | whole-doc rewrite; can cross type/exclusion boundary; **no delta** to tell which fields changed | ✅ re-classify → `room_renamed` + `room_restricted` + `room_sync` (conservative — field events are idempotent + guarded; subs' denormalized name/visibility must not go stale) |
| 3 | Room change | `update` — changed fields only | full current doc | — | ✅ re-read doc → `room_renamed` / `room_restricted` / `room_sync` |
| 4 | Room delete | `delete` — `_id` only | nothing — doc gone | app has no room-delete operation | ❌ skip (no app deletion; un-actionable) |
| **Subscriptions** |
| 5 | Sub create | `insert` — full doc | in payload | `u`, `rid`, `roles[]`, `open`, `f`, `disableNotifications`, `ls`/`lr`, `alert` | ✅ `member_added` + state events |
| 6 | Sub replace | `replace` — full doc | not needed | whole-doc rewrite | ✅ re-classify → `member_added` + state |
| 7 | Sub change (incl. leave/rejoin) | `update` — changed fields only | full current doc | leaving sets `open:false` (not a row delete) | ✅ re-read doc → `open`-toggle → `member_added`/`member_removed`; mute/fav/role/read → matching event |
| 8 | Sub delete (true row removal) | `delete` — `_id` only | nothing — doc gone | destination subs key by generated `UUIDv7`, not source `_id`; removal needs `(roomID, account)` | ❌ skip (un-actionable; rare — leave is `open:false`) |
| **Thread subscriptions** |
| 9 | Follow / first reply | `insert` — full doc | in payload | keyed `(u._id, parentMessage._id)`; carries `rid`, `lastSeenAt`, `unreadMention` | ✅ resolve thread-room+user → `thread_subscription_upserted` |
| 10 | Thread-sub replace | `replace` — full doc | not needed | whole-doc rewrite | ✅ re-resolve → upsert |
| 11 | Thread read / mention change | `update` — changed fields only | full current doc | — | ✅ re-read doc → re-upsert |
| 12 | Thread unfollow | `delete` — `_id` only | nothing — doc gone | destination thread-subs key by `(threadRoomId, userId)`; inbox-worker has no thread-sub removal handler; live stack emits no thread-unfollow federation event | ❌ skip (un-actionable **and** no handler) → stale follow lingers |
| **Users** |
| 13–16 | All `users` change events (create/replace/HR-field/`statusText`/deactivate/delete) | any | n/a — collection not consumed | company-wide user sync owns the destination `users` collection | ❌ **removed from this transformer's scope** — the `users` collection is no longer filtered/consumed; the transformer only *reads* target users for FK resolution (thread-subs, room-members) |
| **All collections** |
| 17 | Collection drop / rename | collection-level (`drop`/`rename`/`invalidate`) | n/a | terminates/invalidates the per-collection change stream | ⚠️ out of scope, deferred — connector re-point, not migration logic |

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
| `user_status_updated` | ⚠️ not emitted | user migration removed from the collections transformer; no producer in the migration path |
| `thread_read` | ⚠️ not emitted | redundant — thread-sub `lastSeenAt` rides `thread_subscription_upserted`; `Subscription.ThreadUnread` is message-pipeline-owned |

## Open confirmations (source engineers)

- Which room field(s) back **`Restricted`** (read-only) and **`ExternalAccess`** — see `SOURCE_DATA.md`.
- Does the source emit whole-doc **`replace`** for these collections, or only field-level `update`? (If never, rows 2/6/10/14 are moot.)
- Where does a user **employee id** live (if at all).
