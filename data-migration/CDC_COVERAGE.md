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
| 1 | Room create | `insert` — full doc | in payload | `t` ∈ `c,p,d,l,v`; `prid`⇒discussion; `teamId`/`teamMain`; `d` can have >2 users | ✅ map → `room_sync` (skip `l`,`v`,group-DM, already-soft-deleted) |
| 2 | Room replace | `replace` — full doc | not needed | whole-doc rewrite; can cross type/exclusion boundary; **no delta** to tell which fields changed | ✅ re-classify → `room_renamed` + `room_restricted` + `room_sync` (a `Del-` doc is applied, not skipped — no delta to recognise the deletion by; conservative — field events are idempotent + guarded; subs' denormalized name/visibility must not go stale) |
| 3 | Room change | `update` — changed fields only | full current doc | — | ✅ re-read doc → `room_renamed` / `room_restricted` / `room_sync`; a rename INTO `Del-` **is** the delete and is applied, other changes on a `Del-` room are skipped |
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

The source has no room-delete operation and no delete flag: "deleting" a room renames it — and the
denormalized name on every subscription to it — to `Del-`+name. The destination honors the same
marker (`user-service/mongorepo/subscriptions.go:21` filters `^Del-` rooms out of
`subscription.list` / `getChannels` / `count`), but nothing in the new stack ever *writes* it, so
this migration is its only producer. That makes the rename both the deletion **event** and the
deletion **state**, handled differently per op:

| Source event | Meaning | Handling |
|---|---|---|
| `insert` of a `Del-` doc | born deleted — never existed downstream | ❌ skip (`room_soft_deleted`) |
| `update` whose delta touches `name`/`fname` (set **or** removed), doc now `Del-` | the deletion itself | ✅ apply → `room_renamed` + `room_sync` |
| `replace` of a `Del-` doc | no delta, so the deletion cannot be recognised from the event | ✅ apply — a missed deletion leaves a room visible to its members, which is worse than the hidden room doc a spurious apply costs |
| `update` touching anything else on a `Del-` doc | churn on a dead room | ❌ skip (`room_soft_deleted`) |
| Any subscription op on a `Del-` sub | sub to a deleted room | ❌ skip (`subscription_soft_deleted`) |

Every applied deletion increments `oplog_collections_transformer_soft_delete_applied_total{op}` —
the cutover reconciliation counter to compare against the source's `Del-` room count. Skips are
counted by `…_events_skipped_total{reason=room_soft_deleted|subscription_soft_deleted}`.

**The applied name always carries the marker.** `user-service` hides a room by matching `^Del-` on
the destination `rooms.name`, which maps from `fname` in preference to `name`. When the source
renamed only the machine name, the mapped display name is re-prefixed on the way out
(`rooms.destinationName()`) — otherwise the deletion would be applied and hide nothing.

**Room-type exclusion is classified first.** A `Del-` livechat/voip/group-DM room still meters
`livechat`/`voip`/`group_dm`, and a malformed `updateDescription` on one still skips rather than
terming as poison.

Only the exact prefix counts — `delta`, `del-general` and `team-Del-old` are live rooms.

**Subscriptions never carry the deletion.** It rides the room lane: `room_renamed` →
`UpdateSubscriptionNamesForRoom` already rewrites the name on *every* sub in the room, so subs
imported while the room was live stay consistent. Letting sub-lane field events through instead
would be actively harmful — `UpdateSubscriptionRoles`/mute/favorite/open treat a missing
subscription as an error so the event redelivers until `member_added` lands
(`inbox-worker/main.go:118`), which never comes for a sub whose insert this guard skipped.

**DMs (`t:"d"`) are exempt on both lanes.** There `name`/`fname` hold the peer's username and
display name, not a room name, so the prefix would match a user rather than a deletion. The trade
is deliberate: dropping a live DM because its peer is named `Del-…` is unrecoverable, while a
soft-deleted DM that slips through is not. Note the containment is partial — the `^Del-` room filter
is type-agnostic, so such a DM is hidden **only if** the source prefixed `fname` (the field the
destination name maps from); a `name`-only rename on a DM stays visible. Confirm with the source
team whether DM docs are renamed at all before treating this as closed.

**Residual — spurious apply.** `UpsertRoom` upserts (`inbox-worker/main.go:85`), so applying the
deletion for a room whose `insert` was skipped *creates* a hidden `Del-` room doc rather than
no-op'ing. Invisible to users; avoiding it would cost a room-existence read per event.

**Residual — restore is not reconstructible from CDC.** If the source ever renames a room back out
of `Del-`, the room re-appears (the rename passes the guard and syncs) but its subscriptions do not:
their inserts were skipped while the room was dead, and the restore reaches the sub lane as a
`name`/`fname` delta, which carries no membership. A restore is indistinguishable from an ordinary
rename in the change stream (no `fullDocumentBeforeChange`, so the previous name is unknown), so
rebuilding membership on every rename-shaped sub update would emit a `member_added` storm for every
legitimate rename. **Repair is a targeted backfill, not a CDC behaviour:** the affected room ids are
exactly those counted by `…_events_skipped_total{reason=room_soft_deleted}` — re-run the bulk
subscription copy for them. The source is not known to support un-delete; confirm with the source
team before relying on this.

**Residual — name-less lanes are not filtered.** `company_room_members` and
`company_thread_subscriptions` carry no denormalized room name, so members and thread subs of a
soft-deleted room are still written. They are not user-visible through the filtered reads, so this
is accepted rather than fixed.

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
