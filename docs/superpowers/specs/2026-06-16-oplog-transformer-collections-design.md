# oplog-transformer (collections) — rooms / subscriptions / thread-subs / users migration (Design)

> **Status:** DESIGN — next increment of the data-migration suite, after the message path (`oplog-transformer`, PR #331). Migrates the **operational collections** from the legacy ("source") RocketChat MongoDB into the new-stack **per-site MongoDB**, reusing the existing cross-site `inbox-worker` apply machinery where one exists and writing Mongo directly where it doesn't. Built on branch `claude/oplog-transformer-collections`.

*The message path wrote message history to **global Cassandra**. This path writes **operational state** — rooms, subscriptions, thread subscriptions, users — to **per-site MongoDB**. That difference drives every decision below.*

---

## 0. Where this sits

```text
                  ┌─ OUT OF SCOPE (separate owner): bulk/initial state sync of all
                  │  existing data ≤ checkpoint (history + the existing rooms/subs/
                  │  thread_subs/users snapshot). Produces the checkpoint we tail from
                  │  and guarantees context is fully synced before our tail applies.
                  ▼
            ┌──────────── checkpoint C (resume token) handed to us ───────────┐
            │                                                                  │
 (per site) source Mongo ─▶ oplog-connector ─▶ MIGRATION_OPLOG_{site}  (chat.migration.oplog.{site}.{coll}.{op})
            startAfter(C): live CDC tail only             │
                                                          ▼  this service (name OPEN — §10)
                            collections-transformer ─┬─▶ INBOX_{site}  (OutboxEvent) ─▶ inbox-worker ─▶ per-site Mongo
                                                      │     rooms · subscriptions · thread_subscriptions
                                                      └─▶ direct Mongo write
                                                            users (insert-if-absent)
```

**Scope boundary — we are the live CDC tail, not the bulk migration.** A separate owner migrates all pre-existing state (message history *and* the existing operational-collection rows) up to a **checkpoint** and hands it to us; by the time we apply a tailed change, its **context and contents are fully synced** in the destination (the referenced user, room, and thread_room already exist). We do **not** snapshot/backfill, compute the cut, or derive thread_rooms. We **only** consume change events **at and after the given checkpoint** and apply them to the new-stack per-site Mongo.

The connector already tails these collections (they're in `WATCH_COLLECTIONS`) and publishes raw `model.OplogEvent`s; for collections it is configured to **start the change stream at the supplied checkpoint** (not `now`). This service is a **new consumer** on `MIGRATION_OPLOG_{site}` filtered to the operational-collection subjects — the counterpart of the message transformer, for non-message data.

**Deployment is per-site.** Each site runs its own connector + this transformer against **that site's** source DB; `SITE_ID` is the site code. There is no cross-record site derivation for routing — a site's pump produces that site's data into that site's new stack (see §6 for the one place origin matters: `siteId` stamping).

---

## 1. Scope

Migrate **4** source collections:

| Source | → Destination (per-site Mongo) | Path |
|---|---|---|
| `users` | `users` | **direct write** |
| `rocketchat_rooms` | `rooms` | **inbox publish** |
| `rocketchat_subscriptions` | `subscriptions` | **inbox publish** |
| `company_thread_subscriptions` | `thread_subscriptions` | **inbox publish** |

**`threadRooms` is explicitly out of scope** — `thread_rooms` are *derived* by `message-worker` from the message canonical stream (created on the first thread reply, accumulating `LastMsgAt`/`LastMsgID`/`ReplyAccounts`). We do not track them here.

**The decision rule** (validated against the live writers): a destination gets the **inbox-publish** path iff `inbox-worker` already has an apply-handler for it (rooms, subscriptions, thread_subscriptions). The two with no apply-path get a **direct write**: `users` (nothing in the new stack writes it — it's populated externally) and, by the same logic, `thread_rooms` (only message-worker writes it) — which is why `thread_rooms` is left to the message path.

**No new collections, no new event types** — reuse the existing `model.OutboxEvent` set and existing Mongo collections.

---

## 2. Two output paths

### 2.1 Inbox publish (rooms, subscriptions, thread-subs)

Emit reused `model.OutboxEvent`s to the **local `INBOX_{site}`** stream; the existing `inbox-worker` applies them to per-site Mongo via its `InboxStore`. This reuses `UpsertRoom` / `BulkCreateSubscriptions` / `UpdateSubscriptionRoles` / `UpsertThreadSubscription` / etc. rather than reimplementing those writes.

Publishing to the **local** INBOX (not OUTBOX) is deliberate: INBOX is normally sourced from *remote* OUTBOX, but here we want the *local* inbox-worker to apply *this* site's migrated data to *this* site's Mongo. The publish is a **fire-and-forget JetStream publish** (no apply-result back to the transformer) — order-healing therefore lives either in inbox-worker (users, §5) or in the transformer's pre-publish resolution (thread-subs, §4.4).

### 2.2 Direct write (users)

`users` has **no** inbox apply-path and is otherwise populated by external company-wide user syncs. The transformer writes the per-site `users` collection **directly**, **insert-if-absent keyed by account** — a pure seed that unblocks subscriptions/thread-subs without ever clobbering what another sync owns. Requires a **unique index on account** so the seed and other syncs converge on one doc.

---

## 3. Cross-site model (the "cross-site CDC")

Each site's source DB **already contains its federated copies** — site B's RocketChat already holds the rooms its B-users joined and those users' subscriptions. So the new stack reproduces the cross-site replication **by construction**: each pump migrates its **full source locally**, and the same room/subscription that was replicated across sites in the source ends up replicated across sites in the new stack.

Consequences:
- **No active fan-out** — no `OUTBOX` routing, no dest-site derivation. N independent per-site pumps.
- **No federation drop-filter** — unlike the message path (global Cassandra ⇒ drop foreign-origin copies to avoid duplicates), per-site Mongo *wants* the federated copies. We migrate everything in the source.

---

## 4. Per-collection mapping

### 4.0 CDC op handling (cross-collection)

The connector forwards raw change events with no `updateLookup` and no `fullDocumentBeforeChange` (`source_mongo.go`), so each op carries different data and the transformer treats them uniformly across all four collections:

| Op | Payload | Handling |
|---|---|---|
| `insert` | full `fullDocument` | classify + map from the doc |
| `replace` | full `fullDocument` | **same path as `insert`** — re-classify from scratch (exclusions, type, `siteId`) then upsert, since a whole-doc rewrite can cross a classification boundary (e.g. `c`→`l`, a `d` gaining a 3rd participant, or `federation.origin` changing). Decided. |
| `update` | only `updateDescription` (changed fields, no post-image) | **re-read the full current source doc by `documentKey._id`** (the doc still exists), then map — a partial delta can't reconstruct full state (e.g. `max(ls,lr)` needs both fields; positional `roles.N` carries no array). Mirrors the message transformer's `FindByID`-on-update. |
| `delete` | only `documentKey._id` (no pre-image, doc gone) | **un-actionable for subs/thread-subs** — the destination keys by generated `UUIDv7` / `(threadRoomId,userId)`, not the source `_id`, and the removal keys live inside the deleted doc. Disposition: **skip + metric**, never an attempted apply. (Rooms/users delete is skipped by policy anyway; subscription *leave* arrives as an `open:false` **update**, which is handled.) |

**Collection-level ops (`drop` / `rename` / `dropDatabase` / `invalidate`) — OUT OF SCOPE (deferred).** A single-collection change stream terminates/invalidates on these; they signal a watched source collection moving out from under the migration. Recovery is an operator action (re-point the connector), not transformer logic. Connector behavior on a terminal event (halt-and-alert vs. resume) is to be decided in that later work. Recorded here so it isn't silently dropped.


### 4.1 users — direct write
- **insert / replace / update** → **insert-if-absent by account**. If the account already exists (another sync got there first), **leave it untouched** — so post-seed **HR-field** `update`s (engName, companyName, dept/sect, roles, …) are intentionally **not** propagated (the company-wide user sync owns those, §9). Field mapping (confirmed):
- **`statusText` is the one exception.** It is **chat-originated** (set by the user inside the legacy chat, e.g. "In a meeting") and is **not** part of the HR dataset, so no other sync carries it. On a `statusText` `update` the transformer fans a `user_status_updated` event to **all** sites (global-visibility; see §4.1a) — otherwise a legacy status change during the migration window would be silently lost.

| Destination | Source |
|---|---|
| `ID` | generate `UUIDv7` on create (joins are by account, not id — source `_id` not preserved) |
| `Account` | `username` (**unique but mutable** — see §9) |
| `EngName` | `customFields.engName` |
| `ChineseName` | `customFields.companyName` |
| `SectID` / `SectName` | `customFields.sectId` / `customFields.sectName` |
| `DeptID` / `DeptName` | `customFields.deptId` / `customFields.deptName` |
| `StatusText` | `statusText` |
| `Roles` | `roles[]` (global; `admin` marker) |
| `SiteID` | `federation.origin` first label (absent ⇒ local; federated users **do** get local docs flagged `isRemote:true`) |
| `SubscriptionUser.IsBot` (consumer side) | `type == "bot"` (bot has `appId`); feeds room-type `botDM` and bot-DM detection |

- **No clean source** (set to zero-value, documented §9): `SectTCName`, `DeptTCName`, `EmployeeID`, `StatusIsShow`. These are owned by the external company-wide user sync, not this seed.
- **delete** → **skip for now**; deactivation is `active:false` (not a deletion) — that's the later signal.

#### 4.1a `statusText` fan-out (the one propagated user field)
A `statusText` `update` fans a **reused** `user_status_updated` `InboxEvent` (no new event type) to **every** site in `ALL_SITE_IDS`, including our own — a user's status is globally visible, and each site holds its own (federated) copy of the user doc, so all must converge. `StatusIsShow` stays nil (the company-wide sync owns it). inbox-worker's `UpdateUserStatus` applies it keyed by **account** (not siteId) under a **`statusUpdatedAt` high-water guard**, so an out-of-order or duplicate fan-out delivery can't regress the status; a missing user on a site is a logged no-op. An empty `ALL_SITE_IDS` (partial deployment / misconfig) → warn + Ack-skip per event (a startup hard-fail is the eventual form — deferred until the failure modes are known). This is the **only** post-seed user field the migration propagates and the **only** additional inbox-worker apply-path beyond §7's `handleMemberAdded` change.

### 4.2 rocketchat_rooms — inbox publish
- **insert / replace** → `room_sync` (full `model.Room` upsert). `Room.UpdatedAt` maps from source `_updatedAt` (`CreatedAt` from `ts`), zero-guarded to `now` when absent — this value is the **high-water mark** `inbox-worker`'s `UpsertRoom` guards on (`$lt`), so a zero would freeze the room after first sync.
- **update** (re-read full doc per §4.0, then diff):
  - name change → `room_renamed` **+** `room_sync` (the rename event updates subscriptions' denormalized name; `room_sync` converges the `rooms` doc — this transformer is the doc's only writer).
  - restricted / externalAccess change → `room_restricted` **+** `room_sync` (same split: subs' denormalized visibility + the room doc).
  - other field deltas → `room_sync`.
  - The companion `room_renamed`/`room_restricted` carry the source `_updatedAt` millis as their guard timestamp, matching the `room_sync` high-water mark.
- **delete** → **skip** — the app has no room deletion (no `DeleteRoom` anywhere; `rooms` is only ever upserted), and the delete event is un-actionable anyway (§4.0). A deleted source room's members are cleaned up via their subscription leaves; the room remains. **Deliberate constraint, not an omission.**
- **Type mapping** (source `t` is one of exactly `c,p,d,l,v` — confirmed by source-data team; new-stack `RoomType` is the closed set `channel,dm,botDM,discussion` per `pkg/model/room.go`):
  - `c` (public channel) / `p` (private group) → `channel` — **the new model has a single `channel` type with no public/private distinction**; both collapse, by design (not a dropped attribute).
  - **`p` with `prid` set** → `discussion` (a discussion is a `p` room carrying a parent-room id — check `prid` **before** the plain `p→channel` rule)
  - `d` (2 participants) → `dm`, or `botDM` if a participant's `users.type == "bot"` (requires a user-type lookup)
  - **Team rooms** (`teamId` present, with/without `teamMain`) → `channel`. The new model has **no team concept**: `teamId`/`teamMain` are dropped, team sub-rooms migrate as ordinary channels (documented gap, §9).
- **Type exclusion** (skip + metric + documented gap, §9) — these have **no** `RoomType` equivalent:
  - `l` (livechat/omnichannel) and `v` (voip) — non-conversational.
  - **Group DMs** — `d` rooms with **>2 participants**. The new stack's DM is strictly two-party (`idgen.BuildDMRoomID` / `model.BuildDMParticipants` require exactly two users); a 3+-party DM has no representable id. Skip.
  - We deliberately do **not** adopt the source team's fuzzier "rooms to ignore" heuristics (name-prefix `_`, message-count, etc.) — type-based exclusion only, deterministic for CDC.

### 4.3 rocketchat_subscriptions — inbox publish (full fidelity)
`member_added` creates a subscription with **defaults** (`rolesForType`, computed `Name`/`IsSubscribed`); real state comes from follow-up events. Source field mapping (confirmed by source-data team):

| Destination | Source field |
|---|---|
| `User.ID`, `User.Account` | `u._id`, `u.username` (unique index `{rid:1,'u._id':1}`) |
| `RoomID` | `rid` |
| `Roles` | `roles[]` (`owner`/`moderator`/`leader`/`user` — role-based ownership, no separate pointer) |
| `Muted` | `disableNotifications` (Company custom — authoritative all-off; **not** `muteGroupMentions`, which is @all/@here-only) |
| `Favorite` | `f` (absent ⇒ false) |
| `Alert` | `alert` (any unread content, not just mentions) |
| `LastSeenAt` | **`max(ls, lr)`** — the furthest point consumed by either path (`ls` scrolled cursor, `lr` explicit mark); minimizes false-unread, consistent with the advance-only (`$lt`) apply guard |
| `JoinedAt` | `ts` (set once on first join; re-join just flips `open` back to true) |
| `Name` | `fname` (friendly display name); `name` is the machine handle |
| `SiteID` | `federation.origin` first label (absent ⇒ local, §6) |
| **`IsSubscribed`** | **NOT from source** — inbox-worker computes it (`subscriptionIsSubscribed`: botDM-human ⇒ true, else false). Never mapped from `open`. |
| **`HasMention`, `ThreadUnread`** | **NOT from subscription CDC** — these are unread-state owned by the message pipeline (broadcast/notification workers). Their value at the checkpoint is set by the bulk-sync owner; ongoing changes during the tail come from the migrated message flow. A tail-created sub starts with no unread (correct default). |

> **Membership is binary in the new backend.** A subscription row exists ⟺ the user is a member; leaving **deletes** the row (`removeMember` → `member_removed` → `DeleteSubscriptionsByAccounts`). There is **no soft "hidden/unsubscribed-but-member" state** — `IsSubscribed` is a botDM marker, *not* a membership flag. So the source `open` toggle maps to the membership lifecycle, and the roles/read/favorite reset on leave **is** the correct new-backend semantics (rejoin starts fresh), not a fidelity loss.

- **insert / replace** → `member_added` **+** the state events that reproduce the source row:
  - `role_updated` ← `roles[]` (overrides the default)
  - `subscription_mute_toggled` ← `disableNotifications`
  - `subscription_favorite_toggled` ← `f`
  - `subscription_read` ← `max(ls, lr)` + `alert`
- **update** (re-read full doc per §4.0, then diff) → emit the matching event(s). **Membership leave/rejoin is an `open` toggle:**
  - `open` true→false → **`member_removed`** (deletes the row — correct: binary membership)
  - `open` false→true → **`member_added`** (re-subscribe; idempotent upsert)
  - mute/favorite/role/read changes → the single matching state event.
- **delete** (true row delete) → **un-actionable, skip + metric** (§4.0) — the event carries only the source `_id`, which doesn't map to the destination sub. Rare: leaving flips `open:false` (an update, handled above), so true deletes are the uncommon case.

The destination `Subscription` fields and how each is set are pinned in §8.

**D1 (decided):** `LastSeenAt = max(ls, lr)` — neither source field alone is correct (`ls`-only shows false unread after a mark-read-without-scroll; `lr`-only after a scroll-without-mark), so take the later of the two. Paired with source `alert` → `Alert`.

### 4.4 company_thread_subscriptions — inbox publish, transformer-resolved
Confirmed source schema: `_id`, `u` (`u._id`, `u.username`), `rid`, `parentMessage._id` (`tmid`), `lastMessage` (`_id`,`_updatedAt`), `createdAt`, `lastSeenAt`, `unreadMention`. Unique index `{'u._id':1, 'parentMessage._id':1}` — **one row per (user, thread)**.

The `thread_subscription_upserted` payload is the **full `model.ThreadSubscription`** and inbox-worker upserts it **verbatim** (keyed by `threadRoomId`+`userId`) — it resolves nothing. Field mapping + two foreign keys the source row lacks in new-stack form (resolved **in the transformer** before publishing):

| Destination | Source |
|---|---|
| `ParentMessageID` | `parentMessage._id` (`tmid`) |
| `UserAccount` | `u.username` |
| `LastSeenAt` | `lastSeenAt` |
| `HasMention` | `unreadMention > 0` |
| `CreatedAt` | `createdAt` |
| **`ThreadRoomID` (+ `RoomID`)** | resolve: lookup target `thread_rooms` by `parentMessage._id` (1:1). One lookup yields both IDs. `rid` cross-checks `RoomID`. |
| **`UserID`** | resolve: lookup target `users` by `u.username`. |
| `SiteID` | inherits the room's site (resolved thread_room), §6 (source row has no `federation.origin`). |

**Double dependency:** thread-subs need **users** *and* the **message migration's thread rooms**. If either lookup misses → **Nak** (bounded retry) until both land. (Unlike subscriptions, this resolution can't be pushed to inbox-worker, since in live federation the origin site already knows both IDs — resolving them is migration-specific.)

- **insert / replace** (created lazily on follow/first reply) → `thread_subscription_upserted` (resolved as above).
- **update** (re-read full doc per §4.0) → re-`thread_subscription_upserted` (idempotent upsert).
- **delete** (unfollow deletes the row — no flag) → **un-actionable, skip + metric (D2).** Two independent reasons: the delete event carries only the source `_id` (can't resolve `(threadRoomId,userId)`, §4.0), **and** there is no `thread_subscription_removed` inbox handler — the live stack doesn't federate unfollows either. A stale follow may linger during the cutover window and self-corrects once live. Recommended skip.

---

## 5. Ordering & retry — a safety net for the live-tail race

Upstream guarantees context is fully synced ≤ checkpoint, so a tailed subscription's user already exists for anything that predates the cut. The residual risk is the **live tail racing across collections**: a brand-new user and their subscription both created *after* the checkpoint stream on the connector's **own concurrent watchers** with no cross-collection ordering, so the subscription change can arrive first. Today `handleMemberAdded` **silently skips** (and Acks) a subscription whose user isn't present — a swallowed loss.

**Change:** `handleMemberAdded` returns an **error (→ Nak/redeliver)** instead of `continue`-skipping when a referenced user is missing. This is **the correct steady-state behavior**, not a migration hack — a cross-site `member_added` for an unknown user *should* wait, not drop — so it persists after the source is sunset. The existence gate is inbox-worker's *existing* `FindUsersByAccounts` (no new lookup, no transformer-side user check for subscriptions). With context synced upstream this rarely fires; it's cheap insurance for the post-checkpoint race window.

Requirements for that change:
- **Bounded `MaxDeliver`** on inbox-worker's consumer so a member_added for a user who never arrives eventually gives up + alerts (the exhaustion-signal lesson from the message path) rather than looping forever.
- **Idempotent on redelivery** — `BulkCreateSubscriptions` must be a guarded upsert (re-applying a partially-done batch can't create duplicates). Verify.
- **Partial-account batches** — live `member_added` can carry several accounts; "error if any missing" retries the whole event and re-upserts the present ones (fine given idempotency). Migration emits single-account events. Audit sibling handlers (`member_removed`, thread-sub) for the same skip-on-missing pattern and make them consistent.

Thread-subs heal order via the **transformer-side** Nak-retry in §4.4.

---

## 6. `siteId` stamping (documented rule)

`Subscription.SiteID` / `Room.SiteID` is the record's **home/origin site, invariant across replicas** — every replica of a subscription carries the same `siteId` regardless of where the doc is stored. So a federated copy must carry its **origin** site, not the local deployment.

`federation.origin` is authoritative and has **three** cases. The origin domain embeds the **site code** as its first label (e.g. `0030204.tchat-test.test.company.com` → `0030204`), and `SITE_ID` *is* that bare code:

```
origin = doc.federation.origin
siteId = (origin absent || origin == "local")  → SITE_ID            // this deployment's code
         else                                    → firstLabel(origin) // e.g. "0030204"
```

Applies to users, rooms, subscriptions (thread-subs inherit the room's site via the resolved thread_room). `"local"` is a sentinel synonym for absent — never used as a literal value. **`federation.domains[]` is ignored** (no target field on `model.Room`; the new stack re-derives cross-site reach from members). This rule is **documented and followed as-is**; any future change is a deliberate edit.

---

## 7. Required `inbox-worker` changes

**One** minimal change to the shared apply path:

1. **`handleMemberAdded` skip→error** on unknown user (§5) — currently `continue`-skips (handler.go:140-143); change to return an error so the event Naks/redelivers until the user lands. Live-safe (the correct cross-site behavior anyway).

No `MemberAddEvent` extension is needed: `IsSubscribed` is already computed correctly by inbox-worker (`subscriptionIsSubscribed`), and `HasMention`/`ThreadUnread` are unread-state owned by the message pipeline (not subscription federation) — see §4.3/§8. Earlier drafts proposed carrying these on an extended `member_added`; that's dropped.

No new event types; no new collections.

---

## 8. Destination `Subscription` field coverage

| Field | Set by |
|---|---|
| `Roles` | `member_added` (default) → **`role_updated`** (source roles) |
| `Muted` | **`subscription_mute_toggled`** |
| `Favorite` | **`subscription_favorite_toggled`** |
| `LastSeenAt`, `Alert` | **`subscription_read`** |
| `IsSubscribed` | inbox-worker computes (`subscriptionIsSubscribed`: botDM-human ⇒ true, else false) — **not** from source |
| `Name` | `member_added` (default) → **`room_renamed`** (channel rename) |
| `RoomType`, `SiteID`, `JoinedAt`, `HistorySharedSince` | `member_added` |
| `Restricted`, `ExternalAccess` | **rooms** migration (`room_restricted`), not per-sub |
| `HasMention`, `ThreadUnread` | **message pipeline** — unread-state, not subscription federation. Initial value at checkpoint owned by the bulk-sync owner; ongoing changes from the migrated message flow. A tail-created sub starts with no unread (correct). |

Every destination field has a faithful path.

### 8.1 inbox-worker handler coverage

Every inbox-worker apply-handler is either produced by the migration or intentionally not:

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
| `user_status_updated` | ✅ | user `statusText` change — chat-owned, fanned to all sites (§4.1a) |
| `thread_read` | ⚠️ **not emitted** | redundant: the thread-sub `lastSeenAt` is carried by `thread_subscription_upserted`; `Subscription.ThreadUnread` is message-pipeline-owned (§8) |

No handler is left silently unaddressed.

---

## 9. Not faithfully migrated (documented gaps — "no hidden gimmick")

The spec carries this list explicitly; nothing is silently defaulted:
- **`federation.domains[]`** — ignored; new model re-derives cross-site reach from members.
- **User delete / deactivation** — skipped for now.
- **Livechat (`l`) / VoIP (`v`) rooms** — skipped (no `RoomType` equivalent); metric on skip.
- **Group DMs** (`d` with >2 participants) — skipped (new stack has no group-DM type, §4.2); metric on skip.
- **Team grouping** (`teamId`/`teamMain`) — team rooms migrate as plain `channel`; the team relationship is dropped (no team concept in the new model).
- **Subscription `Restricted`/`ExternalAccess`** — sourced from the **rooms** migration, not the per-sub row (dependency, not a gap).
- **Subscription unread-state (`HasMention`, `Alert`, `ThreadUnread`)** — owned by the message pipeline, not subscription CDC; initial value at checkpoint owned by the bulk-sync owner (not a gap; §4.3/§8).
- **Thread-sub unfollow** (D2) — `delete` is un-actionable (only the source `_id`, §4.0) **and** there's no inbox removal handler (the live stack never federates unfollows either); skip + metric. **The only true "can't push" item.**
- **Subscription / room / user true `delete`** — un-actionable (only the source `_id`, no pre-image, destination doesn't key by source `_id`; §4.0). Rooms/users skipped by policy anyway; subscription leave arrives as an `open:false` update (handled).
- **User post-seed `update`s** — **HR fields** (engName, companyName, dept/sect, roles, …) not propagated (insert-if-absent; the company-wide user sync owns those). **Exception: `statusText`** is chat-owned and IS propagated (fan `user_status_updated` to all sites, §4.1a) — no other sync carries it, so dropping it would lose legacy status changes during the migration window.
- **Collection-level ops** (`drop`/`rename`/`invalidate`) — **out of scope, deferred** (§4.0); operator re-points the connector, not transformer logic.
- **User fields `SectTCName`/`DeptTCName`/`EmployeeID`/`StatusIsShow`** — no clean source in `users`; owned by the external user sync, left zero-valued by the seed.
- **`username`/account mutability** — the entire new stack joins by account; a source username rename during cutover would orphan rows until the next sync. Low-risk, documented.
- **DM/botDM subscription `Name`** — `member_added`'s `subscriptionName` derives a DM label from `RequesterAccount`, which the migration doesn't set, so migrated DM/botDM subscriptions land with an empty `Name` (channels use the room name and are fine). The DM display label is re-derived client-side from the counterpart's HR info, so impact is cosmetic; recorded as a known minor gap.

---

## 10. Service structure (decided)

A **separate sibling service** `data-migration/oplog-collections-transformer` consumes the operational-collection subjects on `MIGRATION_OPLOG_{site}` — distinct from the Cassandra/canonical message transformer. Shared infrastructure (oplogEvent decode, source lookup, disposition mapping, request-id, metrics) is **factored into a common pkg under `data-migration/`** and imported by both transformers rather than duplicated. (Rejected: extending the existing `oplog-transformer` to route the extra collections — the inbox/Mongo path is different enough that a single service would tangle two unrelated output mechanisms.)

---

## 11. Resolved items (was OPEN)
- **Room-type exclusion** — skip `l`, `v`, and group DMs (`d` >2 participants); §4.2 / §9.
- **Room-type mapping wrinkles** — discussions (`p`+`prid`), teams (`teamId`→`channel`), bot DMs (`users.type=="bot"`); §4.2.
- **D1 — read timestamp** — `LastSeenAt = max(ls, lr)`; §4.3.
- **D3 — `IsSubscribed`/`HasMention`/`ThreadUnread`** — *not* from subscription CDC. `IsSubscribed` is inbox-worker-computed (botDM marker); unread-state is message-pipeline-owned. Drops the `member_added` extension; §4.3/§7/§8.
- **"Can't push" audit** — vs inbox-worker's handler set, the only source change with no apply-path is **thread-sub unfollow** (D2 — also un-actionable per §4.0); room delete is moot. §9.
- **CDC op handling** — `replace` → same path as `insert` (re-classify + upsert); `update` → re-read full source doc; `delete` → un-actionable skip + metric; §4.0.
- **D2 — thread-sub unfollow** — skip + metric (un-actionable **and** no handler); §4.4/§9.
- **Collection-level ops** (`drop`/`rename`/`invalidate`) — out of scope, deferred; §4.0.
- **Service structure** — separate sibling + shared `data-migration/` pkg; §10.
- **Federation filters** for these collections resolve to **no drop** (§3) — recorded, not deferred.

**No open design items remain** — the spec is decision-complete pending source-engineer confirmation of the `SOURCE_DATA.md` assumptions (notably the room `Restricted`/`ExternalAccess` fields).

---

## 12. Disposition, idempotency, observability

Mirror the message transformer's conventions:
- **Disposition:** decode/contract-violation → `Term`; transient (lookup miss, target unavailable) → `Nak`; deliberate skip (out-of-scope collection/op, excluded room type) → Ack-without-count (`onSkipped{reason}`); `MaxDeliver` exhaustion → `Term` + metric (no silent JetStream drop).
- **Idempotency:** the path must tolerate JetStream redelivery and reprocessing across the checkpoint boundary (the bulk-sync owner may have applied the same row's state ≤ checkpoint). Leans on inbox-worker's monotonic guard fields (`UpsertRoom` UpdatedAt high-water mark, `rolesUpdatedAt`, `$lt` lastSeenAt, `$setOnInsert` on thread-subs) and the users insert-if-absent. Verify each.
- **Correlation:** stamp `request_id` at consume; propagate into the published `OutboxEvent` headers and the direct-write context.
- **Metrics:** processed/skipped/nak/term/exhausted by op + collection; thread-sub resolution misses; user-seed insert vs already-present.

---

## 13. Testing
- **Unit** (mocked publisher + target store + source lookup): the op→event mapping per collection (rooms create/rename/restrict/skip-delete; subscription full-fidelity insert + delta-classified updates + member_removed; thread-sub FK resolution + double-dependency Nak; users insert-if-absent vs already-present); `siteId` stamping (absent / `"local"` / remote-domain first-label); disposition mapping.
- **inbox-worker** unit: `member_added` error-on-unknown-user (the one apply-path change); idempotent redelivery.
- **Integration** (testcontainers Mongo + NATS): source CDC → INBOX → inbox-worker → per-site Mongo for a room + its subscriptions; users-before-subscriptions ordering heals via retry; thread-sub resolves against a message-migration-created thread room.

---

## 14. Footprint / teardown
Same blast-radius discipline as the message path: the whole `data-migration/` folder is deletable at source retirement. The single `inbox-worker` change (`member_added` error-on-missing-user) is **retained** — it's correct live-federation behavior, not migration scaffolding. Document it as such so the cleanup PR doesn't revert it.
