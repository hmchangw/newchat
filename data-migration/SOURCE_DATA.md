# Source data shape (legacy RocketChat)

## 1. Source document (`rocketchat_message`)

Example document (values rotated/sanitized):

```js
{
    _id: 'aB3kD9fH2jL5mN7pQ',
    t: 'au',
    u: {
        _id: 'xY8wV2tR6sU4qP0nM',
        username: 'p_demoadmin'
    },
    ts: ISODate('2024-01-15T09:00:00.000Z'),
    msg: 'p_newmember',
    _updatedAt: ISODate('2024-01-15T09:05:00.000Z'),
    unread: true,
    rid: 'rM4nB7vC2xZ9kJ5hG',
    groupable: false,
    federation: {
        origin: 'site-a.example.internal'
    }
}
```

### `t` — message type

| `t` | Meaning |
|---|---|
| `null` (absent) | Normal message |
| `room_changed_avatar` | Avatar changed |
| `room_changed_description` | Description changed |
| `room_changed_name` | Name changed |
| `room_changed_privacy` | Privacy changed |
| `user_added` | User added |
| `user_removed` | User removed |
| `user_left` | User left |
| `msg_encrypted` | Encrypted (requires decryption) |
| `subscription_role` | Role changed |

### Key fields

| Field | Notes |
|---|---|
| `_id` | PK; used for deduplication (always present) |
| `msg` | Meaningful when `t` is null; localized system text when `t` is set |
| `ts` | Insert timestamp — ordering, immutable |
| `_updatedAt` | Modification timestamp — edit detection |
| `federation.origin` | Remote server origin (if federated) |
| `unread` | Legacy flag — do not rely on (use receipts / group mentions) |

## 2. Change event structure

### 2A. Common fields

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | Event id (not the doc id). Used for `resumeAfter`. Not the msg id. |
| `operationType` | str | `insert` \| `update` \| `delete` \| `replace` \| `drop` \| `rename` \| `dropDatabase` \| `invalidate` |
| `clusterTime` | Timestamp | Monotonic; different values = distinct ops |
| `ns.db` / `ns.coll` | str | DB / collection names |
| `wallTime` | Date | Applied wall-clock time |
| `documentKey` | `{_id:<val>}` | Affected doc's `_id` (RC msg id) |
| `txnNumber` (Long) / `txnId` (UUID) | — | Present when inside a transaction |
| `userName` | str | Authenticated user that executed the op |

### 2B. Insert event
- **Insert-only:** `fullDocument` — the complete, newly-inserted doc.
- **Never:** `updateDescription`, `previousDocument`, `fullDocumentBeforeChange`.

### 2C. Update event
- `updateDescription` — **always**. The diff: `updatedFields` (set/modified), `removedFields` (deleted paths), `truncatedArrays` (`$pop`/`$slice`).
- `fullDocument` — only with `updateLookup`/`whenAvailable`. The post-update doc (re-read; may reflect concurrent writes).
- `previousDocument` — only with `showExpandedEvents:true` (5.0+). The pre-update doc.

### 2D. Delete event
- `fullDocument` — **never** (absent; cannot re-read).
- `previousDocument` — only with `showExpandedEvents:true` (5.0+). The pre-deletion doc — **critical for deleted content**. Without it, only `documentKey._id` is available; no way to reconstruct `msg`.

### 2E. Replace event
- `updateDescription` — absent (no field-level diff).
- `fullDocument` — the new version (with the `fullDocument` option).
- `previousDocument` — the old version (with `showExpandedEvents:true`).

---

# Collections migration — source schema (assumptions, for source-engineer cross-check)

> This section is the migration team's **current understanding** of the operational
> source collections read by the collections path (`oplog-collections-transformer`,
> design: `docs/superpowers/specs/2026-06-16-oplog-transformer-collections-design.md`).
> **Every "Assumed" row drives a write into the new system — please correct anything wrong.**
> Legend: ✅ confirmed by source team · ❓ assumption awaiting confirmation · ⛔ deliberately ignored.

## Conventions assumed across these collections
- ✅ **`federation.origin`** is authoritative. **Absent** ⇒ record is **local**. **Present** ⇒ a federated peer domain whose **first dotted label is the site code** (`0030204.tchat-test.test.company.com` ⇒ `0030204`).
- ❓ `federation.origin` is never the literal `"local"` (we treat absent ⇒ local).
- ✅ Each site's source DB already holds its **federated copies**; we migrate the full local source with **no** drop-filter.

## 3. `rocketchat_rooms`

| Source field | Type | Interpretation | Status |
|---|---|---|---|
| `_id` | string | Room id | ✅ |
| `t` | string | Room type — **only** `c`,`p`,`d`,`l`,`v` exist | ✅ |
| `prid` | string (opt) | Parent room id — **present ⇒ discussion** (`t` is `p`) | ✅ |
| `teamId` | string (opt) | Room belongs to a Team | ✅ |
| `teamMain` | bool (opt) | True only on a team's **primary** room | ✅ |
| `name` | string | Machine/handle name | ❓ |
| `fname` | string | Friendly display name | ❓ |
| `uids` / `usernames` | array | Members; for `t:d` length **can exceed 2** (group DM) | ✅ |
| `u` | object | Creator (`u._id`, `u.username`) | ❓ |
| `ts` / `_updatedAt` | date | Created / last-updated | ❓ |
| `restricted` | bool (opt) | **Authoritative restriction flag** (Company custom; absent ⇒ false). Confirmed on TKMS. RC's `ro` (read-only/announcement) is a **different concept** — deliberately ignored | ✅ |
| **external/federation access** | ? | **Which field is authoritative for "external access allowed"?** | ❓ |
| `federation.origin` | string (opt) | Origin site | ✅ |
| `federation.domains[]` | array | Member domains, service-synced, may be stale | ✅ ⛔ |

Type mapping logic to sanity-check: `c`/`p` (no `prid`) → one channel type (no public/private split); `p`+`prid` → discussion; `d` (2 participants) → dm (botDM if a participant is a bot); `d` (>2) → **skip** (no group DM); `l`/`v` → **skip**; team rooms → plain channel (`teamId`/`teamMain` dropped).

## 4. `rocketchat_subscriptions`

One row per (user, room). ✅ Unique index `{ rid:1, 'u._id':1 }`.

| Source field | Type | Interpretation | Status |
|---|---|---|---|
| `u._id`, `u.username` | string | Member identity | ✅ |
| `rid` | string | Room id | ✅ |
| `open` | bool | **Membership active.** Leave ⇒ `open:false` (no delete); re-join ⇒ true | ✅ |
| `ts` | date | Join time (set once, stable across re-joins) | ✅ |
| `roles[]` | string[] | `owner`/`moderator`/`leader`/`user` (role-based ownership) | ✅ |
| `ls` | date | Last **seen** (scrolled cursor) | ✅ |
| `lr` | date | Last **read** (explicit mark) | ✅ |
| `alert` | bool | True if **any** unread content (not just mentions) | ✅ |
| `userMentions` / `groupMentions` | int | Unread `@user` / `@all`,`@here` counts | ✅ |
| `tunread[]` | string[] | Parent-message ids (`tmid`) of threads with any unread | ✅ |
| `tunreadGroup[]` / `tunreadUser[]` | string[] | …group-mention / direct-mention variants | ✅ |
| `disableNotifications` | bool | **Company custom — authoritative mute (all-off)** | ✅ |
| `muteGroupMentions` | bool | `@all`/`@here` only (**not** our mute flag) | ✅ |
| `f` | bool (opt) | Favorited (absent ⇒ false) | ✅ |
| `favoritedAt` | date (opt) | Last favorite time. Exists at source (TKMS) but **unused by CDC** — per the agreed guard mapping below, all guards derive from `_updatedAt` | ✅ ⛔ |
| `name` / `fname` | string | Machine name / friendly display name | ✅ |
| `federation.origin` | string (opt) | Origin site (assumed consistent with room) | ✅ ❓ |

Derived: "has mention" = `userMentions>0 || groupMentions>0`; "muted" = `disableNotifications`; **read timestamp (`lastSeenAt`) = `max(ls, lr)`** (resolved per design D1 — the furthest point consumed by either the scrolled cursor or the explicit mark-read).

**Guard timestamps (agreed with source team, supersedes the earlier conditional/null mapping):** every
destination high-water guard derives uniformly from the source **`_updatedAt`** — `rolesUpdatedAt`,
`muteUpdatedAt`, `favoriteUpdatedAt` (subscriptions) and `nameUpdatedAt`, restricted-guard (rooms).
No null-when-false conditional, no `favoritedAt` source. Note: the canonical restricted guard field
(`restrictedUpdatedAt`) is not in the destination codebase yet; inbox-worker currently applies
`room_restricted` via `visibilityUpdatedAt` — accepted until the rename lands in main.

## 5. `company_thread_subscriptions`

One row per (user, thread). ✅ Unique index `{ 'u._id':1, 'parentMessage._id':1 }`.

| Source field | Type | Interpretation | Status |
|---|---|---|---|
| `_id` | string | Row id | ✅ |
| `u._id`, `u.username` | string | Follower identity | ✅ |
| `rid` | string | Room id (matches parent room) | ✅ |
| `parentMessage._id` | string | Thread root message id (`tmid`) — the thread key | ✅ |
| `lastMessage._id` / `._updatedAt` | string/date | Last message in thread | ✅ |
| `createdAt` | date | Row creation (lazy — on follow/first reply) | ✅ |
| `lastSeenAt` | date | Last-read timestamp for the thread | ✅ |
| `unreadMention` | int | Thread mention/unread count | ✅ |

Lifecycle: created lazily; **unfollow deletes the row** (no soft-delete); no `federation.origin` (site inherited from room/user). **Open:** please share a redacted sample doc to confirm nothing is missed.

## 6. `users`

| Source field | Type | Interpretation | Status |
|---|---|---|---|
| `_id` | string (17-char base62) | Stable user id | ✅ |
| `username` | string | **Account id — unique but mutable** | ✅ |
| `type` | string | `user` or `bot` (bot has `appId`); no other non-human types | ✅ |
| `appId` | string (opt) | Present on bot/app accounts | ✅ |
| `name` | string | Display name | ✅ |
| `customFields.engName` / `companyName` | string | English / Chinese name | ✅ |
| `customFields.deptId` / `deptName` | string | Department id / name | ✅ |
| `customFields.sectId` / `sectName` | string | Section id / name | ✅ |
| `customFields.appId` / `appName` | string | App id / name | ✅ |
| `hrInfo` | `ICompanyUser[]` | HR directory records | ❓ (not consumed yet) |
| `statusText` / `status` | string | Status message / presence | ✅ |
| `roles[]` | string[] | Global roles (`admin` marker) | ✅ |
| `active` | bool | Deactivation ⇒ `active:false` (no deletion) | ✅ |
| `isRemote` | bool | True on local docs of **federated** users | ✅ |
| `federation.origin` | string (opt) | Origin site (absent ⇒ local) | ✅ |
| **employee id** | ? | **Where does an employee id live — is it `username`?** | ❓ |
| **Traditional-Chinese dept/sect names** | ? | Is there a TC variant of `deptName`/`sectName`? | ❓ |

Seeded (insert-if-absent, keyed by account): `username`, `engName`, `companyName`, dept/sect ids+names, `roles`, `statusText`, site, bot flag. Everything else is owned by the company-wide user sync.

Post-seed **updates**: HR fields are **not** re-propagated (the company-wide sync keeps them current). The **one exception is `statusText`** — it is chat-originated (not in the HR dataset), so a live `statusText` change fans a `user_status_updated` event to all sites (design §4.1a); without it, legacy status changes during the migration window would be lost.

## Explicitly **not** migrated
`federation.domains[]`; livechat (`l`) / voip (`v`) rooms; group DMs (`d`>2); team grouping (`teamId`/`teamMain`); user deactivation/deletion; thread-sub unfollows during cutover. Flag any of these you'd expect to matter.


## Collection for direct transfer:
rocketchat_avatar, company_apps_v, company_bot_cmd_men , company_tsso_tokens, rocketchat_uploads, company_bot_authorization, ufsTokens, user_devices

Handled by **`oplog-direct-transfer`**: copied verbatim (whole doc, same `_id`) into the same-named
new-stack collection, mirroring insert/update/replace/delete. Metadata only — the actual file/blob
bytes for `rocketchat_uploads`/`ufsTokens`/`rocketchat_avatar` (UFS/GridFS) are a separate owner's
concern. See `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md`.

## 7. `company_room_members` — open questions (room-member migration)

> For the source engineers / migration owner. Destination: new-stack `room_members`
> (`{_id, rid, ts, member:{id, type: individual|org, account}}`), written **directly** by
> `oplog-collections-transformer` (decision: no room-worker involvement). Please answer inline
> under each question; a few pasted sample documents are worth more than prose.

**Q1 — Document shape.** Is `company_room_members` one document per (room, member), or one document
per room containing a members array? What are the exact field names for the room id, the member
id, and the member kind? Please paste 2–3 real (redacted) sample docs — ideally one org entry and
one individual entry if both exist.

**Answer:**
it is one document per (room, member) pair
_id, rid, member: {type: , id:, }ts federation: {origin:  }

key fields:  rid (room id), member.type org|individual|app|user member.id _id or HR org ID member.username (individual only) and ts


**Q2 — `_id` format.** What does `_id` look like, and is it deterministic/composite (e.g.
`{rid}:{orgId}`) or opaque (ObjectId/random)? Context: a change-stream `delete` carries only the
`_id` of the already-deleted doc — if the natural key `(rid, member kind, member id)` can be
derived from `_id`, deletes map directly; if not, we must persist the source `_id` on each target
doc to route deletes.

**Answer:**
Opaque, both code paths use RandomID()
no deterministic composition
must persist the source _id on target docs to resolve deletes, without showExpandedEvents: true you lose the triple on hard delete



**Q3 — Contents: orgs only, or individuals too?** Does the collection hold only org/department
memberships, or also individual user entries? Context: the new-stack reader
(`room-service.ListRoomMembers`) returns *only* `room_members` rows once a room has any — the
subscriptions fallback then stops. If the source is org-only, rooms would lose their individual
members from the member list after migration unless individual entries are synthesized; if the
source holds both, a faithful copy is complete as-is.

**Answer:**
both org and individuals
.member.type helps here


**Q4 — Org member id semantics.** For an org entry, what exactly is the member identifier (HR
org/dept id?), and does it match the ids used by `company_hr_acct_org` / the HR org sync? (The
new-stack enrichment joins `member.id` against the HR org data — the ids must line up.)

**Answer:**
identical to company_hr_acct_org.orgs[].id no mapping or transfromation is applied.

**Q5 — Mutation pattern.** How does the legacy app maintain this collection — insert/delete only,
or in-place updates too? Are removals hard deletes, or a soft-delete flag? Any TTL/out-of-band
cleanup that would bypass the change stream?

**Answer:**
insert/delete only, hard deletes
insert + hard delete only . no in-plce updates. No soft-delete flag.

no updateone/updatemany calls
change stream: we see insert and delete events only no updates


**Q6 — Timestamp.** Which source field (if any) records when the member was added? The target's
`ts` drives the member-list sort order.

**Answer:**
ts filed. set at insert time. insertation timestamp no seperate updated timestamp. 



**Q7 — Bulk-migration mapping.** The bulk owner will populate `room_members` for state ≤ the
checkpoint. What mapping will they apply (field mapping, `_id` strategy, org/individual handling)?
Our CDC tail must apply the **identical** mapping or the data diverges at the checkpoint boundary.

**Answer:**
do we need this answer?
Bulk is not our job.


**Q8 — Volume/churn.** Rough doc count and change rate (events/day)? Only used to sanity-check
that the sequential consumer is sufficient.

**Answer:**
will see later. This question can be asked about any collection we handle, we dont need this answer.

### §7 finding — `member.type` value-set mismatch (decision recorded 2026-07-13)

**Finding:** the legacy collection carries **four** `member.type` values — `org | individual | app | user` —
while the new-stack `room_members` schema defines exactly **two** (`individual | org`). The semantics of
legacy `app` and `user` entries are not yet confirmed (they come from two different legacy code paths).

**Decision:** the transformer maps ONLY the two types whose meaning is known — `org` and `individual`.
**Any other `member.type` value** (the known `app`/`user`, or anything unexpected) is **error-logged and
skipped** (Ack + `skipped` metric with a distinct reason label, so the volume is visible) — a catch-all
rule, not an enumerated skip-list. These entries are NOT migrated for now; where `app`/`user` map will be
decided in a follow-up once their semantics are confirmed. Anyone reading counts: skipped ≠ lost — the events remain
in the source collection and can be re-migrated once mapped.

**Individual-entry mapping (code-anchored, for the record):** target `member.account` ← legacy
`member.username` (the reader's enrichment joins individuals on `account`); target `member.id` ← the
**new-stack** user `_id` resolved via the transformer's existing `FindUserID(account)` (room-worker's
dedup queries match `member.id` against new-stack user ids — carrying the legacy user `_id` would break
them). Unresolvable user at event time ⇒ Nak-retry until the user is seeded (thread-subs precedent).

---

# §8. users / sessions / hr_employee migration — information required (oplog-user-session-transformer)

> Questions from the migration team for the **user/session/hr lanes** (design:
> `docs/superpowers/specs/2026-07-18-oplog-user-session-transformer-design.md`).
> Please fill in each **Answer:** below — items marked **[blocking]** gate implementation of that
> lane; the rest only tighten defaults. Same legend as above (✅ / ❓ / ⛔).

## §8A — Legacy sessions collection

**Q1 — Collection name. [blocking]** What is the exact name of the legacy sessions collection the
connector must watch (`WATCH_COLLECTIONS` entry / `SESSIONS_COLLECTION` env)?

**Answer:**


**Q2 — Doc shape. [blocking]** What does one session doc look like? Specifically: is it **one doc
per user** carrying a resume-token **list**, or **one doc per session/token**? Please paste a
sanitized example doc, and name the exact field path of the token list (we currently assume a
Meteor-style `…resume.loginTokens: [{hashedToken, when}, …]`).

**Answer:**


**Q3 — Token encoding + timestamp semantics. [blocking]** Are stored tokens already
**base64(SHA-256(rawToken))** (the new stack keys `sessions._id` by exactly that), or raw/other?
And is the per-token timestamp (`when`?) the token's **issue time** (we map it to
`sessions.issuedAt`, unix ms)?

**Answer:**


**Q4 — User reference.** Which field on the session doc identifies the owner, and is it the
`users.username` (account) or the legacy `users._id`? (We resolve the new-stack
`userId`/`siteId`/`roles` from the **target** users collection by account.)

**Answer:**


**Q5 — Federation marker.** Do session docs carry `federation.origin` (or any foreign-origin
marker)? This only decides whether the connector-side `$match` can pre-drop foreign sessions —
the transformer independently skips sessions whose owner's home site is not local.

**Answer:**


**Q6 — Mutation pattern.** How does the legacy app maintain this collection — in-place updates to
the token list, doc replaces, hard deletes on logout/expiry? Any TTL/out-of-band cleanup that
bypasses the change stream? (Current plan is **upsert-only**: token additions migrate; removals /
revocations are deliberately ignored during the cutover window.)

**Answer:**


## §8B — `users` status fields

**Q7 — Status show-flag field.** The new stack has `statusIsShow` (bool) next to `statusText`.
Which **source** field on the legacy `users` doc backs it — and if none exists, confirm the
migration should carry `statusText` only. (This gates both the update-delta trigger and the
insert-time mapping.)

**Answer:**


## §8C — `company_hr_acct_org` → `hr_employee`

**Q8 — Doc shape + field mapping. [blocking]** Please paste a sanitized `company_hr_acct_org`
example doc and identify the fields that map to the destination `hr_employee` row:
`account` ← ? · `employeeId` ← ? · `siteId` (home-site assignment) ← ?. Is it one doc per
account, and is `orgs[]` relevant to the employee row or only to room-member enrichment?

**Answer:**


**Q9 — Mutation pattern.** insert/update/replace/delete pattern for `company_hr_acct_org`
(HR-sync rewrite cadence? hard deletes on leaver?). Until answered, source **deletes are skipped**
(not mirrored to `hr_employee`).

**Answer:**

