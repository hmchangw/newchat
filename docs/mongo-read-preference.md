# MongoDB Read Preference

How this codebase offloads staleness-tolerant reads onto MongoDB secondaries.
The shared plumbing lives in `pkg/mongoutil`; each participating service opts in
via a `MONGO_READ_PREFERENCE` env var. This doc is the reference for which
collection reads run where, and why.

**Golden rule:** writes and index creation always target the primary — read
preference never routes writes. A read may go to a secondary only if it is pure
display / list / browse / badge / fan-out. Any read feeding an authorization
decision, a dedup/existence guard, a read-modify-write, or a read-your-own-write
stays on the primary.

---

## 1. Mechanism

`pkg/mongoutil` exposes three pieces:

- `ParseReadPreference(string) (*readpref.ReadPref, error)` — maps a config
  string (`primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`,
  `nearest`, case-insensitive) to a driver read preference. Empty ⇒ `primary`.
- `Connect(ctx, uri, user, pass, WithReadPreference(rp))` — binds a
  **client-level** read preference, inherited by every collection handle.
  `WithReadPreference` composes with `WithObservability`; a fixed
  secondaryPreferred read client is also available as `ConnectRead`.
- `Collection[T].WithReadPreference(rp)` — clones a wrapper onto `rp` without
  mutating the base handle, for **per-read-site** routing. A nil `rp` returns the
  receiver unchanged.

Every participating service reads `MONGO_READ_PREFERENCE` (default
`secondaryPreferred`), validated at startup. `secondaryPreferred` degrades to the
primary when no secondary is available, so it is a no-op on a standalone Mongo
(read preference is ignored without a replica set) and safe for local/dev.

Two tiers, by service shape:

- **Tier 1 (client-level):** wholly read-tolerant services set the preference on
  the client. All reads go to a secondary except handles pinned back to primary.
- **Tier 2 (per-read-site):** mixed readers keep the client on **primary** and
  route only individual safe reads to a secondary via a parallel handle. For the
  raw store (room-service) that is a second `*mongo.Collection`; for the wrapper
  repos (user-service) it is `Collection[T].WithReadPreference`.

---

## 2. Tier 1 — client-level `secondaryPreferred`

All reads go to a secondary except the pinned handles.

| Service | Collections read → **secondary** | Pinned → **primary** (why) |
|---|---|---|
| history-service | `rooms`, `subscriptions`, `thread_rooms`, `thread_subscriptions`, `apps`, `users` | DEK store (`atrest.CollectionName`) — decryption must not miss an unreplicated key |
| search-service | `apps` | — |
| portal-service | `users` (directory) | — |
| broadcast-worker | `rooms` (metadata), `subscriptions`, `thread_rooms`, `users` | `rooms` **via roomkeystore** — encryption must not miss a fresh/rotated key |
| notification-worker | `subscriptions`, `thread_rooms`, `rooms` (meta backfill) | — |

broadcast-worker is read-only against MongoDB — its room/subscription MongoDB
writes (`rooms.lastMsgAt`/`lastMsgId`, subscription `lastSeenAt`, `hasMention`)
moved to `room-state-worker`. The reads above are unaffected by that split.

The DEK store and roomkeystore pins matter because those keys are written by
*other* services and read here to decrypt / encrypt; a lagging secondary could
miss a just-created key. Both are pinned even when the client default is
`secondaryPreferred`.

---

## 3. Tier 2 — client on `primary`, safe reads opted to secondary

### user-service

| Collection | → **secondary** (reads) | → **primary** (reads) |
|---|---|---|
| `subscriptions` | count, DM, by-room-id, active-list | paged subscription list (`AggregateSubscriptions`/`FindChannelsByMembers`) — must reflect latest changes; `GetAppSubscription` (dedup guard before create) |
| `apps` | `ListApps`, `GetAppsByAssistants` | `GetApp` (gates create-vs-reactivate) |
| `fab_domain_mapping` | `ListAppCategories` | — |
| `users` | `GetUserStatus`, `GetHRInfoByAccounts` | `GetUserSettings` (read-your-own-write) |
| `thread_subscriptions` | — | `ListByAccount` / `ListByAccountInRooms` badge tally — must reflect latest changes |

### room-service

room-service is the heaviest reader, and several store methods are called from
both safe and must-primary contexts — so the raw store keeps its primary handles
and adds parallel secondary-bound handles used only by the uniformly-safe reads.

| Collection | → **secondary** (reads) | → **primary** (reads) |
|---|---|---|
| `subscriptions` | `ListMemberStatuses`, `ListMentionableSubscriptions`, `ListReadReceipts`, `ListRoomBotApps` | membership/counts aggregations, `FindDMSubscription`, `ListSubscriptionsByRoom` (event fan-out / room-restrict after membership writes), messageRead RMW/floor reads |
| `users` | `GetUserSiteID`, `ListOrgMembers`, `FindUsersByAccounts` | `GetUser` (platform-admin authz), `CountNewMembers`, `FindExistingAccounts/OrgIDs` |
| `rooms` | `ListRoomsByIDs` | `GetRoom` (authz / restrict RMW / existence gate) |
| `thread_rooms` | `GetThreadRoomInfos` | `GetThreadRoomByID` |
| `thread_subscriptions` | `ListThreadReadReceipts` | `GetThreadSubscriptionByParent`, floor RMW reads |
| `apps` | `ListDefaultChannelTabApps` | `GetApp` (bot existence in add-members) |
| `bot_cmd_menu` | `ListActiveCmdMenus` | — |
| `room_members` | — (no secondary handle) | all reads (membership authz) |
| `teams_meetings` | — (no secondary handle) | all reads (dedup + read-after-write) |

---

## 4. Write-only services

`room-state-worker` derives its writes purely from the canonical event — it never
reads MongoDB to decide anything — so it deliberately has no `MONGO_READ_PREFERENCE`
config field at all, not even pinned to `primary`. Its writes (via `BulkWrite`) always
target the primary regardless; there is no read path for a preference to apply to.

## 5. Operational notes

- **Requires a replica set.** On a standalone Mongo the preference is ignored;
  the offload only happens against a replica set with provisioned secondaries.
- **Replication lag is a correctness signal.** Moving reads to secondaries trades
  a bounded staleness window for primary offload — monitor and alert on lag.
- **Adding a new read.** Classify it first (see the golden rule). Default a new
  read to the primary handle; move it to the secondary handle only once you have
  confirmed it is display-only with no authorization/dedup/read-your-own-write
  dependency. When touching a service that reads room encryption keys or DEKs,
  keep those handles pinned to the primary.
