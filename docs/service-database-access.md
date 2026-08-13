# Service Database Access

Per-service inventory of datastore reads and writes. One row per (service, datastore).
Derived from each service's store interface and its concrete implementation.

**Datastores:** MongoDB (operational), Cassandra (message history), Valkey (cluster-mode
cache / presence state), MinIO (object storage), Elasticsearch (search indices).

NATS/JetStream is a message bus, not a datastore, and is out of scope here. Process-local
LRU caches (`pkg/userstore`, `pkg/roommetacache` L1, `history-service/internal/readcache`,
`broadcast-worker/keycache`) are noted only where they front a listed store.

## Services

| Service | Store | Reads | Writes |
|---|---|---|---|
| **admin-service** | MongoDB | `users` (search by site/account, auth lookup), `admin_audit` (list w/ filter) | `users` (create, update fields, password + force-change, `active=false`), `admin_audit` (append), `sessions` (`DeleteMany` on password change / deactivate, in a transaction) |
| **auth-service** | — | none | none |
| **bot-message-handler** | MongoDB | `subscriptions` (by room+user), `rooms` (by id), `users` (by id), member-id list | none |
| **bot-message-worker** | Cassandra | — | `messages_by_room`, `messages_by_id`, `thread_messages_by_thread` (INSERT); `messages_by_id` + `messages_by_room` (UPDATE thread counters) |
| **bot-message-worker** | MongoDB | `room_data_keys` (DEK, only when at-rest encryption is enabled) | none |
| **bot-room-service** | MongoDB | `rooms` (by id, `encKey` projection), `users` (by id), `subscriptions` (member accounts) | `rooms` (insert; `encKey` set / set-with-version / rotate), `subscriptions` (upsert, delete) |
| **botplatform-service** | MongoDB | `users` (by account), `sessions` (by token hash), `subscriptions` (bot sub in room, bot DM sub) | `sessions` (insert; delete beyond per-account cap) |
| **botplatform-service** | Valkey | idempotency keys (request-replay middleware) | idempotency keys |
| **broadcast-worker** | MongoDB | `rooms` (meta), `subscriptions` (room member list, `historySharedSince`), `thread_rooms` (thread followers), `users` (LRU-cached), `rooms.encKey` (pinned to primary) | `rooms` (last message id/time, `mentionAll`), `subscriptions` (set mentions, advance `lastSeenAt`) |
| **broadcast-worker** | Valkey | room-meta L2 cache | room-meta L2 cache (write-through, TTL) |
| **client-update-service** | MinIO | version blob `Open` | version blob `Put`; bucket create at startup |
| **history-service** | Cassandra | `messages_by_room`, `messages_by_id`, `thread_messages_by_thread`, `pinned_messages_by_room` (paged reads, by-id, thread slices, pins) | `messages_by_id` / `messages_by_room` / `thread_messages_by_thread` / `pinned_messages_by_room` (edit content, soft delete, pin/unpin, add/remove reaction); `pinned_messages_by_room` INSERT/DELETE |
| **history-service** | MongoDB | `rooms` (times, min last-seen, user count), `subscriptions` (access check, `historySharedSince`), `thread_rooms`, `thread_subscriptions`, `apps` (bot display name), `users`, `room_data_keys` (DEK, primary read pref) | none |
| **hr-sync-worker** | MongoDB | `hr_employee`, `users` (change detection for upserts) | `hr_employee` (upsert batch), `users` (upsert identities, mark Teams employees quit) |
| **inbox-worker** | MongoDB | `users` (by accounts) | `subscriptions` (create / bulk create / delete by accounts; roles, read, mute, favorite, open — each under an ordering guard), `rooms` (upsert guarded by `updatedAt`), `thread_subscriptions` (upsert, delete, thread-read, bulk read-all) |
| **media-service** | MongoDB | `users` (by account, employee id, bot site), `subscriptions` (room site/type/name, membership check), `avatars` (by subject), `custom_emojis` (by shortcode, list per site) | `avatars` (set bot avatar), `custom_emojis` (upsert, delete) |
| **media-service** | MinIO | avatar / custom-emoji objects (bucket `avatars` by default) | avatar / custom-emoji objects |
| **message-gatekeeper** | MongoDB | `subscriptions` (by account+room), `rooms` (meta), `users` (LRU-cached) | none |
| **message-gatekeeper** | Valkey | room-meta L2 cache | room-meta L2 cache (write-through, TTL) |
| **message-worker** | Cassandra | `messages_by_id` (sender, quoted-parent snapshot, `createdAt`) | `messages_by_room`, `messages_by_id`, `thread_messages_by_thread` (INSERT); `messages_by_id` + `messages_by_room` (UPDATE thread counters, parent `threadRoomId`) |
| **message-worker** | MongoDB | `users`, `teams_user` (HR identity), `subscriptions` (`historySharedSince`), `thread_rooms` (by parent message), `room_data_keys` (DEK) | `thread_rooms` (create, last message, reply accounts), `thread_subscriptions` (insert, upsert, mark mention, advance last-seen), `subscriptions` (advance last-seen) |
| **notification-worker** | MongoDB | `subscriptions` (member fan-out set), `thread_rooms` (followers), `rooms` (meta), `users` (notification settings, primary read pref) | none |
| **notification-worker** | Valkey | room member-list cache, room-meta L2 cache | room member-list cache (set / invalidate), room-meta L2 cache |
| **outbox-worker** | — | none (drains the OUTBOX JetStream stream only) | none |
| **portal-service** | MongoDB | `users`, `hr_employee` (directory list, by account) | none |
| **push-notification-service** | — | none | none |
| **room-service** | MongoDB | `rooms`, `subscriptions`, `thread_subscriptions`, `thread_rooms`, `users`, `apps`, `bot_cmd_menu`, `room_members`, `teams_meetings`, `room_data_keys` — membership checks, member/mention listings, counts, read receipts, section ordering, DM lookup, thread info | `rooms` (visibility, min user last-seen), `subscriptions` (read state, mute, favorite, section move/rebalance, open, owner role, restriction apply, thread-unread clear), `thread_subscriptions` (read, clear for account), `thread_rooms` (min user last-seen), `teams_meetings` (insert) |
| **room-worker** | MongoDB | `rooms` (meta, key `encKey`), `subscriptions` (by account+room, DM pair, accounts, by room), `users`, `apps`, `room_members` (existence, org members), `thread_rooms` / `thread_subscriptions` (followers) | `rooms` (create, rename, member-count delta/reconcile, cross-site flag, `encKey` set/rotate), `subscriptions` (bulk create, delete, delete by accounts, remove role, rename), `room_members` (bulk create, delete), `thread_rooms` (pull followers), `thread_subscriptions` (delete), `room_data_keys` (ensure DEK) |
| **room-worker** | Valkey | — | room-meta L2 cache bust (delete on room change) |
| **search-service** | Elasticsearch | message / spotlight / spotlight-org / user-room indices (query + user-room doc) | none |
| **search-service** | MongoDB | `apps` (name search, by assistant), `subscriptions` (by room ids), `users` (HR enrichment) | none |
| **search-service** | Valkey | restricted-rooms cache (`searchservice:restrictedrooms:<account>`) | restricted-rooms cache (TTL) |
| **search-sync-worker** | Elasticsearch | — | bulk index/update/delete into monthly message indices + spotlight indices |
| **search-sync-worker** | MongoDB | `teams_user`, `users` (identity resolution for indexed docs) | none |
| **tcard-service** | MongoDB | `cards` (list) | none |
| **teams-chat-member-sync** | MongoDB | `teams_chat` (chats to sync — read replica), `teams_user` (by ids) | `teams_chat` (members synced + seen timestamp) |
| **teams-chat-sync** | MongoDB | `teams_user` (list) | `teams_user` (advance `from` watermark), `teams_chat` (upsert chats) |
| **teams-hr-sync** | MongoDB | `hr_employee` (Teams employees — read replica) | `hr_employee` (upsert), `users` (upsert identities, mark quit) — direct-write cluster |
| **teams-room-creation** | MongoDB | `teams_chat` (chats needing a room — read replica) | `teams_chat` (mark rooms created) |
| **teams-room-inspector** | MongoDB | `rooms`, `subscriptions` (room state + sub counts — read replica) | none |
| **teams-room-verify** | MongoDB | `teams_chat` (chats needing verification — read replica) | `teams_chat` (mark verified) |
| **teams-user-sync** | MongoDB | `teams_user` (existing ids), `hr_employee` (HR rows) — read replica | `teams_user` (upsert) — write cluster |
| **translation-service** | — | none | none |
| **upload-service** | MongoDB | `subscriptions` (membership), `rooms` (site id), `uploads` (download metadata) | none |
| **upload-service** | MinIO | object download | object upload (bucket `chat-<siteID>` by default) |
| **user-presence-service** | Valkey | `presence:{account}:conns` / `:manual` / `:status` / `:azure`, `presence:sweep` (all via Lua) | same keys — ping, activity, disconnect, manual override, external status, sweep |
| **user-presence-service** | MongoDB | `users` (by accounts, for directory enrichment) | none |
| **user-presence-service/sync** | Valkey | `presence:status:index:azure` (in-call index), `presence:idmap:azure` (account → Azure object id) | both keys (add/remove in-call, store id map) |
| **user-service** | MongoDB | `subscriptions` (aggregations, DM, by room, counts), `rooms`, `users` (status, settings, chatlist, priority contacts, HR info), `apps` + `fab_domain_mapping` (app list/categories), `thread_subscriptions` (unread rows), `sso_tokens` (by username) | `users` (status, settings, chatlist, add/remove priority contact), `subscriptions` (app subscribe/mute), `sso_tokens` (upsert) |

## Datastore ownership summary

| Store | Writers | Readers only |
|---|---|---|
| **MongoDB** | admin-service, bot-room-service, botplatform-service, broadcast-worker, hr-sync-worker, inbox-worker, media-service, message-worker, room-service, room-worker, teams-chat-member-sync, teams-chat-sync, teams-hr-sync, teams-room-creation, teams-room-verify, teams-user-sync, user-service | bot-message-handler, bot-message-worker, history-service, message-gatekeeper, notification-worker, portal-service, search-service, search-sync-worker, tcard-service, teams-room-inspector, upload-service, user-presence-service |
| **Cassandra** | bot-message-worker, message-worker, history-service | — |
| **Valkey** | botplatform-service, broadcast-worker, message-gatekeeper, notification-worker, room-worker (invalidate only), search-service, user-presence-service (+ `sync`) | — |
| **MinIO** | client-update-service, media-service, upload-service | — |
| **Elasticsearch** | search-sync-worker | search-service |

## Notes

- **Cassandra is write-fanned, read-narrow.** `message-worker` and `bot-message-worker` only
  write; `history-service` is the only service that both reads and mutates message rows.
  Services needing a single historical message (`message-gatekeeper` quoted-parent snapshot,
  `notification-worker` thread parent) go through history-service over NATS request/reply
  rather than touching Cassandra directly.
- **Read-replica split.** The `teams-*` sync services and `teams-room-inspector` connect via
  `mongoutil.ConnectRead` for their read side and, where they write, open a second client
  against the direct-write cluster (`teams-hr-sync`, `teams-user-sync`,
  `teams-chat-member-sync`, `teams-room-creation`, `teams-room-verify`).
- **Primary-pinned reads.** `broadcast-worker` reads `rooms.encKey` and
  `history-service` reads `room_data_keys` with `readpref.Primary()` so a freshly rotated key
  is never missed on a lagging secondary; `notification-worker` pins its `users` reads for the
  same reason.
- **Room encryption keys** live inline as the `encKey` sub-document of the `rooms` collection
  (`pkg/roomkeystore`), not in a separate collection. Per-room data-encryption keys live in
  `room_data_keys` (`pkg/atrest`).
- **`uploads` is read-only from this stack** — `upload-service` reads download metadata from
  it but never writes the collection.
- **Stateless services** (`auth-service`, `outbox-worker`, `push-notification-service`,
  `translation-service`) hold no datastore connection at all; they operate purely over NATS
  and outbound HTTP.
