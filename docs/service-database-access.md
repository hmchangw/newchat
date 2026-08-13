# Service Database Access

Per-service inventory of datastore reads and writes. Services that touch more than one
datastore span multiple rows, one per store.
Derived from each service's store interface and its concrete implementation.

**Datastores:** MongoDB (operational), Cassandra (message history), Valkey (cluster-mode
cache / presence state), MinIO (object storage), Elasticsearch (search indices).

NATS/JetStream is a message bus, not a datastore, and is out of scope here. Process-local
LRU caches (`pkg/userstore`, `pkg/roommetacache` L1, `history-service/internal/readcache`,
`broadcast-worker/keycache`) are noted only where they front a listed store.

## Services

<table>
<thead>
<tr><th>Service</th><th>Store</th><th>Reads</th><th>Writes</th></tr>
</thead>
<tbody>

<tr>
<td><b>admin-service</b></td>
<td>MongoDB</td>
<td><code>users</code> (search by site/account, auth lookup), <code>admin_audit</code> (list w/ filter)</td>
<td><code>users</code> (create, update fields, password + force-change, <code>active=false</code>), <code>admin_audit</code> (append), <code>sessions</code> (<code>DeleteMany</code> on password change / deactivate, in a transaction)</td>
</tr>

<tr>
<td><b>auth-service</b></td>
<td>—</td>
<td>none</td>
<td>none</td>
</tr>

<tr>
<td><b>bot-message-handler</b></td>
<td>MongoDB</td>
<td><code>subscriptions</code> (by room+user), <code>rooms</code> (by id), <code>users</code> (by id), member-id list</td>
<td>none</td>
</tr>

<tr>
<td rowspan="2"><b>bot-message-worker</b></td>
<td>Cassandra</td>
<td>—</td>
<td><code>messages_by_room</code>, <code>messages_by_id</code>, <code>thread_messages_by_thread</code> (INSERT); <code>messages_by_id</code> + <code>messages_by_room</code> (UPDATE thread counters)</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>room_data_keys</code> (DEK, only when at-rest encryption is enabled)</td>
<td>none</td>
</tr>

<tr>
<td><b>bot-room-service</b></td>
<td>MongoDB</td>
<td><code>rooms</code> (by id, <code>encKey</code> projection), <code>users</code> (by id), <code>subscriptions</code> (member accounts)</td>
<td><code>rooms</code> (insert; <code>encKey</code> set / set-with-version / rotate), <code>subscriptions</code> (upsert, delete)</td>
</tr>

<tr>
<td rowspan="2"><b>botplatform-service</b></td>
<td>MongoDB</td>
<td><code>users</code> (by account), <code>sessions</code> (by token hash), <code>subscriptions</code> (bot sub in room, bot DM sub)</td>
<td><code>sessions</code> (insert; delete beyond per-account cap)</td>
</tr>
<tr>
<td>Valkey</td>
<td>idempotency keys (request-replay middleware)</td>
<td>idempotency keys</td>
</tr>

<tr>
<td rowspan="2"><b>broadcast-worker</b></td>
<td>MongoDB</td>
<td><code>rooms</code> (meta), <code>subscriptions</code> (room member list, <code>historySharedSince</code>), <code>thread_rooms</code> (thread followers), <code>users</code> (LRU-cached), <code>rooms.encKey</code> (pinned to primary)</td>
<td><code>rooms</code> (last message id/time, <code>mentionAll</code>), <code>subscriptions</code> (set mentions, advance <code>lastSeenAt</code>)</td>
</tr>
<tr>
<td>Valkey</td>
<td>room-meta L2 cache</td>
<td>room-meta L2 cache (write-through, TTL)</td>
</tr>

<tr>
<td><b>client-update-service</b></td>
<td>MinIO</td>
<td>version blob <code>Open</code></td>
<td>version blob <code>Put</code>; bucket create at startup</td>
</tr>

<tr>
<td rowspan="2"><b>history-service</b></td>
<td>Cassandra</td>
<td><code>messages_by_room</code>, <code>messages_by_id</code>, <code>thread_messages_by_thread</code>, <code>pinned_messages_by_room</code> (paged reads, by-id, thread slices, pins)</td>
<td><code>messages_by_id</code> / <code>messages_by_room</code> / <code>thread_messages_by_thread</code> / <code>pinned_messages_by_room</code> (edit content, soft delete, pin/unpin, add/remove reaction); <code>pinned_messages_by_room</code> INSERT/DELETE</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>rooms</code> (times, min last-seen, user count), <code>subscriptions</code> (access check, <code>historySharedSince</code>), <code>thread_rooms</code>, <code>thread_subscriptions</code>, <code>apps</code> (bot display name), <code>users</code>, <code>room_data_keys</code> (DEK, primary read pref)</td>
<td>none</td>
</tr>

<tr>
<td><b>hr-sync-worker</b></td>
<td>MongoDB</td>
<td><code>hr_employee</code>, <code>users</code> (change detection for upserts)</td>
<td><code>hr_employee</code> (upsert batch), <code>users</code> (upsert identities, mark Teams employees quit)</td>
</tr>

<tr>
<td><b>inbox-worker</b></td>
<td>MongoDB</td>
<td><code>users</code> (by accounts)</td>
<td><code>subscriptions</code> (create / bulk create / delete by accounts; roles, read, mute, favorite, open — each under an ordering guard), <code>rooms</code> (upsert guarded by <code>updatedAt</code>), <code>thread_subscriptions</code> (upsert, delete, thread-read, bulk read-all)</td>
</tr>

<tr>
<td rowspan="2"><b>media-service</b></td>
<td>MongoDB</td>
<td><code>users</code> (by account, employee id, bot site), <code>subscriptions</code> (room site/type/name, membership check), <code>avatars</code> (by subject), <code>custom_emojis</code> (by shortcode, list per site)</td>
<td><code>avatars</code> (set bot avatar), <code>custom_emojis</code> (upsert, delete)</td>
</tr>
<tr>
<td>MinIO</td>
<td>avatar / custom-emoji objects (bucket <code>avatars</code> by default)</td>
<td>avatar / custom-emoji objects</td>
</tr>

<tr>
<td rowspan="2"><b>message-gatekeeper</b></td>
<td>MongoDB</td>
<td><code>subscriptions</code> (by account+room), <code>rooms</code> (meta), <code>users</code> (LRU-cached)</td>
<td>none</td>
</tr>
<tr>
<td>Valkey</td>
<td>room-meta L2 cache</td>
<td>room-meta L2 cache (write-through, TTL)</td>
</tr>

<tr>
<td rowspan="2"><b>message-worker</b></td>
<td>Cassandra</td>
<td><code>messages_by_id</code> (sender, quoted-parent snapshot, <code>createdAt</code>)</td>
<td><code>messages_by_room</code>, <code>messages_by_id</code>, <code>thread_messages_by_thread</code> (INSERT); <code>messages_by_id</code> + <code>messages_by_room</code> (UPDATE thread counters, parent <code>threadRoomId</code>)</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>users</code>, <code>teams_user</code> (HR identity), <code>subscriptions</code> (<code>historySharedSince</code>), <code>thread_rooms</code> (by parent message), <code>room_data_keys</code> (DEK)</td>
<td><code>thread_rooms</code> (create, last message, reply accounts), <code>thread_subscriptions</code> (insert, upsert, mark mention, advance last-seen), <code>subscriptions</code> (advance last-seen)</td>
</tr>

<tr>
<td rowspan="2"><b>notification-worker</b></td>
<td>MongoDB</td>
<td><code>subscriptions</code> (member fan-out set), <code>thread_rooms</code> (followers), <code>rooms</code> (meta), <code>users</code> (notification settings, primary read pref)</td>
<td>none</td>
</tr>
<tr>
<td>Valkey</td>
<td>room member-list cache, room-meta L2 cache</td>
<td>room member-list cache (set / invalidate), room-meta L2 cache</td>
</tr>

<tr>
<td><b>outbox-worker</b></td>
<td>—</td>
<td>none (drains the OUTBOX JetStream stream only)</td>
<td>none</td>
</tr>

<tr>
<td><b>portal-service</b></td>
<td>MongoDB</td>
<td><code>users</code>, <code>hr_employee</code> (directory list, by account)</td>
<td>none</td>
</tr>

<tr>
<td><b>push-notification-service</b></td>
<td>—</td>
<td>none</td>
<td>none</td>
</tr>

<tr>
<td><b>room-service</b></td>
<td>MongoDB</td>
<td><code>rooms</code>, <code>subscriptions</code>, <code>thread_subscriptions</code>, <code>thread_rooms</code>, <code>users</code>, <code>apps</code>, <code>bot_cmd_menu</code>, <code>room_members</code>, <code>teams_meetings</code>, <code>room_data_keys</code> — membership checks, member/mention listings, counts, read receipts, section ordering, DM lookup, thread info</td>
<td><code>rooms</code> (visibility, min user last-seen), <code>subscriptions</code> (read state, mute, favorite, section move/rebalance, open, owner role, restriction apply, thread-unread clear), <code>thread_subscriptions</code> (read, clear for account), <code>thread_rooms</code> (min user last-seen), <code>teams_meetings</code> (insert)</td>
</tr>

<tr>
<td rowspan="2"><b>room-worker</b></td>
<td>MongoDB</td>
<td><code>rooms</code> (meta, key <code>encKey</code>), <code>subscriptions</code> (by account+room, DM pair, accounts, by room), <code>users</code>, <code>apps</code>, <code>room_members</code> (existence, org members), <code>thread_rooms</code> / <code>thread_subscriptions</code> (followers)</td>
<td><code>rooms</code> (create, rename, member-count delta/reconcile, cross-site flag, <code>encKey</code> set/rotate), <code>subscriptions</code> (bulk create, delete, delete by accounts, remove role, rename), <code>room_members</code> (bulk create, delete), <code>thread_rooms</code> (pull followers), <code>thread_subscriptions</code> (delete), <code>room_data_keys</code> (ensure DEK)</td>
</tr>
<tr>
<td>Valkey</td>
<td>—</td>
<td>room-meta L2 cache bust (delete on room change)</td>
</tr>

<tr>
<td rowspan="3"><b>search-service</b></td>
<td>Elasticsearch</td>
<td>message / spotlight / spotlight-org / user-room indices (query + user-room doc)</td>
<td>none</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>apps</code> (name search, by assistant), <code>subscriptions</code> (by room ids), <code>users</code> (HR enrichment)</td>
<td>none</td>
</tr>
<tr>
<td>Valkey</td>
<td>restricted-rooms cache (<code>searchservice:restrictedrooms:&lt;account&gt;</code>)</td>
<td>restricted-rooms cache (TTL)</td>
</tr>

<tr>
<td rowspan="2"><b>search-sync-worker</b></td>
<td>Elasticsearch</td>
<td>—</td>
<td>bulk index/update/delete into monthly message indices + spotlight indices</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>teams_user</code>, <code>users</code> (identity resolution for indexed docs)</td>
<td>none</td>
</tr>

<tr>
<td><b>tcard-service</b></td>
<td>MongoDB</td>
<td><code>cards</code> (list)</td>
<td>none</td>
</tr>

<tr>
<td><b>teams-chat-member-sync</b></td>
<td>MongoDB</td>
<td><code>teams_chat</code> (chats to sync — read replica), <code>teams_user</code> (by ids)</td>
<td><code>teams_chat</code> (members synced + seen timestamp)</td>
</tr>

<tr>
<td><b>teams-chat-sync</b></td>
<td>MongoDB</td>
<td><code>teams_user</code> (list)</td>
<td><code>teams_user</code> (advance <code>from</code> watermark), <code>teams_chat</code> (upsert chats)</td>
</tr>

<tr>
<td><b>teams-hr-sync</b></td>
<td>MongoDB</td>
<td><code>hr_employee</code> (Teams employees — read replica)</td>
<td><code>hr_employee</code> (upsert), <code>users</code> (upsert identities, mark quit) — direct-write cluster</td>
</tr>

<tr>
<td><b>teams-room-creation</b></td>
<td>MongoDB</td>
<td><code>teams_chat</code> (chats needing a room — read replica)</td>
<td><code>teams_chat</code> (mark rooms created)</td>
</tr>

<tr>
<td><b>teams-room-inspector</b></td>
<td>MongoDB</td>
<td><code>rooms</code>, <code>subscriptions</code> (room state + sub counts — read replica)</td>
<td>none</td>
</tr>

<tr>
<td><b>teams-room-verify</b></td>
<td>MongoDB</td>
<td><code>teams_chat</code> (chats needing verification — read replica)</td>
<td><code>teams_chat</code> (mark verified)</td>
</tr>

<tr>
<td><b>teams-user-sync</b></td>
<td>MongoDB</td>
<td><code>teams_user</code> (existing ids), <code>hr_employee</code> (HR rows) — read replica</td>
<td><code>teams_user</code> (upsert) — write cluster</td>
</tr>

<tr>
<td><b>translation-service</b></td>
<td>—</td>
<td>none</td>
<td>none</td>
</tr>

<tr>
<td rowspan="2"><b>upload-service</b></td>
<td>MongoDB</td>
<td><code>subscriptions</code> (membership), <code>rooms</code> (site id), <code>uploads</code> (download metadata)</td>
<td>none</td>
</tr>
<tr>
<td>MinIO</td>
<td>object download</td>
<td>object upload (bucket <code>chat-&lt;siteID&gt;</code> by default)</td>
</tr>

<tr>
<td rowspan="2"><b>user-presence-service</b></td>
<td>Valkey</td>
<td><code>presence:{account}:conns</code>, <code>:manual</code>, <code>:status</code>, <code>:azure</code>, <code>presence:sweep</code> (all via Lua)</td>
<td>same keys — ping, activity, disconnect, manual override, external status, sweep</td>
</tr>
<tr>
<td>MongoDB</td>
<td><code>users</code> (by accounts, for directory enrichment)</td>
<td>none</td>
</tr>

<tr>
<td><b>user-presence-service/sync</b></td>
<td>Valkey</td>
<td><code>presence:status:index:azure</code> (in-call index), <code>presence:idmap:azure</code> (account → Azure object id)</td>
<td>both keys (add/remove in-call, store id map)</td>
</tr>

<tr>
<td><b>user-service</b></td>
<td>MongoDB</td>
<td><code>subscriptions</code> (aggregations, DM, by room, counts), <code>rooms</code>, <code>users</code> (status, settings, chatlist, priority contacts, HR info), <code>apps</code> + <code>fab_domain_mapping</code> (app list/categories), <code>thread_subscriptions</code> (unread rows), <code>sso_tokens</code> (by username)</td>
<td><code>users</code> (status, settings, chatlist, add/remove priority contact), <code>subscriptions</code> (app subscribe/mute), <code>sso_tokens</code> (upsert)</td>
</tr>

</tbody>
</table>

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
