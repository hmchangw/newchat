# Cassandra Message Data Model
Description: This schema is for message-related operation in Cassandra, include query, upsert... 
## Schema
### UDT
#### Card
```cql
CREATE TYPE IF NOT EXISTS "Card"(
  data BLOB,
  template TEXT
);
```
#### CardAction
```cql
CREATE TYPE IF NOT EXISTS "CardAction"(
  card_id TEXT,
  card_tmid TEXT,
  data BLOB,
  display_text TEXT,
  hide_exec_log BOOLEAN,
  text TEXT,
  verb TEXT
);
```
#### EncMeta
```cql
CREATE TYPE IF NOT EXISTS "EncMeta"(
  nonce BLOB  // 12 bytes, AES-256-GCM nonce for enc_payload
);
```
Per-row metadata for at-rest encryption. The KEK version that wrapped the
room's DEK is intentionally **not** stored here — it lives on the
`room_data_keys` MongoDB document and is authoritative there. See
`docs/superpowers/specs/2026-05-05-message-at-rest-encryption-design.md`.
#### Participant
```cql
CREATE TYPE IF NOT EXISTS "Participant"(
  account TEXT,
  app_id TEXT,
  app_name TEXT,
  company_name TEXT, // need to change internal
  eng_name TEXT,
  id TEXT,
  is_bot BOOLEAN
);
```
#### QuotedParentMessage
```cql
CREATE TYPE IF NOT EXISTS "QuotedParentMessage"(
  attachments LIST<BLOB>,
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,  // actual CreatedAt of the thread parent; used by history-service
                                       // to enforce access-window checks without a Cassandra round-trip.
                                       // Resolved server-side by message-gatekeeper from the parent
                                       // message (NOT client-supplied) — see #322.
  thread_parent_id TEXT                // set by message-worker when quoted message is a TShow reply
);
```
#### ForwardedMessage
```cql
CREATE TYPE IF NOT EXISTS "ForwardedMessage"(  // snapshot of a forwarded source message; mirrors
                                               // QuotedParentMessage minus the thread-context fields.
                                               // Non-null on a row marks that row as a forward.
  attachments LIST<BLOB>,
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">
);
```
#### reaction_key
```cql
CREATE TYPE IF NOT EXISTS chat.reaction_key (
  emoji        TEXT,
  user_account TEXT
);
```
#### reactor_info
```cql
CREATE TYPE IF NOT EXISTS chat.reactor_info (
  account     TEXT,
  chn_name    TEXT,
  eng_name    TEXT,
  reacted_at  TIMESTAMP,
  user_id     TEXT
);
```
### Table

### Partition Bucketing

`messages_by_room` uses a composite partition key `(room_id, bucket)`. `bucket`
is the start-of-window in unix milliseconds derived deterministically from
`created_at` via `pkg/msgbucket.Sizer`. The window size is configured per
service via `MESSAGE_BUCKET_HOURS` (envDefault 72 in both `message-worker` and
`history-service`); all services that read or write this table MUST be
configured with the same window.

`thread_messages_by_thread` is partitioned by `thread_room_id` alone — one
partition per thread. Reads slice the partition by `created_at`; no bucket
walk is needed. This shape keeps the worst-case fetch latency bounded by
partition size rather than by the thread's lifespan.

### Compaction

`messages_by_room` uses `TimeWindowCompactionStrategy` with
`compaction_window_size` matching `MESSAGE_BUCKET_HOURS`, so each Cassandra
compaction window corresponds to exactly one logical bucket: a sealed bucket's
SSTables are compacted once and then left alone, keeping compaction cost
proportional to recent write volume rather than total table size.

`thread_messages_by_thread` keeps the default compaction strategy — it is
partitioned per thread (not time-bucketed), so the window-alignment rationale
does not apply.

Operational notes:
- Federation replays (`inbox-worker`) that lag more than one window write
  late-arriving rows into the current window's SSTable; tolerable in small
  volume but worth monitoring if sustained federation lag is expected.
- Prefer sub-range / incremental `nodetool repair`; a full-cluster repair
  rewrites old SSTables into the current TWCS window and defeats the point.
- Local dev: the `docker-local/cassandra/init/*.cql` scripts already create
  fresh keyspaces with TWCS. Production clusters apply the migration in
  `docker-local/cassandra/migrations/2026-05-twcs-message-tables.cql`.

#### messages_by_room
```cql
CREATE TABLE IF NOT EXISTS messages_by_room(
  room_id TEXT,
  bucket BIGINT,
  created_at TIMESTAMP,
  message_id TEXT,
  attachments LIST<BLOB>,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  deleted BOOLEAN,
  edited_at TIMESTAMP,
  enc_meta FROZEN<"EncMeta">,       // 12-byte AES-GCM nonce; null for legacy plaintext rows
  enc_payload BLOB,                 // bundled JSON ciphertext of user-authored content; non-null for rows
                                    //   written after the at-rest encryption rollout
  mentions SET<FROZEN<"Participant">>,
  msg TEXT,
  pinned_at TIMESTAMP,              // pin indicator for the channel timeline; null when not pinned.
                                    //   pinned_by is intentionally NOT mirrored here — the timeline
                                    //   indicator only needs pinned_at; richer pin metadata is a
                                    //   point lookup on messages_by_id.
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  forwarded FROZEN<"ForwardedMessage">,  // forward marker + source snapshot; only on messages_by_room
                                         //   (the room-list preview reads it). Null for non-forwards.
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  sender FROZEN<"Participant">,
  site_id TEXT,
  sys_msg_data BLOB,
  tcount INT, // bounded non-deleted thread reply count, capped at 99 (pkg/threadcount.Cap); FE renders >= 99 as "99+"
  thread_last_msg_at TIMESTAMP, // timestamp of most recent thread reply; null until first reply
  thread_parent_created_at TIMESTAMP, // for FE to query thread parent message when also sent to channel (tshow=true)
  thread_parent_id TEXT,
  thread_room_id TEXT,
  tshow BOOLEAN, // means from thread [also send to channel]
  type TEXT,
  updated_at TIMESTAMP,
  visible_to TEXT,
  PRIMARY KEY((room_id, bucket),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)
  // compaction_window_size MUST match MESSAGE_BUCKET_HOURS.
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '72'
  };
```

Note: `messages_by_room` rows originate from channel messages AND from
`tshow=true` ("also send to channel") thread replies — message-worker
dual-writes such replies here (keyed by the reply's own `created_at`/bucket,
with `tshow`, `thread_parent_id`, `thread_parent_created_at` populated) in
addition to the usual `thread_messages_by_thread` + `messages_by_id` writes.
Edits and soft-deletes of a tshow reply propagate to this copy as well.

#### thread_messages_by_thread
```cql
CREATE TABLE IF NOT EXISTS thread_messages_by_thread(
  thread_room_id TEXT,
  created_at TIMESTAMP,
  message_id TEXT,
  attachments LIST<BLOB>,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  deleted BOOLEAN,
  edited_at TIMESTAMP,
  enc_meta FROZEN<"EncMeta">,       // 12-byte AES-GCM nonce; null for legacy plaintext rows
  enc_payload BLOB,                 // bundled JSON ciphertext of user-authored content; non-null for rows
                                    //   written after the at-rest encryption rollout
  mentions SET<FROZEN<"Participant">>,
  msg TEXT,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  room_id TEXT,
  sender FROZEN<"Participant">,
  site_id TEXT,
  sys_msg_data BLOB,
  thread_parent_id TEXT,
  tshow BOOLEAN,                    // "also send to channel" flag; set when the reply was dual-written into
                                    //   messages_by_room as well. Null/false for legacy rows (backfill out of scope).
  type TEXT,
  updated_at TIMESTAMP,
  visible_to TEXT,
  PRIMARY KEY((thread_room_id),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```
#### pinned_messages_by_room
```cql
CREATE TABLE IF NOT EXISTS pinned_messages_by_room(
  room_id TEXT,
  pinned_at TIMESTAMP,
  message_id TEXT,
  attachments LIST<BLOB>,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  created_at TIMESTAMP, // message's true creation time
  deleted BOOLEAN,
  edited_at TIMESTAMP,
  enc_meta FROZEN<"EncMeta">,       // 12-byte AES-GCM nonce; null for legacy plaintext rows
  enc_payload BLOB,                 // bundled JSON ciphertext of user-authored content; non-null for rows
                                    //   written after the at-rest encryption rollout
  mentions SET<FROZEN<"Participant">>,
  msg TEXT,
  pinned_by FROZEN<"Participant">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  -- No reactions column: pinned panel does not render reactions, so this
  -- table is not a reactions mirror (unlike messages_by_id / by_room /
  -- thread_messages_by_thread). Reads needing reactions side-fetch from
  -- messages_by_id.
  sender FROZEN<"Participant">,
  site_id TEXT,
  sys_msg_data BLOB,
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT,
  tshow BOOLEAN,
  type TEXT,
  updated_at TIMESTAMP,
  visible_to TEXT,
  PRIMARY KEY((room_id),pinned_at,message_id)
)WITH CLUSTERING ORDER BY (pinned_at DESC, message_id DESC);
```
#### messages_by_id
```cql
CREATE TABLE IF NOT EXISTS messages_by_id(
  message_id TEXT,
  attachments LIST<BLOB>,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  created_at TIMESTAMP,
  deleted BOOLEAN,
  edited_at TIMESTAMP,
  enc_meta FROZEN<"EncMeta">,       // 12-byte AES-GCM nonce; null for legacy plaintext rows
  enc_payload BLOB,                 // bundled JSON ciphertext of user-authored content; non-null for rows
                                    //   written after the at-rest encryption rollout
  mentions SET<FROZEN<"Participant">>,
  msg TEXT,
  pinned_at TIMESTAMP,
  pinned_by FROZEN<"Participant">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  room_id TEXT,
  sender FROZEN<"Participant">,
  site_id TEXT,
  sys_msg_data BLOB,
  tcount INT, // bounded non-deleted thread reply count, capped at 99 (pkg/threadcount.Cap); FE renders >= 99 as "99+"
  thread_last_msg_at TIMESTAMP, // timestamp of most recent thread reply; null until first reply
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT,
  thread_room_id TEXT,
  tshow BOOLEAN,
  type TEXT,
  updated_at TIMESTAMP,
  visible_to TEXT,
  PRIMARY KEY(message_id)  -- message_id is unique per message; sole partition key
);
```

## Encryption (at rest)

Rows written after the at-rest encryption rollout encrypt user-authored
content into a single `enc_payload` blob and leave the encrypted legacy
plaintext columns (`msg`, `attachments`, `card`, `card_action`, and the
body fields of `quoted_parent_message`) null. `sys_msg_data` is **not** encrypted —
it carries system-generated metadata (e.g. the room members being added), not
user-authored secrets, so it stays in its plaintext column. Rows written before
the rollout retain their plaintext columns and have `enc_payload IS NULL`. The
read path branches on `enc_payload IS NOT NULL`. See the design spec for
details: `docs/superpowers/specs/2026-05-05-message-at-rest-encryption-design.md`.
