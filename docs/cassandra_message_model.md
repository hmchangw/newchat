# Cassandra Message Data Model
Description: This schema is for message-related operation in Cassandra, include query, upsert... 
## Schema
### UDT
#### Participant
```cql
CREATE TYPE IF NOT EXISTS "Participant"(
  id TEXT,
  eng_name TEXT,
  company_name TEXT, // need to change internal
  app_id TEXT,
  app_name TEXT,
  is_bot BOOLEAN,
  account TEXT
);
```
#### Card
```cql
CREATE TYPE IF NOT EXISTS "Card"(
  template TEXT,
  data BLOB
);
```
#### CardAction
```cql
CREATE TYPE IF NOT EXISTS "CardAction"(
  verb TEXT,
  text TEXT,
  card_id TEXT,
  display_text TEXT,
  hide_exec_log BOOLEAN,
  card_tmid TEXT,
  data BLOB
);
```
#### File
```cql
CREATE TYPE IF NOT EXISTS "File"(
  id TEXT,
  name TEXT,
  type TEXT
);
```
#### QuotedParentMessage
```cql
CREATE TYPE IF NOT EXISTS "QuotedParentMessage"(
  message_id TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  created_at TIMESTAMP,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  message_link TEXT,
  thread_parent_id TEXT,          // set by message-worker when quoted message is a TShow reply
  thread_parent_created_at TIMESTAMP  // actual CreatedAt of the thread parent; used by history-service
                                      // to enforce access-window checks without a Cassandra round-trip
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
  user_id     TEXT,
  eng_name    TEXT,
  chn_name    TEXT,
  account     TEXT,
  reacted_at  TIMESTAMP
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
  thread_room_id TEXT,
  sender FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  tshow BOOLEAN, // means from thread [also send to channel]
  tcount INT, // message reply thread count
  thread_parent_id TEXT,
  thread_parent_created_at TIMESTAMP, // for FE to query thread parent message when also sent to channel (tshow=true)
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((room_id, bucket),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)
  // compaction_window_size MUST match MESSAGE_BUCKET_HOURS.
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '72'
  };
```
#### thread_messages_by_thread
```cql
CREATE TABLE IF NOT EXISTS thread_messages_by_thread(
  thread_room_id TEXT,
  created_at TIMESTAMP,
  message_id TEXT,
  room_id TEXT,
  thread_parent_id TEXT,
  sender FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((thread_room_id),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```
#### pinned_messages_by_room
```cql
CREATE TABLE IF NOT EXISTS pinned_messages_by_room(
  room_id TEXT,
  pinned_at TIMESTAMP,
  message_id TEXT,
  sender FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  pinned_by FROZEN<"Participant">,
  created_at TIMESTAMP, // message's true creation time
  tshow BOOLEAN,
  thread_parent_id TEXT,
  thread_parent_created_at TIMESTAMP,
  PRIMARY KEY((room_id),pinned_at,message_id)
)WITH CLUSTERING ORDER BY (pinned_at DESC, message_id DESC);
```
#### messages_by_id
```cql
CREATE TABLE IF NOT EXISTS messages_by_id(
  message_id TEXT,
  room_id TEXT,
  thread_room_id TEXT,
  sender FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  tshow BOOLEAN,
  tcount INT, // message reply thread count
  thread_parent_id TEXT,
  thread_parent_created_at TIMESTAMP,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  pinned_at TIMESTAMP,
  pinned_by FROZEN<"Participant">,
  PRIMARY KEY(message_id,created_at)
)WITH CLUSTERING ORDER BY (created_at DESC);
```
