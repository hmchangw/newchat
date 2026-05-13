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
### Table

### Partition Bucketing

`messages_by_room` and `thread_messages_by_room` use a composite partition key
`(room_id, bucket)`. `bucket` is the start-of-window in unix milliseconds derived
deterministically from `created_at` via `pkg/msgbucket.Sizer`. The window size
is configured per service via `MESSAGE_BUCKET_HOURS` (default 24); all services
that read or write these tables MUST be configured with the same window.

#### messages_by_room
```cql
CREATE TABLE IF NOT EXISTS messages_by_room(
  room_id TEXT,
  bucket BIGINT,
  created_at TIMESTAMP,
  message_id TEXT,
  thread_room_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
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
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((room_id, bucket),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```
#### thread_messages_by_room
```cql
CREATE TABLE IF NOT EXISTS thread_messages_by_room(
  room_id TEXT,
  bucket BIGINT,
  thread_room_id TEXT,
  created_at TIMESTAMP,
  message_id TEXT,
  thread_parent_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((room_id, bucket),thread_room_id,created_at,message_id)
)WITH CLUSTERING ORDER BY (thread_room_id DESC,created_at DESC, message_id DESC);
```
#### pinned_messages_by_room
```cql
CREATE TABLE IF NOT EXISTS pinned_messages_by_room(
  room_id TEXT,
  created_at TIMESTAMP, // =pinnedAt
  message_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  pinned_by FROZEN<"Participant">,
  PRIMARY KEY((room_id),created_at,message_id)
)WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```
#### messages_by_id
```cql
CREATE TABLE IF NOT EXISTS messages_by_id(
  message_id TEXT,
  room_id TEXT,
  thread_room_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
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
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
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
