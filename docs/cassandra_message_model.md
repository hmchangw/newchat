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
  bot_username TEXT,    // target bot for the tcard_execute callback (client-supplied on the cardAction payload); optional
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
service via `MESSAGE_BUCKET_HOURS` (envDefault 360 in both `message-worker` and
`history-service`); all services that read or write this table MUST be
configured with the same window.

`thread_messages_by_thread` is partitioned by `thread_room_id` alone — one
partition per thread. Reads slice the partition by `created_at`; no bucket
walk is needed. This shape keeps the worst-case fetch latency bounded by
partition size rather than by the thread's lifespan.

### Thread reply count

`tcount` on the parent row (mirrored in `messages_by_room` and `messages_by_id`)
is maintained by `pkg/threadcount` on every reply add (`message-worker`,
`bot-message-worker`) and reply delete (`history-service`). Each writer first
point-reads the stamped count, because that is what decides how it is
maintained:

- **Stamped count under `threadcount.DefaultScanLimit`** — the count is
  recomputed from a bounded scan of the thread partition and blind-SET. The
  reply rows are the source of truth, so the result is exact, idempotent under
  JetStream redelivery, and self-healing: whatever was stamped before is
  replaced by what is actually there. The scan reads one row past its cap so it
  knows whether it reached the end of the partition; soft-deleted replies still
  occupy rows, so a read that filled its cap may have seen nothing but
  tombstones while live replies survive just past it. Such a read may only
  **raise** the count and **advance** `thread_last_msg_at` — never stamp an
  exact value, never clear the timestamp.
- **At or past the limit** — the scan is skipped entirely and the stamped count
  is moved by one with a plain write. Per-reply cost stops depending on thread
  length, which is what keeps a long thread out of the timeout → NAK →
  redelivery storm a full partition walk caused. `messages_by_id` is the
  authority; the `messages_by_room` mirror is written from the value committed
  there, and only ever carries a `thread_last_msg_at` that was actually
  resolved (an unresolved one leaves the mirror's column alone rather than
  clearing it).

**The count above the limit is approximate.** Adjusting by ±1 without
coordinating means concurrent replies can lose an increment and a redelivery
can add one twice, and neither leaves an error to react to. This is deliberate:
a lightweight transaction would make every reply to the busiest threads
serialize through Paxos on one row, to buy an exactness that no reader needs.

**Re-anchoring bounds the error.** `threadcount.ShouldReanchor` re-derives the
count from an exact scan with probability `budget/stamped`, so the expected
rows scanned per reply stays flat at `DefaultReanchorBudget` however long the
thread grows — a fixed "every Nth reply" rule would instead cost O(thread
length) per reply amortized, reintroducing the quadratic behaviour. Sampling is
stateless: no coordination between workers, no column recording when a thread
was last anchored. Re-anchoring is best-effort — on failure the stamped count
stands, which is never worse than not having tried.

`DefaultReconcileRowLimit` caps what one re-anchor may read. The sample prices a
re-anchor at the **live** count, but the partition holds every reply ever
written, soft deletes included, so a thread with a thousand survivors and a
million tombstones would be sampled as if a scan cost a thousand rows and
charged a million. Past the cap the re-anchor errors instead of stamping a count
it cannot verify, and the parent keeps its approximate value — which is the
state this design already tolerates.

A JetStream redelivery above the limit does not repeat the adjustment: the reply
may already be counted, and counting it twice is worse than the at-most-one
undercount the next re-anchor erases. It does re-stamp both rows from the
authority, because the two are written in one **unlogged** batch across two
partitions and the delivery that failed can have landed on only one of them;
re-stamping is idempotent and is the one chance to repair that divergence before
the retry is acked. It also advances `thread_last_msg_at` with the reply's own
time (idempotent, unlike the count), and deliberately skips re-anchor sampling —
a retry burst is the worst moment to add partition scans.

Consequence for readers: `tcount` is exact for threads under the scan limit —
every thread in practice — and beyond it is an approximation that stays close
and is periodically corrected. Nothing server-side depends on the precise
value: the only consumers test it against zero (`history-service` skips
Cassandra for a thread with no surviving replies, and treats a positive count
as "this message is a thread parent"). The exact figure is used for display.

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
  reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  sender FROZEN<"Participant">,
  site_id TEXT,
  sys_msg_data BLOB,
  tcount INT, // non-deleted thread reply count (pkg/threadcount; see "Thread reply count")
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
    'compaction_window_size': '360'
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
  tcount INT, // non-deleted thread reply count (pkg/threadcount; see "Thread reply count")
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
