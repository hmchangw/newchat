-- Drop created_at from the messages_by_id primary key.
--
-- messages_by_id is the canonical point-lookup table: every read targets a
-- single known message_id. Keeping created_at in the PK added no query
-- benefit but required every UPDATE/DELETE to bind the timestamp — a source
-- of "row not found" failures when the bound value drifted even 1 ms from
-- the stored value due to client-side clock skew or re-created rows.
--
-- A Cassandra PRIMARY KEY change cannot be performed with ALTER TABLE. The
-- procedure is:
--
--   1. DROP TABLE chat.messages_by_id;
--   2. CREATE TABLE chat.messages_by_id ... PRIMARY KEY (message_id);
--      (see CREATE statement below — same columns, no CLUSTERING ORDER BY)
--   3. Backfill by walking chat.messages_by_room (the durable source of
--      truth for all messages) and re-inserting each row into
--      messages_by_id. The INSERT is idempotent on message_id.
--
-- Fresh / docker-local clusters get the new schema from the init scripts
-- (docker-local/cassandra/init/13-table-messages_by_id.cql); the backfill
-- is a no-op when the table is empty.
--
-- On a live cluster, co-ordinate with in-flight writes:
--   * message-worker and history-service bind only message_id in their WHERE
--     clauses after this deploy, so no stale created_at arg is sent.
--   * INSERTs carry created_at as a regular column — already present in the
--     new schema; wire format is unchanged.

-- Step 1: drop the old table (destructive — run ONLY after a full backup).
-- DROP TABLE chat.messages_by_id;

-- Step 2: recreate with message_id as sole primary key.
CREATE TABLE IF NOT EXISTS chat.messages_by_id (
  message_id               TEXT,
  room_id                  TEXT,
  thread_room_id           TEXT,
  sender                   FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  file                     FROZEN<"File">,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  tshow                    BOOLEAN,
  tcount                   INT,
  thread_parent_id         TEXT,
  thread_parent_created_at TIMESTAMP,
  quoted_parent_message    FROZEN<"QuotedParentMessage">,
  visible_to               TEXT,
  reactions                MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted                  BOOLEAN,
  type                     TEXT,
  sys_msg_data             BLOB,
  site_id                  TEXT,
  edited_at                TIMESTAMP,
  created_at               TIMESTAMP,
  updated_at               TIMESTAMP,
  pinned_at                TIMESTAMP,
  pinned_by                FROZEN<"Participant">,
  PRIMARY KEY (message_id)
);

-- Step 3: add the at-rest-encryption columns as post-create ALTERs, mirroring
-- the init script (docker-local/cassandra/init/13-table-messages_by_id.cql) so
-- the recreated table matches a fresh one. enc_payload/enc_meta are
-- intentionally NOT in the CREATE column list above (an ADD of an existing
-- column would error).
ALTER TABLE chat.messages_by_id ADD enc_payload BLOB;
ALTER TABLE chat.messages_by_id ADD enc_meta FROZEN<"EncMeta">;
