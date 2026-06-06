-- NOTE: the clustering key changed during development (created_at → pinned_at)
-- and the column set changed (added: created_at, tshow, thread_parent_id,
-- thread_parent_created_at). Devs whose persistent cassandra-data volume was
-- populated from an earlier branch state must drop the table or wipe the
-- volume before re-running: `docker compose -f docker-local/compose.deps.yaml
-- down -v` OR `cqlsh -e 'DROP TABLE chat.pinned_messages_by_room'`.
CREATE TABLE IF NOT EXISTS chat.pinned_messages_by_room (
  room_id               TEXT,
  pinned_at             TIMESTAMP,
  message_id            TEXT,
  sender                FROZEN<"Participant">,
  msg                   TEXT,
  mentions              SET<FROZEN<"Participant">>,
  attachments           LIST<BLOB>,
  file                  FROZEN<"File">,
  card                  FROZEN<"Card">,
  card_action           FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to            TEXT,
  -- No reactions column: the pinned panel does not render reactions, so the
  -- v3 reactions feature does not mirror them here. Reads of the pinned
  -- panel that need reactions must side-fetch from messages_by_id.
  deleted               BOOLEAN,
  type                  TEXT,
  sys_msg_data          BLOB,
  site_id               TEXT,
  edited_at             TIMESTAMP,
  updated_at            TIMESTAMP,
  pinned_by             FROZEN<"Participant">,
  created_at            TIMESTAMP, // message's true creation time
  tshow                 BOOLEAN,
  thread_parent_id      TEXT,
  thread_parent_created_at TIMESTAMP,
  PRIMARY KEY ((room_id), pinned_at, message_id)
) WITH CLUSTERING ORDER BY (pinned_at DESC, message_id DESC);

ALTER TABLE chat.pinned_messages_by_room ADD enc_payload BLOB;
ALTER TABLE chat.pinned_messages_by_room ADD enc_meta FROZEN<"EncMeta">;
