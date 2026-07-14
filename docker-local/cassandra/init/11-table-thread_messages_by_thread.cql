CREATE TABLE IF NOT EXISTS chat.thread_messages_by_thread (
  thread_room_id        TEXT,
  created_at            TIMESTAMP,
  message_id            TEXT,
  room_id               TEXT,
  thread_parent_id      TEXT,
  sender                FROZEN<"Participant">,
  msg                   TEXT,
  mentions              SET<FROZEN<"Participant">>,
  attachments           LIST<BLOB>,
  card                  FROZEN<"Card">,
  card_action           FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to            TEXT,
  reactions             MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted               BOOLEAN,
  type                  TEXT,
  sys_msg_data          BLOB,
  site_id               TEXT,
  edited_at             TIMESTAMP,
  updated_at            TIMESTAMP,
  PRIMARY KEY ((thread_room_id), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);

ALTER TABLE chat.thread_messages_by_thread ADD enc_payload BLOB;
ALTER TABLE chat.thread_messages_by_thread ADD enc_meta FROZEN<"EncMeta">;
ALTER TABLE chat.thread_messages_by_thread ADD tshow BOOLEAN;
