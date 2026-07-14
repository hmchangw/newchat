CREATE TABLE IF NOT EXISTS chat.messages_by_id (
  message_id               TEXT,
  room_id                  TEXT,
  thread_room_id           TEXT,
  sender                   FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  tshow                    BOOLEAN,
  tcount                   INT, // reply thread count
  thread_last_msg_at       TIMESTAMP, // timestamp of most recent thread reply
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

ALTER TABLE chat.messages_by_id ADD enc_payload BLOB;
ALTER TABLE chat.messages_by_id ADD enc_meta FROZEN<"EncMeta">;
