CREATE TABLE IF NOT EXISTS chat.messages_by_room (
  room_id                  TEXT,
  bucket                   BIGINT,
  created_at               TIMESTAMP,
  message_id               TEXT,
  thread_room_id           TEXT,
  sender                   FROZEN<"Participant">,
  target_user              FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  file                     FROZEN<"File">,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  tshow                    BOOLEAN, // means from thread [also sent to channel]
  tcount                   INT,     // reply thread count
  thread_parent_id         TEXT,
  thread_parent_created_at TIMESTAMP, // for FE to query thread parent message when also sent to channel (tshow=true)
  quoted_parent_message    FROZEN<"QuotedParentMessage">,
  visible_to               TEXT,
  reactions                MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
  deleted                  BOOLEAN,
  type                     TEXT,
  sys_msg_data             BLOB,
  site_id                  TEXT,
  edited_at                TIMESTAMP,
  updated_at               TIMESTAMP,
  PRIMARY KEY ((room_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
