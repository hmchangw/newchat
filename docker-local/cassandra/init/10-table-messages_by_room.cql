CREATE TABLE IF NOT EXISTS chat.messages_by_room (
  room_id                  TEXT,
  bucket                   BIGINT,
  created_at               TIMESTAMP,
  message_id               TEXT,
  thread_room_id           TEXT,
  sender                   FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  tshow                    BOOLEAN, // means from thread [also sent to channel]
  tcount                   INT,     // reply thread count
  thread_last_msg_at       TIMESTAMP, // timestamp of most recent thread reply
  thread_parent_id         TEXT,
  thread_parent_created_at TIMESTAMP, // for FE to query thread parent message when also sent to channel (tshow=true)
  quoted_parent_message    FROZEN<"QuotedParentMessage">,
  visible_to               TEXT,
  reactions                MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
  deleted                  BOOLEAN,
  type                     TEXT,
  sys_msg_data             BLOB,
  site_id                  TEXT,
  edited_at                TIMESTAMP,
  updated_at               TIMESTAMP,
  PRIMARY KEY ((room_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)
  // TimeWindowCompactionStrategy: messages arrive in roughly chronological
  // order and are rarely updated, so SSTables for sealed time windows can be
  // compacted once and then left alone. STCS would otherwise rewrite cold
  // historical data on every tier rebalance, scaling compaction CPU with
  // total table size rather than recent write volume.
  //
  // compaction_window_size MUST match MESSAGE_BUCKET_HOURS so each Cassandra
  // compaction window aligns with exactly one logical bucket; misalignment
  // produces SSTable fragmentation or cross-bucket compaction work.
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '72'
  };

ALTER TABLE chat.messages_by_room ADD enc_payload BLOB;
ALTER TABLE chat.messages_by_room ADD enc_meta FROZEN<"EncMeta">;

-- Pin indicator for the channel timeline (null when not pinned). pinned_by is
-- intentionally not mirrored here; richer pin metadata is a point lookup on
-- messages_by_id.
ALTER TABLE chat.messages_by_room ADD pinned_at TIMESTAMP;
