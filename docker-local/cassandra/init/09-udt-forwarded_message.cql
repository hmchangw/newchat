CREATE TYPE IF NOT EXISTS chat."ForwardedMessage" (
  message_id              TEXT,
  room_id                 TEXT,
  room_type               TEXT,
  sender                  FROZEN<"Participant">,
  created_at              TIMESTAMP,
  msg                     TEXT,
  mentions                SET<FROZEN<"Participant">>,
  thread_parent_id        TEXT,
  thread_parent_created_at TIMESTAMP,
  thread_room_id          TEXT,
  tshow                   BOOLEAN
);
