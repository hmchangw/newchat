-- Snapshot of a forwarded source message, embedded on a message row (mirrors
-- "QuotedParentMessage"). Non-null = the message is a forward.
CREATE TYPE IF NOT EXISTS chat."ForwardedMessage" (
  message_id   TEXT,
  room_id      TEXT,
  sender       FROZEN<"Participant">,
  created_at   TIMESTAMP,
  msg          TEXT,
  mentions     SET<FROZEN<"Participant">>,
  attachments  LIST<BLOB>,
  message_link TEXT
);
