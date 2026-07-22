-- Forwarded-message support: a snapshot UDT + an additive column on
-- messages_by_room so the room-list preview can mark a forward and render
-- "Forwarded a message" for an empty-content forward.
--
-- Additive and non-breaking: existing rows keep forwarded=NULL (a normal,
-- non-forward message). Only messages_by_room carries the column — it is the
-- one table the room-list preview reads.
--
-- Apply manually against the production keyspace; the docker-local init
-- scripts already create new clusters with this UDT + column.

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

ALTER TABLE chat.messages_by_room ADD forwarded FROZEN<"ForwardedMessage">;
