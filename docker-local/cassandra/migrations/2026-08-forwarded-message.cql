-- Message forwarding: snapshot UDT + column on the three main-timeline tables.
-- Additive and online-safe; old rows read back NULL. thread_messages_by_thread
-- is deliberately excluded (forwards never land in threads).
--
-- Apply manually against the production keyspace; the docker-local init
-- scripts already create new clusters with this column.
--
-- If a pre-release cluster already created an earlier ForwardedMessage without
-- room_type/thread_room_id/tshow, CREATE TYPE IF NOT EXISTS is a no-op there —
-- add the three columns explicitly instead:
--   ALTER TYPE chat."ForwardedMessage" ADD room_type TEXT;
--   ALTER TYPE chat."ForwardedMessage" ADD thread_room_id TEXT;
--   ALTER TYPE chat."ForwardedMessage" ADD tshow BOOLEAN;

CREATE TYPE IF NOT EXISTS chat."ForwardedMessage"(
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  msg TEXT,
  room_id TEXT,
  room_type TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT,
  thread_room_id TEXT,
  tshow BOOLEAN
);
ALTER TABLE chat.messages_by_room        ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE chat.messages_by_id          ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE chat.pinned_messages_by_room ADD forwarded_message FROZEN<"ForwardedMessage">;
