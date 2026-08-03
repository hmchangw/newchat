-- Message forwarding: snapshot UDT + column on the three main-timeline tables.
-- Additive and online-safe; old rows read back NULL. thread_messages_by_thread
-- is deliberately excluded (forwards never land in threads).
CREATE TYPE IF NOT EXISTS "ForwardedMessage"(
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT
);
ALTER TABLE messages_by_room        ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE messages_by_id          ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE pinned_messages_by_room ADD forwarded_message FROZEN<"ForwardedMessage">;
