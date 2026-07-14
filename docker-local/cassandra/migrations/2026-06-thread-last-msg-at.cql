-- Add thread_last_msg_at (thread-last-message timestamp) to messages_by_id and messages_by_room.
-- Stored on thread parent rows; updated whenever tcount is updated (same UPDATE statement).
-- Existing rows keep thread_last_msg_at=NULL; clients fall back to no thread-freshness sort on null.
--
-- Apply manually against the production keyspace; the docker-local init
-- scripts already create new clusters with this column.

ALTER TABLE chat.messages_by_id   ADD thread_last_msg_at TIMESTAMP;
ALTER TABLE chat.messages_by_room ADD thread_last_msg_at TIMESTAMP;
