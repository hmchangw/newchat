-- Add tshow (also-send-to-channel flag) column to thread_messages_by_thread.
-- thread_messages_by_thread previously omitted tshow; msg.thread (Get Thread Messages)
-- now returns the field so clients can distinguish tshow replies in thread context.
-- Backfill of pre-migration rows is out of scope: legacy rows are treated as tshow=false.
ALTER TABLE chat.thread_messages_by_thread ADD tshow BOOLEAN;
