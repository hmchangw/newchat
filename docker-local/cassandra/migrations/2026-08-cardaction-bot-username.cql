-- Add bot_username to the CardAction UDT.
--
-- The desktop / KR-backend client attaches a botUsername on the cardAction
-- payload of a tcard_execute event to name the target bot for the callback.
-- The column is optional and legacy card actions predate it (they read back
-- as null); the change is additive and non-destructive.
--
-- Fresh / docker-local clusters get the field via the init script
-- (docker-local/cassandra/init/04-udt-card_action.cql); this migration
-- applies the same shape to a live cluster.
--
-- ALTER TYPE ADD is safe on a running cluster: existing rows do not need to
-- be rewritten, and readers using the reflection-based UDT decoder ignore
-- unknown fields on old rows.

ALTER TYPE chat."CardAction" ADD bot_username TEXT;
