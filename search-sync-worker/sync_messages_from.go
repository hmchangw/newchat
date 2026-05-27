package main

import (
	"fmt"
	"time"
)

// syncMessagesFromLayout is the only accepted form for SYNC_MESSAGES_FROM:
// a date-only YYYY-MM-DD, parsed as UTC midnight. Operators set this when
// migrating legacy message data into JetStream to skip records older than
// the date.
const syncMessagesFromLayout = "2006-01-02"

// parseSyncMessagesFrom parses the SYNC_MESSAGES_FROM env value into a UTC
// cutoff for the messages collection. An empty string disables the filter
// and returns the zero time.Time; any non-empty value MUST parse as
// YYYY-MM-DD or the worker fails fast at startup (CLAUDE.md "fail fast on
// bad config").
//
// Only the messages collection consumes this cutoff — it compares against
// the DOMAIN-level Message.CreatedAt, not the event publish timestamp. A
// migrator stamps evt.Timestamp at publish time, so it does not reflect
// the original data age. Spotlight and user-room are intentionally NOT
// filtered: a user must still be able to discover and search a room they
// joined long ago.
func parseSyncMessagesFrom(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.ParseInLocation(syncMessagesFromLayout, raw, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse SYNC_MESSAGES_FROM %q: must be YYYY-MM-DD: %w", raw, err)
	}
	return t, nil
}
