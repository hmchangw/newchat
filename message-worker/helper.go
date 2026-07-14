package main

import "time"

// mentionVisible reports whether a mentionee whose subscription carries
// historySharedSince may see a thread reply with parent createdAt parentCreatedAt.
// nil window = full access; a set window with a missing/older parent = no access.
func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool {
	if historySharedSince == nil {
		return true
	}
	if parentCreatedAt == nil {
		return false
	}
	return !parentCreatedAt.Before(*historySharedSince)
}
