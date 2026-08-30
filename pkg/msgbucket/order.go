package msgbucket

import "time"

// NewerRow reports whether (at, id) sorts newer than (curAt, curID) in
// messages_by_room's clustering order: created_at DESC, message_id DESC.
//
// created_at alone does not identify a row — the table clusters by created_at
// AND message_id — so comparing it alone leaves same-instant messages resolved
// by arrival order, which need not match the order a walk reads them back in
// (#293).
//
// It lives here, beside the partition math, because both answer the same
// question about the same table: which partition a row lands in, and where it
// sorts inside one. It is exported because more than one writer coalesces the
// same message stream — broadcast-worker buffers the room-list preview while
// roomlist-worker buffers the room's lastMsgId — and the reader serves a stored
// preview only when the two agree on which message is the room's newest. Two
// copies of this rule would let them disagree at a tie, silently, forever.
func NewerRow(at time.Time, id string, curAt time.Time, curID string) bool {
	// Compared at the precision Cassandra STORES, not the precision Go carries.
	// created_at is a Cassandra timestamp — milliseconds — so two messages that
	// differ only in sub-millisecond digits are one clustering position there.
	// Comparing full Go precision would take the timestamp branch and skip the
	// id tiebreaker that exists to match that position, which is the whole point
	// of the comparator.
	a, b := at.UnixMilli(), curAt.UnixMilli()
	if a != b {
		return a > b
	}
	return id > curID
}
