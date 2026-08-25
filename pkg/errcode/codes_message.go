package errcode

// Reasons emitted by message-gatekeeper and history-service.
const (
	MessageLargeRoomPostRestricted Reason = "large_room_post_restricted"
	MessageNotSubscribed           Reason = "not_subscribed"
	// MessageOutsideAccessWindow distinguishes "caller IS subscribed but the
	// message predates HSS" from MessageNotSubscribed — the frontend renders
	// different UX ("history hidden before you joined").
	MessageOutsideAccessWindow Reason = "outside_access_window"
	// Pin-feature reasons — all three are "forbidden" cases the frontend needs
	// to distinguish to render the right copy (kill-switch vs hard cap vs
	// large-room gate). "not subscribed" reuses MessageNotSubscribed above.
	PinDisabled     Reason = "pin_disabled"
	PinLimitReached Reason = "pin_limit_reached"
	PinRoomTooLarge Reason = "pin_room_too_large"
	// MessageThreadStartUnavailable: the sender is starting a NEW thread while
	// history is unreachable. The thread has no thread_rooms document, so no
	// consumer can resolve the parent and the reply would reach nobody. Replies to
	// existing threads are unaffected — the frontend uses this to disable thread
	// starts, not thread replies.
	MessageThreadStartUnavailable Reason = "thread_start_unavailable"
)
