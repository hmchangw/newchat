package errcode

// Reasons emitted by message-gatekeeper and history-service.
const (
	MessageLargeRoomPostRestricted Reason = "large_room_post_restricted"
	MessageNotSubscribed           Reason = "not_subscribed"
	// MessageOutsideAccessWindow distinguishes "caller IS subscribed but the
	// message predates HSS" from MessageNotSubscribed — the frontend renders
	// different UX ("history hidden before you joined").
	MessageOutsideAccessWindow Reason = "outside_access_window"
	// MessageThreadParentNotFound marks a reply whose threadParentMessageId does
	// not resolve, distinguishing it from a missing room on the same not_found
	// code so the frontend can refresh the thread instead of the room.
	MessageThreadParentNotFound Reason = "thread_parent_not_found"
	// Pin-feature reasons — all three are "forbidden" cases the frontend needs
	// to distinguish to render the right copy (kill-switch vs hard cap vs
	// large-room gate). "not subscribed" reuses MessageNotSubscribed above.
	PinDisabled     Reason = "pin_disabled"
	PinLimitReached Reason = "pin_limit_reached"
	PinRoomTooLarge Reason = "pin_room_too_large"
)
