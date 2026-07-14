package errcode

// Reasons emitted by room-service and room-worker.
const (
	RoomMaxSizeReached            Reason = "max_room_size_reached"
	RoomNotMember                 Reason = "not_room_member"
	RoomNotOwner                  Reason = "not_room_owner"
	RoomLastOwnerCannotLeave      Reason = "last_owner_cannot_leave"
	RoomBotInChannel              Reason = "bot_in_channel"
	RoomBotNotAvailable           Reason = "bot_not_available"
	RoomUserNotFound              Reason = "user_not_found"
	RoomInvalidOrg                Reason = "invalid_org"
	RoomSelfDM                    Reason = "self_dm"
	RoomLastMemberCannotRemove    Reason = "last_member_cannot_remove"
	RoomTargetNotMember           Reason = "target_not_member"
	RoomAlreadyOwner              Reason = "already_owner"
	RoomCannotDemoteLastOwner     Reason = "cannot_demote_last_owner"
	RoomPromoteRequiresIndividual Reason = "promote_requires_individual"
	// RoomNonChannelOperation marks operations that are only supported on
	// channel rooms (add-member, remove-member, role update) but were invoked
	// against a DM or bot-DM. The frontend uses it to render a "this only
	// works in channels" hint instead of a generic 400.
	RoomNonChannelOperation Reason = "non_channel_operation"
	// RoomReadReceiptsUnavailable marks a read-receipt request that could not be
	// served because the message-history service used to resolve the target
	// message is unreachable. room-service treats this as a soft dependency —
	// core room operations keep working — so the frontend should show "read
	// receipts temporarily unavailable" and allow a retry rather than surfacing a
	// hard failure.
	RoomReadReceiptsUnavailable Reason = "read_receipts_unavailable"
)
