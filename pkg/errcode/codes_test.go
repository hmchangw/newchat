package errcode

import (
	"regexp"
	"testing"
)

var allReasons = []Reason{
	RoomMaxSizeReached, RoomNotMember, RoomNotOwner,
	RoomLastOwnerCannotLeave, RoomBotInChannel, RoomBotNotAvailable,
	RoomBotCannotBeOwner,
	RoomUserNotFound, RoomInvalidOrg,
	RoomSelfDM, RoomLastMemberCannotRemove, RoomTargetNotMember,
	RoomAlreadyOwner, RoomCannotDemoteLastOwner, RoomPromoteRequiresIndividual,
	RoomNonChannelOperation,
	MessageLargeRoomPostRestricted, MessageNotSubscribed, MessageOutsideAccessWindow,
	PinDisabled, PinLimitReached, PinRoomTooLarge,
	UserAppNotFound, UserAppDisabled, UserSubscriptionNotFound, UserSSOTokenNotFound,
	AuthTokenExpired, AuthInvalidToken, AuthInvalidRequest, AuthInvalidNKey, AuthMissingFields,
	PortalAccountNotReady,
	RequestIDRequired, NatsNoResponders, NatsRequestTimeout, ResponseTooLarge,
	EmojiShortcodeReserved,
	EmojiDeleteDisabled,
	PermissionUnknownKey, PermissionInvalidSubjects, PermissionInvalidReason,
	PermissionMissingFields, PermissionInvalidWindow, PermissionUnexpectedWindow,
	PermissionInactiveSubject, PermissionUnknownAccounts,
}

func TestReasons_SnakeCase(t *testing.T) {
	re := regexp.MustCompile(`^[a-z][a-z0-9_]*[a-z0-9]$`)
	for _, r := range allReasons {
		if !re.MatchString(string(r)) {
			t.Errorf("reason %q is not flat snake_case", r)
		}
	}
}

func TestReasons_Unique(t *testing.T) {
	seen := map[Reason]bool{}
	for _, r := range allReasons {
		if seen[r] {
			t.Errorf("duplicate reason: %q", r)
		}
		seen[r] = true
	}
}

// The oversize fallback envelope is a wire contract documented in
// docs/client-api.md §6: clients branch on this exact reason string rather
// than on the message text, so a rename here is a breaking API change.
func TestResponseTooLarge_WireValue(t *testing.T) {
	if got := string(ResponseTooLarge); got != "response_too_large" {
		t.Errorf("ResponseTooLarge = %q, want %q", got, "response_too_large")
	}
}
