package errcode

import (
	"regexp"
	"testing"
)

var allReasons = []Reason{
	RoomMaxSizeReached, RoomNotMember, RoomNotOwner,
	RoomLastOwnerCannotLeave, RoomBotInChannel, RoomBotNotAvailable,
	RoomBotCrossSite, RoomBotCannotBeOwner,
	RoomUserNotFound, RoomInvalidOrg,
	RoomSelfDM, RoomLastMemberCannotRemove, RoomTargetNotMember,
	RoomAlreadyOwner, RoomCannotDemoteLastOwner, RoomPromoteRequiresIndividual,
	RoomNonChannelOperation,
	MessageLargeRoomPostRestricted, MessageNotSubscribed, MessageOutsideAccessWindow,
	PinDisabled, PinLimitReached, PinRoomTooLarge,
	UserAppNotFound, UserAppDisabled, UserInvalidDMTarget, UserSubscriptionNotFound,
	AuthTokenExpired, AuthInvalidToken, AuthInvalidRequest, AuthInvalidNKey, AuthMissingFields,
	PortalAccountNotReady,
	RequestIDRequired,
	EmojiShortcodeReserved,
	EmojiDeleteDisabled,
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
