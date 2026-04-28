package main

import (
	"errors"
	"regexp"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// Sentinel errors for user-facing validation failures.
var (
	errInvalidRole      = errors.New("invalid role: must be owner or member")
	errOnlyOwners       = errors.New("only owners can update roles")
	errAlreadyOwner     = errors.New("user is already an owner")
	errNotOwner         = errors.New("user is not an owner")
	errCannotDemoteLast = errors.New("cannot demote the last owner")
	errRoomTypeGuard    = errors.New("role update is only allowed in channel rooms")
	errTargetNotMember  = errors.New("target user is not a member of this room")
	// Used by both list-members (requester subscription check) and add-member
	// channel-source expansion. Both contexts mean "the requester is not a
	// member of the room they are asking about".
	errNotRoomMember = errors.New("only room members can list members")
	errInvalidOrg    = errors.New("invalid org")
	// Only subscribers with an individual membership source can hold the owner
	// role. Remove-member's dual-membership path relies on this invariant:
	// stripping the owner role during an individual-leave is only sound when
	// the role can only be held alongside an individual entry.
	errPromoteRequiresIndividual = errors.New("only individual members can be promoted to owner")
)

var botPattern = regexp.MustCompile(`\.bot$|^p_`)

// hasRole checks if a given role is present in a slice of roles.
func hasRole(roles []model.Role, target model.Role) bool {
	for _, r := range roles {
		if r == target {
			return true
		}
	}
	return false
}

// isBot returns true if an account name matches the bot naming pattern.
func isBot(account string) bool { return botPattern.MatchString(account) }

// filterBots removes bot accounts from a slice of account names.
func filterBots(accounts []string) []string {
	var filtered []string
	for _, a := range accounts {
		if !isBot(a) {
			filtered = append(filtered, a)
		}
	}
	return filtered
}

// dedup removes duplicate strings from a slice while preserving order.
func dedup(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var result []string
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

// sanitizeError returns a user-safe error message for known error sentinels and approved patterns.
func sanitizeError(err error) string {
	switch {
	case errors.Is(err, errNotRoomMember):
		// Always return the sentinel message, even when wrapped (e.g. by
		// add-member's "expand channels: %w"), so callers get a clean
		// user-safe message without the wrapping context.
		return errNotRoomMember.Error()
	case errors.Is(err, errInvalidRole),
		errors.Is(err, errOnlyOwners),
		errors.Is(err, errAlreadyOwner),
		errors.Is(err, errNotOwner),
		errors.Is(err, errCannotDemoteLast),
		errors.Is(err, errRoomTypeGuard),
		errors.Is(err, errTargetNotMember),
		errors.Is(err, errInvalidOrg),
		errors.Is(err, errPromoteRequiresIndividual):
		return err.Error()
	default:
		msg := err.Error()
		for _, safe := range []string{"only owners can", "cannot add members", "room is at maximum capacity", "requester not in room", "invalid request", "remote member.list:"} {
			if strings.Contains(msg, safe) {
				return msg
			}
		}
		return "internal error"
	}
}
