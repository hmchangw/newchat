package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/model"
)

// Sentinel errors for user-facing validation failures, typed as *errcode.Error
// so they classify directly at the reply boundary (errnats.Reply) without a
// per-message allowlist.
//
// These are package-level singletons SHARED across all goroutines. Callers
// MUST NOT mutate (today's Options return fresh *Error values so mutation is
// not a concern, but a future Option that wrote in place would silently alias
// state across callers). Use errors.Is for identity, errcode.HasReason for
// reason matching, and construct fresh *Error values via the named
// constructors when a caller needs a wrapped message or extra metadata.
var (
	errInvalidRole           = errcode.BadRequest("invalid role: must be owner or member")
	errOnlyOwners            = errcode.Forbidden("only owners can update roles", errcode.WithReason(errcode.RoomNotOwner))
	errOnlyOwnersCanRemove   = errcode.Forbidden("only owners can remove members", errcode.WithReason(errcode.RoomNotOwner))
	errOnlyOwnersCanAddToRes = errcode.Forbidden("only owners can add members to a restricted room", errcode.WithReason(errcode.RoomNotOwner))
	errAlreadyOwner          = errcode.Conflict("user is already an owner", errcode.WithReason(errcode.RoomAlreadyOwner))
	errNotOwner              = errcode.Forbidden("user is not an owner", errcode.WithReason(errcode.RoomNotOwner))
	errCannotDemoteLast      = errcode.Conflict("cannot demote the last owner", errcode.WithReason(errcode.RoomCannotDemoteLastOwner))
	errRoomTypeGuard         = errcode.BadRequest("role update is only allowed in channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation))
	errAddMembersChannelOnly = errcode.BadRequest("cannot add members to a non-channel room", errcode.WithReason(errcode.RoomNonChannelOperation))
	errTargetNotMember       = errcode.BadRequest("target user is not a member of this room", errcode.WithReason(errcode.RoomTargetNotMember))
	// Shared sentinel for any membership-gated RPC (list-members,
	// member.statuses, subscription.mentionable, message.read) and the
	// add-member channel-source expansion.
	errNotRoomMember     = errcode.Forbidden("only room members can perform this action", errcode.WithReason(errcode.RoomNotMember))
	errInvalidThreadID   = errcode.BadRequest("threadId is required")
	errThreadSubNotFound = errcode.NotFound("thread subscription not found")
	// Only subscribers with an individual membership source can hold the owner
	// role. Remove-member's dual-membership path relies on this invariant:
	// stripping the owner role during an individual-leave is only sound when
	// the role can only be held alongside an individual entry.
	errPromoteRequiresIndividual = errcode.BadRequest("only individual members can be promoted to owner", errcode.WithReason(errcode.RoomPromoteRequiresIndividual))

	// Sentinels for create-room validation.
	errEmptyCreateRequest  = errcode.BadRequest("request must include at least one of users, orgs, channels, or name")
	errSelfDM              = errcode.BadRequest("cannot create a DM with yourself", errcode.WithReason(errcode.RoomSelfDM))
	errBotInChannel        = errcode.BadRequest("bots cannot be added to a channel", errcode.WithReason(errcode.RoomBotInChannel))
	errBotNotAvailable     = errcode.NotFound("bot not available", errcode.WithReason(errcode.RoomBotNotAvailable))
	errInvalidUserData     = errcode.BadRequest("user is missing required name fields")
	errChannelNameRequired = errcode.BadRequest("channel name is required")
	errChannelNameTooLong  = errcode.BadRequest("channel name must be at most 100 characters")

	errMessageNotFound     = errcode.NotFound("message not found")
	errMessageRoomMismatch = errcode.BadRequest("message does not belong to this room")
	errNotMessageSender    = errcode.Forbidden("only the message sender can view read receipts")

	// Sentinels for remove-member validation (surfaced to the client verbatim).
	errRemoveTargetAmbiguous    = errcode.BadRequest("exactly one of account or orgId must be set")
	errCannotRemoveLastMember   = errcode.Conflict("cannot remove the last member of the room", errcode.WithReason(errcode.RoomLastMemberCannotRemove))
	errLastOwnerCannotLeave     = errcode.Conflict("last owner cannot leave the room", errcode.WithReason(errcode.RoomLastOwnerCannotLeave))
	errOrgMemberCannotLeaveSolo = errcode.Forbidden("org members cannot leave individually")
	errRoomIDMismatch           = errcode.BadRequest("room ID mismatch")
	errRemoveChannelOnly        = errcode.BadRequest("remove-member only supported on channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation))

	// Sentinels for list-members pagination validation.
	errListLimitInvalid  = errcode.BadRequest("limit must be > 0")
	errListOffsetInvalid = errcode.BadRequest("offset must be >= 0")

	// Sentinels for member.statuses + subscription.mentionable limit validation.
	errMemberStatusesLimitInvalid = errcode.BadRequest("limit must be > 0 and <= room user count")
	errMentionableLimitInvalid    = errcode.BadRequest("limit must be > 0 and <= room user count + app count")

	// errRoomKeyAbsent is returned when the requested key version is not held
	// by the key store (either the current key is missing or the historical
	// version has aged out of the grace window).
	errRoomKeyAbsent = errcode.NotFound("room key not available")

	// Sentinels for rename and restricted operations.
	errOnlyOwnersOrAdmins       = errcode.Forbidden("only owners or platform admins can rename a channel")
	errOnlyAdmins               = errcode.Forbidden("only admins can change room restricted state")
	errOwnerNotMember           = errcode.BadRequest("owner account is not a member of this room")
	errOwnerAccountRequired     = errcode.BadRequest("owner account is required when restricting a room")
	errNotEnoughMembers         = errcode.Conflict("not enough members to restrict")
	errInvalidName              = errcode.BadRequest("invalid name")
	errRenameChannelOnly        = errcode.BadRequest("rename is only allowed in channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation))
	errRestrictedChannelOnly    = errcode.BadRequest("restricted change is only allowed in channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation))
	errRoomNotFound             = errcode.NotFound("room not found")
	errInvalidRenameSubject     = errcode.BadRequest("invalid rename subject")
	errInvalidRestrictedSubject = errcode.BadRequest("invalid restricted subject")
	// App-read RPC sentinels (app.tabs / app.cmd-menu). Forbidden when the
	// caller is neither a room member nor a platform admin; Internal when a
	// reply would exceed the negotiated NATS max_payload.
	errAppAccessDenied  = errcode.Forbidden("not authorized to access this room's apps", errcode.WithReason(errcode.RoomNotMember))
	errResponseTooLarge = errcode.Internal("response payload exceeds maximum size")
)

var botPattern = regexp.MustCompile(`\.bot$|^p_`)

// platformAdminRegex matches platform-admin / webhook accounts by their `p_`
// prefix. Mentionable autocomplete hides these accounts entirely so they do
// not appear as `@`-mention targets.
const platformAdminRegex = `^p_`

// sameFloor reports whether two read-floor pointers represent the same instant.
// Two nil pointers are equal (both "no floor"); a nil and a non-nil differ; two
// non-nil pointers compare by time value (millisecond instants from Mongo), not
// pointer identity.
func sameFloor(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}

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

// isPlatformAdmin returns true when u has the UserRoleAdmin role. Nil-safe.
func isPlatformAdmin(u *model.User) bool {
	if u == nil {
		return false
	}
	for _, r := range u.Roles {
		if r == model.UserRoleAdmin {
			return true
		}
	}
	return false
}

// determineRoomType classifies a post-strip request; caller must guarantee non-empty input.
// Uses the shared isBot predicate so both ".bot" suffix and "p_" prefix accounts
// classify as botDM, matching the bot-pattern guard used elsewhere in the service
// (filterBots, errBotInChannel) and in pkg/pipelines.
func determineRoomType(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		if isBot(req.Users[0]) {
			return model.RoomTypeBotDM
		}
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}

// contextWithMemberListTimeout returns a derived context bounded by the
// configured per-ref member-list timeout. When the configured timeout is
// non-positive, the parent ctx is returned unchanged with a no-op cancel.
func (h *Handler) contextWithMemberListTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if h.memberListTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, h.memberListTimeout)
}

// stripAccount returns slice with all occurrences of account removed (order preserved).
func stripAccount(slice []string, account string) []string {
	out := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != account {
			out = append(out, s)
		}
	}
	return out
}

// marshalBounded marshals v and enforces h.maxResponseBytes (<= 0 disables
// the bound). Returns (body, nil) on success, or (nil, err) suitable for
// errnats.Reply: errResponseTooLarge on a size violation, a wrapped marshal
// error (classified to a generic internal error) on a marshal failure.
func (h *Handler) marshalBounded(v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal bounded response: %w", err)
	}
	if h.maxResponseBytes > 0 && int64(len(body)) > h.maxResponseBytes {
		return nil, errResponseTooLarge
	}
	return body, nil
}

// replyBoundedJSON sends v on msg's reply subject, enforcing the negotiated
// NATS max_payload. nats* handlers use this in place of natsutil.ReplyJSON
// when a response payload could exceed max_payload; an oversize payload is
// surfaced to the caller via errnats.Reply rather than silently dropped.
func (h *Handler) replyBoundedJSON(ctx context.Context, msg *nats.Msg, v any) {
	body, err := h.marshalBounded(v)
	if err != nil {
		errnats.Reply(ctx, msg, err)
		return
	}
	if err := msg.Respond(body); err != nil {
		slog.ErrorContext(ctx, "reply failed", "error", err)
	}
}

// isURLSafeIDToken reports whether s is safe to inline into a URL
// template path without risk of injection. Allows alphanumerics,
// hyphen, underscore, dot, and tilde (RFC3986 unreserved); rejects
// characters that could escape the path segment (?, #, /, etc.).
func isURLSafeIDToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.' || r == '~':
		default:
			return false
		}
	}
	return true
}
