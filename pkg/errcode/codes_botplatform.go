package errcode

// Reasons emitted by botplatform-service (and by portal-service / auth-service on forwarding paths).
const (
	// BotplatformInvalidCredentials: uniform 401 (unknown account, wrong password, SSO-only).
	// Not enumerated to avoid revealing which accounts are password-eligible.
	BotplatformInvalidCredentials Reason = "invalid_credentials"

	// BotplatformInvalidToken: 401 for /v1/auth/validate when the session hash is not found.
	BotplatformInvalidToken Reason = "invalid_token"

	// BotplatformAmbiguousToken: 400 when both ssoToken and authToken are supplied.
	BotplatformAmbiguousToken Reason = "ambiguous_token"

	// BotplatformMissingToken: 400 when neither ssoToken nor authToken is supplied.
	BotplatformMissingToken Reason = "missing_token"

	// BotplatformUpstreamUnavailable: 502 when portal/auth cannot reach the home-site BP.
	BotplatformUpstreamUnavailable Reason = "upstream_unavailable"

	// BotplatformSiteUnknown: 500 when PORTAL_SITE_URLS has no entry for the user's siteId.
	BotplatformSiteUnknown Reason = "site_unknown"

	// BotNotABot: 403 when the session lacks the bot role, or x-user-id disagrees with the session.
	// Merged so the wire doesn't reveal which mismatched.
	BotNotABot Reason = "not_a_bot"

	// BotInFlight: 409 when an identical bot request (same opID) is already in flight. Retry-After: 1.
	BotInFlight Reason = "in_flight"

	// BotRateLimitedCaller / Global: 429 with Retry-After from the bucket.
	BotRateLimitedCaller Reason = "rate_limited_caller"
	BotRateLimitedGlobal Reason = "rate_limited_global"

	// BotContentInvalid: 400 for structural body failures (empty/oversized content, missing name, ...).
	BotContentInvalid Reason = "content_invalid"

	// BotUnknownField: 400 on undeclared body fields; primary vehicle for the no-attachments guarantee.
	BotUnknownField Reason = "unknown_field"

	// BotBatchTooLarge: 400 when a create/add/remove batch exceeds the per-endpoint caps.
	BotBatchTooLarge Reason = "batch_too_large"

	// BotHandlerTimeout: 503 when a BP -> handler NATS req/reply exceeds its budget.
	BotHandlerTimeout Reason = "handler_timeout"

	// BotNotARoomMember: 403 when the bot has no local subscription for the target room.
	BotNotARoomMember Reason = "not_a_room_member"

	// BotNotARoomOwner: 403 when the caller is not the room's creator on add/remove.
	BotNotARoomOwner Reason = "not_a_room_owner"

	// BotRoomNotFound: 404 when the room lookup fails at the origin site.
	BotRoomNotFound Reason = "room_not_found"

	// BotMemberNotFound: 404 when a create/add target userID has no local users doc.
	BotMemberNotFound Reason = "member_not_found"

	// BotMentionInvalid: 400 when a canonicalized mention hits a non-member or missing userID.
	BotMentionInvalid Reason = "mention_invalid"

	// BotInvalidHeader: 400 when X-Bot-Identity / X-Bot-Message-ID / X-Bot-Created-At are missing/malformed.
	// Indicates a BP wiring bug; only BP is account-permissioned to publish on chat.server.bot.request.>.
	BotInvalidHeader Reason = "invalid_header"

	// BotCannotDMSelf: 400 when a bot attempts to DM itself.
	BotCannotDMSelf Reason = "cannot_dm_self"

	// BotDMTargetNotFound: 404 when the DM target has no users doc at the origin site.
	BotDMTargetNotFound Reason = "dm_target_not_found"

	// BotCannotRemoveSelf: 403 when a bot attempts to remove itself via member.remove.
	BotCannotRemoveSelf Reason = "cannot_remove_self"

	// BotRoomExists: 409 when create-room collides on the generated roomID (vanishingly rare).
	BotRoomExists Reason = "room_exists"

	// BotUnsupported: 400 for fields not yet implemented (e.g. org expansion).
	BotUnsupported Reason = "unsupported"
)
