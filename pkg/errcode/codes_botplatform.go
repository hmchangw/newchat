package errcode

// Reasons emitted by botplatform-service (and by portal-service / auth-service
// on their forwarding paths to botplatform-service).
const (
	// BotplatformInvalidCredentials: uniform 401 covering unknown account,
	// wrong password, and SSO-only accounts that lack bot/admin role.
	// Deliberately not enumerated to avoid revealing which accounts are
	// password-eligible.
	BotplatformInvalidCredentials Reason = "invalid_credentials"

	// BotplatformInvalidToken: 401 for /v1/auth/validate when the session
	// hash is not found in the local sessions collection.
	BotplatformInvalidToken Reason = "invalid_token"

	// BotplatformAmbiguousToken: 400 when an auth-service request carries
	// BOTH ssoToken and authToken (mutually exclusive).
	BotplatformAmbiguousToken Reason = "ambiguous_token"

	// BotplatformMissingToken: 400 when an auth-service request carries
	// NEITHER ssoToken nor authToken.
	BotplatformMissingToken Reason = "missing_token"

	// BotplatformUpstreamUnavailable: 502 when portal-service can't reach
	// the home-site botplatform OR auth-service can't reach the local
	// botplatform validate endpoint.
	BotplatformUpstreamUnavailable Reason = "upstream_unavailable"

	// BotplatformSiteUnknown: 500 when portal-service resolves a user's
	// siteId but PORTAL_SITE_URLS has no entry for it. Configuration gap.
	BotplatformSiteUnknown Reason = "site_unknown"
)
