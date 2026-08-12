package errcode

// Reasons emitted by portal-service.
const (
	// PortalAccountNotReady: account absent from the portal's in-memory employee directory cache (portal lookup).
	PortalAccountNotReady Reason = "account_not_ready"
	// PortalBotLoginDisabled: portal /api/v1/login rejects a bot-role login because BOT_LOGIN_ENABLED=false.
	PortalBotLoginDisabled Reason = "bot_login_disabled"
	// PortalFailoverUnauthorized: the failover control surface rejected a request whose ops bearer token was missing or wrong.
	PortalFailoverUnauthorized Reason = "failover_unauthorized"
	// PortalFailoverIllegalTransition: the requested failover action is not valid from the site's current status.
	PortalFailoverIllegalTransition Reason = "failover_illegal_transition"
	// PortalFailoverVersionConflict: the failover state changed concurrently (optimistic-concurrency CAS lost); retry.
	PortalFailoverVersionConflict Reason = "failover_version_conflict"
)
