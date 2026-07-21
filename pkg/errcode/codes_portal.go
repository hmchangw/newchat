package errcode

// Reasons emitted by portal-service.
const (
	// PortalAccountNotReady: account absent from the portal's in-memory employee directory cache (portal lookup).
	PortalAccountNotReady Reason = "account_not_ready"
	// PortalBotLoginDisabled: portal /api/v1/login rejects a bot-role login because BOT_LOGIN_ENABLED=false.
	PortalBotLoginDisabled Reason = "bot_login_disabled"
)
