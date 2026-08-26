package errcode

// client-update-service reasons. Emitted by its service-account auth middleware.
const (
	ClientUpdateInvalidToken  Reason = "invalid_token"  // 401: missing, malformed, unsigned, or expired service token
	ClientUpdateNotAuthorized Reason = "not_authorized" // 403: valid token whose subject is not in ALLOWED_SERVICE_ACCOUNTS
)
