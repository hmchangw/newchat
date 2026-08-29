package errcode

// Admin-service reasons. Emitted by admin-service handlers/middleware.
const (
	AdminNotAuthorized Reason = "not_admin" // 403: valid session, role != admin
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	AdminInvalidToken  Reason = "invalid_token"  // 401: missing/unknown session token
	AdminUserNotFound  Reason = "user_not_found" // 404
	AdminAccountExists Reason = "account_exists" // 409: duplicate account on create
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	AdminInvalidCredentials Reason = "invalid_credentials" // 401: /v1/login denied (unknown / wrong password / not admin / deactivated)
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	AdminOldPasswordMismatch  Reason = "old_password_mismatch"  // 401: /v1/password/change oldPassword wrong
	AdminMixedDeactivatePatch Reason = "mixed_deactivate_patch" // 400: PATCH mixes active=false with other field updates
)
