package errcode

// Admin-service reasons. Emitted by admin-service handlers/middleware.
const (
	AdminNotAuthorized Reason = "not_admin"      // 403: valid session, role != admin
	AdminInvalidToken  Reason = "invalid_token"  // 401: missing/unknown session token
	AdminUserNotFound  Reason = "user_not_found" // 404
	AdminAccountExists Reason = "account_exists" // 409: duplicate account on create
)
