package errcode

// Permission-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	PermissionUnknownKey       Reason = "unknown_permission"
	PermissionInvalidSubjects  Reason = "invalid_subject_count"
	PermissionInvalidReason    Reason = "invalid_reason"
	PermissionMissingFields    Reason = "missing_permission_fields"
	PermissionInvalidWindow    Reason = "invalid_permission_window"
	PermissionUnexpectedWindow Reason = "unexpected_permission_window"
	PermissionInactiveSubject  Reason = "inactive_subject"
	PermissionUnknownAccounts  Reason = "unknown_accounts"
)
