package errcode

// User-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserSubscriptionNotFound Reason = "subscription_not_found"
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	UserSSOTokenNotFound Reason = "sso_token_not_found"

	// Chatlist section reasons — the client branches on each.
	UserChatlistInvalidName      Reason = "chatlist_invalid_name"
	UserChatlistDuplicateName    Reason = "chatlist_duplicate_name"
	UserChatlistBuiltinImmutable Reason = "chatlist_builtin_immutable"
	UserChatlistSectionNotFound  Reason = "chatlist_section_not_found"
	UserChatlistInvalidOrder     Reason = "chatlist_invalid_order"
	UserChatlistInvalidSortMode  Reason = "chatlist_invalid_sort_mode"
	UserChatlistBuiltinTarget    Reason = "chatlist_builtin_target"

	// Priority-contact reasons — the client branches on each.
	UserPriorityContactLimit    Reason = "priority_contact_limit"
	UserPriorityContactNotFound Reason = "priority_contact_not_found"
)
