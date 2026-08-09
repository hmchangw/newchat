package errcode

// User-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserSubscriptionNotFound Reason = "subscription_not_found"
	UserSSOTokenNotFound     Reason = "sso_token_not_found"

	// Chatlist section reasons — the client branches on each.
	UserChatlistInvalidName      Reason = "chatlist_invalid_name"
	UserChatlistDuplicateName    Reason = "chatlist_duplicate_name"
	UserChatlistBuiltinImmutable Reason = "chatlist_builtin_immutable"
	UserChatlistSectionNotFound  Reason = "chatlist_section_not_found"
	UserChatlistInvalidOrder     Reason = "chatlist_invalid_order"
	UserChatlistInvalidSortMode  Reason = "chatlist_invalid_sort_mode"
	UserChatlistBuiltinTarget    Reason = "chatlist_builtin_target"
)
