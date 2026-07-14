package errcode

// User-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserInvalidDMTarget      Reason = "invalid_dm_target"
	UserSubscriptionNotFound Reason = "subscription_not_found"
)
