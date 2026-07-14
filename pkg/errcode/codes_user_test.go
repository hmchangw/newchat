package errcode

import "testing"

func TestUserReasons(t *testing.T) {
	cases := map[Reason]string{
		UserAppNotFound:          "app_not_found",
		UserAppDisabled:          "app_disabled",
		UserInvalidDMTarget:      "invalid_dm_target",
		UserSubscriptionNotFound: "subscription_not_found",
	}
	for r, want := range cases {
		if string(r) != want {
			t.Errorf("reason %q != %q", string(r), want)
		}
	}
}
