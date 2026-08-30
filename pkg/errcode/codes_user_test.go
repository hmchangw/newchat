package errcode

import "testing"

func TestUserReasons(t *testing.T) {
	// #nosec G101 -- reason-code table, not credentials; asserts each Reason's wire string.
	// nosemgrep: gosec.G101-1
	cases := map[Reason]string{
		UserAppNotFound:          "app_not_found",
		UserAppDisabled:          "app_disabled",
		UserSubscriptionNotFound: "subscription_not_found",
		UserSSOTokenNotFound:     "sso_token_not_found",

		UserPriorityContactLimit:    "priority_contact_limit",
		UserPriorityContactNotFound: "priority_contact_not_found",
	}
	for r, want := range cases {
		if string(r) != want {
			t.Errorf("reason %q != %q", string(r), want)
		}
	}
}
