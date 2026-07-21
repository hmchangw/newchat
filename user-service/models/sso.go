package models

// SSOSetRequest stores a user's SSO token pair (sso.set); Account targets another user (admin only, empty means caller) and the set response reuses OKResponse.
type SSOSetRequest struct {
	SSOToken     string `json:"ssoToken"`
	RefreshToken string `json:"refreshToken"`
	Account      string `json:"account,omitempty"`
}

// SSORefreshRequest retrieves (and maybe refreshes) a stored SSO token (sso.refresh); all fields optional, an empty payload means self-service.
type SSORefreshRequest struct {
	Account string `json:"account,omitempty"`
}

// SSORefreshResponse carries the stored or freshly-refreshed ssoToken.
type SSORefreshResponse struct {
	SSOToken string `json:"ssoToken"`
}
