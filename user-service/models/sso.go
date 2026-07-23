package models

// SSOSetRequest stores the caller's own SSO token pair (sso.set); the set response reuses OKResponse.
type SSOSetRequest struct {
	SSOToken     string `json:"ssoToken"`
	RefreshToken string `json:"refreshToken"`
}

// SSORefreshRequest retrieves (and maybe refreshes) the caller's stored SSO token (sso.refresh);
// the payload carries no fields — an empty body is expected (self-service).
type SSORefreshRequest struct{}

// SSORefreshResponse carries the stored or freshly-refreshed ssoToken.
type SSORefreshResponse struct {
	SSOToken string `json:"ssoToken"`
}
