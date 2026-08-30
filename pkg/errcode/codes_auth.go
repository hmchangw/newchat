package errcode

// Reasons emitted by auth-service.
const (
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	AuthTokenExpired Reason = "sso_token_expired"
	// #nosec G101 -- wire-format reason code, not a credential
	// nosemgrep: gosec.G101-1
	AuthInvalidToken   Reason = "invalid_sso_token"
	AuthInvalidRequest Reason = "invalid_request"
	AuthInvalidNKey    Reason = "invalid_nkey"
	AuthMissingFields  Reason = "missing_fields"
)
