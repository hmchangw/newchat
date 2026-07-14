package errcode

// Reasons emitted by portal-service.
const (
	// PortalAccountNotReady: account absent from the portal's in-memory employee directory cache (portal lookup).
	PortalAccountNotReady Reason = "account_not_ready"
)
