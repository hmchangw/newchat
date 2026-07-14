// Package principal centralizes the principal wire type that flows between
// botplatform-service (issuer) and auth-service / gateway (consumers). Both
// services serialize/deserialize from the same wire bytes; putting the type
// here eliminates the drift risk of mirror copies on each service.
package principal

// Principal is the canonical wire shape of a session principal. It is the
// payload of botplatform-service's POST /v1/auth/validate response and the
// input shape consumed by auth-service when minting a NATS JWT.
//
// The Roles list is passed through as-is; auth-service does not currently
// derive JWT permissions from it — permissions come from the scoped signing
// key template on the NATS server side, keyed off the `account:<name>` tag
// stamped on every user JWT. Role-specific scoped signing keys (so a bot
// gets `chat.bot.<stripped>.>` and an admin gets `chat.>`) is planned as a
// follow-up; for now bots and admins get the same scoped_user template
// perms as an SSO user (i.e. `chat.user.<account>.>`).
type Principal struct {
	UserID  string   `json:"userId"`
	Account string   `json:"account"`
	SiteID  string   `json:"siteId"`
	Roles   []string `json:"roles"`
}
