// Package botauth validates botplatform session tokens for the services that gate
// endpoints on a bot/admin credential, by reading the sessions collection
// botplatform-service issues into (pkg/session).
package botauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

// Headers carrying the bot credential, matching the casing bot SDKs already
// send to botplatform-service. Header lookup is case-insensitive.
const (
	HeaderUserID = "x-user-id"
	// #nosec G101 -- HTTP header name, not a credential
	// nosemgrep: gosec.G101-1
	HeaderAuthToken = "x-auth-token"
)

// TokenValidator resolves a raw session token to its principal. Authenticate
// consumes it; *Validator is the production implementation.
type TokenValidator interface {
	Validate(ctx context.Context, authToken string) (principal.Principal, error)
}

// SessionFinder is the slice of session.Store a Validator reads. Declared here,
// in the consumer, so a service can hand in the full store or a narrower stub.
type SessionFinder interface {
	FindByHash(ctx context.Context, hash string) (*session.Session, error)
}

// Validator resolves session tokens straight from the shared sessions
// collection — the same read botplatform's /auth/validate performs, minus the
// HTTP hop, so a botplatform outage no longer takes every session caller down.
type Validator struct {
	sessions SessionFinder
}

// NewValidator returns a Validator over sessions, which must be the LOCAL
// site's session store: a session is issued against its holder's home site.
func NewValidator(sessions SessionFinder) *Validator {
	return &Validator{sessions: sessions}
}

// Validate exchanges a session token for its principal: a typed Unauthenticated
// when the session is unknown or carries no userId; a raw error when the store
// could not answer. Only the token's hash reaches the store.
func (v *Validator) Validate(ctx context.Context, authToken string) (principal.Principal, error) {
	s, err := v.sessions.FindByHash(ctx, sessiontoken.Hash(authToken))
	switch {
	case errors.Is(err, session.ErrNotFound):
		return principal.Principal{}, errInvalidToken()
	case err != nil:
		return principal.Principal{}, fmt.Errorf("find session: %w", err)
	}
	// No userId cannot be matched against a caller — reject rather than return an
	// empty identity a direct Validate caller could mistake for authenticated.
	if s.UserID == "" {
		return principal.Principal{}, errInvalidToken()
	}
	return principal.Principal{
		UserID:  s.UserID,
		Account: s.Account,
		SiteID:  s.SiteID,
		Roles:   s.Roles,
	}, nil
}

// Credentials returns the bot user ID and raw session token from a request's
// headers. Either may be empty; Authenticate rejects that.
func Credentials(h http.Header) (userID, token string) {
	return h.Get(HeaderUserID), h.Get(HeaderAuthToken)
}

// Authenticate validates token and confirms it belongs to userID. Missing, unknown
// and mismatched all return the same 401; an unreachable session store returns 503.
func Authenticate(ctx context.Context, v TokenValidator, userID, token string) (principal.Principal, error) {
	if userID == "" || token == "" {
		return principal.Principal{}, errInvalidToken()
	}

	p, err := v.Validate(ctx, token)
	if err != nil {
		// A typed error is already a client-facing verdict (invalid token);
		// anything raw means the check itself could not be completed.
		var ec *errcode.Error
		if errors.As(err, &ec) {
			// Re-issue, don't forward: a validator's own 401 wording would make an
			// unknown token distinguishable from the local rejections below.
			if ec.Code == errcode.CodeUnauthenticated {
				return principal.Principal{}, errInvalidToken()
			}
			return principal.Principal{}, ec
		}
		return principal.Principal{}, errcode.Unavailable("session store unavailable",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable),
			errcode.WithCause(err))
	}

	if p.UserID != userID {
		return principal.Principal{}, errInvalidToken()
	}
	return p, nil
}

// HasRole reports whether roles carries role. Takes the slice, not a Principal,
// so any roles carrier can pass one. Roles are as of token issue.
func HasRole(roles []string, role model.UserRole) bool {
	return slices.Contains(roles, string(role))
}

// errInvalidToken is the one envelope every rejection returns, so the wire cannot
// reveal which failed. Built per call: *errcode.Error has exported fields, and a
// shared pointer handed to every caller is one mutation away from a data race.
func errInvalidToken() error {
	return errcode.Unauthenticated("invalid session token",
		errcode.WithReason(errcode.BotplatformInvalidToken))
}
