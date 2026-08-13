// Package botauth validates botplatform session tokens for the services that gate
// endpoints on a bot/admin credential. botplatform alone owns the sessions store.
package botauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/principal"
)

// Headers carrying the bot credential, matching the casing bot SDKs already
// send to botplatform-service. Header lookup is case-insensitive.
const (
	HeaderUserID    = "x-user-id"
	HeaderAuthToken = "x-auth-token"
)

// validatePath is botplatform's session-validation endpoint (client-api.md §10.2).
const validatePath = "/api/v1/auth/validate"

// validateTimeout bounds the detached shared call so a hung botplatform cannot
// leak the singleflight goroutine or pin the in-flight key.
const validateTimeout = 10 * time.Second

// maxInFlight caps concurrent upstream validations. The singleflight key is the
// caller-supplied token, so its cardinality is unbounded: distinct tokens each
// start their own detached call, and without a ceiling a client spraying unique
// tokens could accumulate goroutines and sockets faster than they drain.
const maxInFlight = 64

// errShedded is safe to share, unlike the errcode envelopes: it is immutable and
// never reaches a client, only a WithCause log line.
var errShedded = errors.New("validation capacity exhausted")

// TokenValidator resolves a raw session token to its principal. Authenticate
// consumes it; *Validator is the production implementation.
type TokenValidator interface {
	Validate(ctx context.Context, authToken string) (principal.Principal, error)
}

// Validator resolves session tokens against a botplatform-service instance.
type Validator struct {
	client  *resty.Client
	baseURL string
	sf      singleflight.Group
	sem     chan struct{}
}

// NewValidator returns a Validator for baseURL, which must be the LOCAL site's
// botplatform — validation is a local-DB lookup there.
func NewValidator(client *resty.Client, baseURL string) *Validator {
	return &Validator{client: client, baseURL: baseURL, sem: make(chan struct{}, maxInFlight)}
}

// Validate exchanges a session token for its principal: a typed Unauthenticated
// on 401, valid:false, or a principal with no userId; a raw error on transport
// failure, any other status, and on caller cancellation.
//
// Concurrent calls for the same token share one upstream request, so a bot
// fetching N attachments in parallel costs botplatform one validation, not N.
// Nothing is cached between requests: the shared window is a single in-flight
// call, so a revoked token cannot outlive it. Waiters share one result value,
// so consumers must treat it as read-only.
func (v *Validator) Validate(ctx context.Context, authToken string) (principal.Principal, error) {
	resCh := v.sf.DoChan(authToken, func() (val any, err error) {
		// singleflight re-raises a panic on its own goroutine, where no
		// gin.Recovery can reach it, so an unhandled one here kills the process
		// rather than failing the request. Contain it as an error instead.
		defer func() {
			if r := recover(); r != nil {
				val, err = nil, fmt.Errorf("validate session token: panic: %v", r)
			}
		}()

		// Shed rather than queue: a full pool means botplatform is already
		// struggling, and waiting would just convert it into pile-up here.
		// Raw, so callers wrap it into the same 503 as an unreachable botplatform
		// — shedding is our capacity state, not something the wire should name.
		select {
		case v.sem <- struct{}{}:
			defer func() { <-v.sem }()
		default:
			return nil, errShedded
		}

		// Detached from the first caller's ctx: its cancellation must not fail
		// the validation everyone else is waiting on. maxInFlight bounds how much
		// work this can strand.
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), validateTimeout)
		defer cancel()
		return v.validate(callCtx, authToken)
	})

	select {
	case res := <-resCh:
		if res.Err != nil {
			return principal.Principal{}, res.Err
		}
		return res.Val.(principal.Principal), nil
	case <-ctx.Done():
		return principal.Principal{}, ctx.Err()
	}
}

// validate performs the actual round trip. Callers go through Validate, which
// coalesces concurrent requests for the same token.
func (v *Validator) validate(ctx context.Context, authToken string) (principal.Principal, error) {
	var body struct {
		Valid     bool                `json:"valid"`
		Principal principal.Principal `json:"principal"`
	}
	req := v.client.R().
		SetContext(ctx).
		SetBody(map[string]string{"authToken": authToken}).
		SetResult(&body)
	if id := natsutil.RequestIDFromContext(ctx); id != "" {
		req = req.SetHeader(natsutil.RequestIDHeader, id)
	}

	resp, err := req.Post(v.baseURL + validatePath)
	if err != nil {
		return principal.Principal{}, fmt.Errorf("validate session token: %w", err)
	}
	switch resp.StatusCode() {
	case http.StatusOK:
		// No userId cannot be matched against a caller — reject rather than return an
		// empty identity a direct Validate caller could mistake for authenticated.
		if !body.Valid || body.Principal.UserID == "" {
			return principal.Principal{}, errInvalidToken()
		}
		return body.Principal, nil
	case http.StatusUnauthorized:
		return principal.Principal{}, errInvalidToken()
	default:
		// Body length only — a validate response can echo request material.
		return principal.Principal{}, fmt.Errorf("botplatform validate: HTTP %d (body %d bytes)",
			resp.StatusCode(), len(resp.Body()))
	}
}

// Credentials returns the bot user ID and raw session token from a request's
// headers. Either may be empty; Authenticate rejects that.
func Credentials(h http.Header) (userID, token string) {
	return h.Get(HeaderUserID), h.Get(HeaderAuthToken)
}

// Authenticate validates token and confirms it belongs to userID. Missing, unknown
// and mismatched all return the same 401; an unreachable botplatform returns 503.
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
		return principal.Principal{}, errcode.Unavailable("botplatform unavailable",
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
