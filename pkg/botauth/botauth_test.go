package botauth

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

// stubValidator is the Authenticate seam: it records the token it saw and
// returns the canned principal/error.
type stubValidator struct {
	principal principal.Principal
	err       error
	gotToken  string
}

func (s *stubValidator) Validate(_ context.Context, authToken string) (principal.Principal, error) {
	s.gotToken = authToken
	return s.principal, s.err
}

func TestCredentials(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantUserID string
		wantToken  string
	}{
		{
			name:       "both headers present",
			headers:    map[string]string{HeaderUserID: "u1", HeaderAuthToken: "tok"},
			wantUserID: "u1",
			wantToken:  "tok",
		},
		{
			name:       "canonical casing is accepted",
			headers:    map[string]string{"X-User-Id": "u1", "X-Auth-Token": "tok"},
			wantUserID: "u1",
			wantToken:  "tok",
		},
		{name: "missing token", headers: map[string]string{HeaderUserID: "u1"}, wantUserID: "u1"},
		{name: "missing user id", headers: map[string]string{HeaderAuthToken: "tok"}, wantToken: "tok"},
		{name: "no headers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.headers {
				h.Set(k, v)
			}
			userID, token := Credentials(h)
			assert.Equal(t, tt.wantUserID, userID)
			assert.Equal(t, tt.wantToken, token)
		})
	}
}

func TestHasRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		role  model.UserRole
		want  bool
	}{
		{name: "bot role present", roles: []string{"bot"}, role: model.UserRoleBot, want: true},
		{name: "admin among many", roles: []string{"user", "admin"}, role: model.UserRoleAdmin, want: true},
		{name: "role absent", roles: []string{"user"}, role: model.UserRoleBot},
		{name: "no roles", role: model.UserRoleBot},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasRole(tt.roles, tt.role))
		})
	}
}

// stubFinder is the session.Store slice Validator reads: it records the hash it
// was asked for and returns the canned session/error.
type stubFinder struct {
	session *session.Session
	err     error
	gotHash string
}

func (s *stubFinder) FindByHash(_ context.Context, hash string) (*session.Session, error) {
	s.gotHash = hash
	return s.session, s.err
}

func TestValidator_Validate(t *testing.T) {
	tests := []struct {
		name          string
		session       *session.Session
		err           error
		wantPrincipal principal.Principal
		wantCode      errcode.Code
		wantReason    errcode.Reason
		wantRawErr    bool
	}{
		{
			name: "valid session",
			session: &session.Session{
				ID: "h", UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
			},
			wantPrincipal: principal.Principal{
				UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
			},
		},
		{
			name:       "unknown session",
			err:        session.ErrNotFound,
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{
			// A session with no userId cannot be matched against a caller — it
			// must fail closed rather than return an empty identity as success.
			name:       "session without userId",
			session:    &session.Session{ID: "h", Account: "ghost.bot"},
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{name: "store fault is a raw error", err: errors.New("mongo: connection reset"), wantRawErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finder := &stubFinder{session: tt.session, err: tt.err}
			v := NewValidator(finder)

			got, err := v.Validate(context.Background(), "raw-token")

			assert.Equal(t, sessiontoken.Hash("raw-token"), finder.gotHash,
				"the store must be queried by the token hash, never the raw token")

			var ec *errcode.Error
			switch {
			case tt.wantRawErr:
				require.Error(t, err)
				assert.False(t, errors.As(err, &ec), "store faults must stay raw so the caller maps them to 503")
			case tt.wantCode != "":
				require.Error(t, err)
				require.ErrorAs(t, err, &ec)
				assert.Equal(t, tt.wantCode, ec.Code)
				assert.Equal(t, tt.wantReason, ec.Reason)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantPrincipal, got)
			}
		})
	}
}

// TestAuthenticate_StoreFaultIs503 pins the wire contract a store outage maps
// to: the same unavailable envelope the HTTP validator used, so clients see no
// change from the switch to a direct session read.
func TestAuthenticate_StoreFaultIs503(t *testing.T) {
	v := NewValidator(&stubFinder{err: errors.New("mongo: connection reset")})

	_, err := Authenticate(context.Background(), v, "u1", "tok")

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.BotplatformUpstreamUnavailable, ec.Reason)
	assert.Equal(t, "session store unavailable", ec.Message)
}

func TestAuthenticate(t *testing.T) {
	validPrincipal := principal.Principal{
		UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
	}

	tests := []struct {
		name       string
		userID     string
		token      string
		stub       *stubValidator
		wantCode   errcode.Code
		wantReason errcode.Reason
	}{
		{name: "valid bot session", userID: "u1", token: "tok", stub: &stubValidator{principal: validPrincipal}},
		{
			name: "missing token", userID: "u1",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "missing user id", token: "tok",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			// Same envelope as an unknown token: the wire must not reveal that the
			// token was real but belonged to a different user.
			name: "user id disagrees with session", userID: "someone-else", token: "tok",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "upstream says invalid", userID: "u1", token: "tok",
			stub: &stubValidator{err: errcode.Unauthenticated("session token invalid",
				errcode.WithReason(errcode.BotplatformInvalidToken))},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "upstream unreachable", userID: "u1", token: "tok",
			stub:     &stubValidator{err: errors.New("dial tcp: connection refused")},
			wantCode: errcode.CodeUnavailable, wantReason: errcode.BotplatformUpstreamUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Authenticate(context.Background(), tt.stub, tt.userID, tt.token)

			if tt.wantCode == "" {
				require.NoError(t, err)
				assert.Equal(t, validPrincipal, got)
				assert.Equal(t, "tok", tt.stub.gotToken)
				return
			}

			require.Error(t, err)
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tt.wantCode, ec.Code)
			assert.Equal(t, tt.wantReason, ec.Reason)
		})
	}
}

// Pins the uniform-rejection invariant: the client-visible message is part of the
// envelope, so asserting Code and Reason alone would not catch a leak.
func TestAuthenticate_RejectionsAreIndistinguishable(t *testing.T) {
	valid := principal.Principal{UserID: "u1", Account: "alerts.sa.bot", Roles: []string{"bot"}}

	cases := map[string]struct {
		userID, token string
		stub          *stubValidator
	}{
		"missing token":   {userID: "u1", stub: &stubValidator{principal: valid}},
		"missing user id": {token: "tok", stub: &stubValidator{principal: valid}},
		"user id mismatch": {
			userID: "someone-else", token: "tok",
			stub: &stubValidator{principal: valid},
		},
		// Deliberately worded differently from errInvalidToken's message.
		"upstream rejects with its own wording": {
			userID: "u1", token: "tok",
			stub: &stubValidator{err: errcode.Unauthenticated("session token invalid",
				errcode.WithReason(errcode.BotplatformInvalidToken))},
		},
	}

	var envelopes []string
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Authenticate(context.Background(), tc.stub, tc.userID, tc.token)

			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, errcode.CodeUnauthenticated, ec.Code)
			assert.Equal(t, errcode.BotplatformInvalidToken, ec.Reason)
			envelopes = append(envelopes, string(ec.Code)+"|"+string(ec.Reason)+"|"+ec.Message)
		})
	}

	for _, got := range envelopes {
		assert.Equal(t, envelopes[0], got, "every rejection must be byte-identical on the wire")
	}
}

func TestAuthenticate_NonAuthTypedErrorPassesThrough(t *testing.T) {
	// Only 401s are re-issued to keep rejections uniform. A different typed
	// verdict is a distinct condition and must reach the caller intact.
	stub := &stubValidator{err: errcode.TooManyRequests("slow down",
		errcode.WithReason(errcode.BotRateLimitedCaller))}

	_, err := Authenticate(context.Background(), stub, "u1", "tok")

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeTooManyRequests, ec.Code)
	assert.Equal(t, "slow down", ec.Message)
}

func TestAuthenticate_NoUpstreamCallWhenCredentialsMissing(t *testing.T) {
	stub := &stubValidator{principal: principal.Principal{UserID: "u1"}}

	_, err := Authenticate(context.Background(), stub, "", "")

	require.Error(t, err)
	assert.Empty(t, stub.gotToken, "an empty credential must be rejected before the upstream call")
}

// TestValidator_Validate_CoalescesConcurrentCalls pins the stampede guard: N
// simultaneous requests for one token must cost botplatform a single validation.

// (or one concurrent write) away from corrupting unrelated requests.
func TestErrInvalidToken_NotShared(t *testing.T) {
	stub := &stubValidator{principal: principal.Principal{UserID: "u1"}}

	_, first := Authenticate(context.Background(), stub, "", "")
	_, second := Authenticate(context.Background(), stub, "", "")

	var a, b *errcode.Error
	require.ErrorAs(t, first, &a)
	require.ErrorAs(t, second, &b)
	assert.NotSame(t, a, b, "each rejection must get its own error value")

	// Mutating one must not reach the other.
	a.Message = "mutated"
	assert.Equal(t, "invalid session token", b.Message)
}
