package botauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// decodeJSON reads a request body into out.
func decodeJSON(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

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

func TestValidator_Validate(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantPrincipal principal.Principal
		wantCode      errcode.Code
		wantReason    errcode.Reason
		wantRawErr    bool
	}{
		{
			name:   "valid session",
			status: http.StatusOK,
			body: `{"valid":true,"principal":{"userId":"u1","account":"alerts.sa.bot",` +
				`"siteId":"site-a","roles":["bot"]}}`,
			wantPrincipal: principal.Principal{
				UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
			},
		},
		{
			name:       "200 with valid false",
			status:     http.StatusOK,
			body:       `{"valid":false}`,
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name:       "401 unknown session",
			status:     http.StatusUnauthorized,
			body:       `{"code":"unauthenticated","reason":"invalid_token"}`,
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{name: "500 upstream fault is a raw error", status: http.StatusInternalServerError, body: `{"code":"internal"}`, wantRawErr: true},
		{name: "400 upstream rejection is a raw error", status: http.StatusBadRequest, body: `{"code":"bad_request"}`, wantRawErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotRequestID string
			var gotBody map[string]string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotRequestID = r.Header.Get(natsutil.RequestIDHeader)
				_ = decodeJSON(r, &gotBody)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			v := NewValidator(restyutil.New(""), srv.URL)
			ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
			got, err := v.Validate(ctx, "raw-token")

			assert.Equal(t, "/api/v1/auth/validate", gotPath)
			assert.Equal(t, "raw-token", gotBody["authToken"])
			assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", gotRequestID)

			var ec *errcode.Error
			switch {
			case tt.wantRawErr:
				require.Error(t, err)
				assert.False(t, errors.As(err, &ec), "upstream faults must stay raw so the caller maps them to 503")
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

func TestValidator_Validate_TransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // closed: the POST cannot connect

	v := NewValidator(restyutil.New(""), srv.URL)
	_, err := v.Validate(context.Background(), "raw-token")

	require.Error(t, err)
	var ec *errcode.Error
	assert.False(t, errors.As(err, &ec), "transport failures must stay raw")
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

func TestValidator_Validate_ValidTrueWithoutPrincipal(t *testing.T) {
	// A 200 {"valid":true} with no principal must fail closed — an empty identity
	// returned as success is a bypass for direct Validate callers.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true}`))
	}))
	defer srv.Close()

	_, err := NewValidator(restyutil.New(""), srv.URL).Validate(context.Background(), "tok")

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnauthenticated, ec.Code)
	assert.Equal(t, errcode.BotplatformInvalidToken, ec.Reason)
}

func TestAuthenticate_NoUpstreamCallWhenCredentialsMissing(t *testing.T) {
	stub := &stubValidator{principal: principal.Principal{UserID: "u1"}}

	_, err := Authenticate(context.Background(), stub, "", "")

	require.Error(t, err)
	assert.Empty(t, stub.gotToken, "an empty credential must be rejected before the upstream call")
}

// TestValidator_Validate_CoalescesConcurrentCalls pins the stampede guard: N
// simultaneous requests for one token must cost botplatform a single validation.
func TestValidator_Validate_CoalescesConcurrentCalls(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		<-release // hold the request open so the others pile up behind it
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true,"principal":{"userId":"u1","account":"a.bot"}}`))
	}))
	defer srv.Close()

	v := NewValidator(restyutil.New(""), srv.URL)
	const n = 20
	var wg, entered sync.WaitGroup
	results := make([]principal.Principal, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		entered.Add(1)
		go func() {
			defer wg.Done()
			entered.Done() // struck immediately before Validate, so the window below is a few instructions
			results[i], errs[i] = v.Validate(context.Background(), "same-token")
		}()
	}
	// Every goroutine must be at the shared call before the server responds:
	// singleflight forgets the key on completion, so a straggler arriving after
	// the release would start a second upstream call and fail the count below.
	entered.Wait()
	assert.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int64(1), calls.Load(), "N concurrent validations must collapse to one upstream call")
	for i := range n {
		require.NoError(t, errs[i])
		assert.Equal(t, "a.bot", results[i].Account)
	}
}

// TestValidator_Validate_CallerCancellationDoesNotPoisonOthers proves the shared
// call is detached: one caller giving up must not fail the rest.
func TestValidator_Validate_CallerCancellationDoesNotPoisonOthers(t *testing.T) {
	proceed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-proceed
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true,"principal":{"userId":"u1","account":"a.bot"}}`))
	}))
	defer srv.Close()

	v := NewValidator(restyutil.New(""), srv.URL)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)

	var firstErr, secondErr error
	var second principal.Principal
	firstDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(firstDone)
		_, firstErr = v.Validate(firstCtx, "tok")
	}()
	go func() { defer wg.Done(); second, secondErr = v.Validate(context.Background(), "tok") }()

	// Sequence rather than race: while proceed is held the shared call cannot
	// resolve, so the cancelled caller has only one way out of its select.
	cancelFirst()
	<-firstDone
	close(proceed)
	wg.Wait()

	assert.Error(t, firstErr, "the cancelled caller gets its own context error")
	require.NoError(t, secondErr, "the surviving caller must still be served")
	assert.Equal(t, "a.bot", second.Account)
}

// TestErrInvalidToken_NotShared guards against handing every caller the same
// *errcode.Error: its fields are exported, so a shared pointer is one mutation
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

// panicTransport panics inside the singleflight closure, where resty runs.
type panicTransport struct{}

func (panicTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("boom")
}

// TestValidator_Validate_ContainsPanic pins the recover. singleflight re-raises a
// panic from its own goroutine (`go panic(e)` when waiters exist, which DoChan
// always creates), out of reach of gin.Recovery — so without the recover this
// does not fail the request, it takes the process down with it.
func TestValidator_Validate_ContainsPanic(t *testing.T) {
	v := NewValidator(restyutil.New("", restyutil.WithTransport(panicTransport{})), "http://example")

	_, err := v.Validate(context.Background(), "tok")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
	var ec *errcode.Error
	assert.False(t, errors.As(err, &ec), "a panic is infra failure: raw, so it collapses to 503 at the boundary")

	// The slot must come back, or one panic would permanently shrink the pool.
	_, err = v.Validate(context.Background(), "tok2")
	require.Error(t, err)
	assert.NotErrorIs(t, err, errShedded)
}

// TestValidator_Validate_ShedsWhenSaturated pins the ceiling: the singleflight key
// is a caller-supplied token, so unique tokens each start their own detached call.
// Past maxInFlight the validator must shed with 503 rather than accumulate work.
func TestValidator_Validate_ShedsWhenSaturated(t *testing.T) {
	release := make(chan struct{})
	var active atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		active.Add(1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true,"principal":{"userId":"u1","account":"a.bot"}}`))
	}))
	defer srv.Close()

	v := NewValidator(restyutil.New(""), srv.URL)

	// Declared after srv.Close's defer, so LIFO runs it first: a failed require
	// below Goexits, and without this the 64 parked handlers would hang Close for
	// the package timeout, burying the assertion under a timeout panic.
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	// Fill the pool with distinct tokens so nothing coalesces.
	var wg sync.WaitGroup
	for i := range maxInFlight {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = v.Validate(context.Background(), fmt.Sprintf("tok-%d", i))
		}()
	}
	assert.Eventually(t, func() bool { return active.Load() == int64(maxInFlight) },
		2*time.Second, time.Millisecond)

	// One more distinct token must be shed immediately, not queued.
	_, err := v.Validate(context.Background(), "one-too-many")
	require.ErrorIs(t, err, errShedded)

	// Shedding is capacity state, not a wire contract: through Authenticate it
	// must reach the client as the documented "botplatform unavailable" 503,
	// indistinguishable from a botplatform that could not be reached at all.
	_, err = Authenticate(context.Background(), v, "u1", "one-too-many-2")

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.BotplatformUpstreamUnavailable, ec.Reason)
	assert.Equal(t, "botplatform unavailable", ec.Message)

	unblock()
	wg.Wait()
}
