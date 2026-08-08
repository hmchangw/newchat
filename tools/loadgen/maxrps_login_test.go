package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: loginWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*loginWorkload)(nil)

// The status→eligibility table itself is covered by TestClassifyHTTPStatus in
// slooutcome_test.go, now that login shares that classifier with every other
// SLO-scoring workload.

func TestBuildLoginInputs(t *testing.T) {
	c := newLoginCollector()
	for range 90 {
		c.Record(outcomeGood, 50*time.Millisecond)
	}
	for range 10 {
		c.Record(outcomeFailed, 0)
	}
	for range 25 {
		c.Record(outcomeExcluded, 0)
	}

	in := buildLoginInputs(200, 10*time.Second, c)

	// Excluded events leave the denominator entirely: 90 good + 10 failed.
	assert.Equal(t, 100, in.AttemptedOps)
	assert.Equal(t, 10, in.FailedOps)
	require.Len(t, in.Latencies, 1)
	assert.Equal(t, "login", in.Latencies[0].Name)
	assert.Len(t, in.Latencies[0].Samples, 90, "only successful logins are timed")
	// Login is a synchronous HTTP call with no JetStream consumer behind it.
	assert.Empty(t, in.Pending)
}

// A failed login has no meaningful latency, so timing it would let failures
// drag the percentile in whichever direction they happen to land.
func TestLoginCollector_OnlyTimesSuccesses(t *testing.T) {
	c := newLoginCollector()
	c.Record(outcomeGood, 20*time.Millisecond)
	c.Record(outcomeFailed, 900*time.Millisecond)
	c.Record(outcomeExcluded, 900*time.Millisecond)

	assert.Equal(t, []time.Duration{20 * time.Millisecond}, c.Samples())
	assert.Equal(t, 1, c.Good())
	assert.Equal(t, 1, c.Failed())
	assert.Equal(t, 1, c.Excluded())
}

// A full in-flight pool is a load-box limit, so it must reach Saturation
// (which drives INCONCLUSIVE) and never FailedOps (which drives TRIP).
func TestBuildLoginInputs_SaturationIsNotFailure(t *testing.T) {
	c := newLoginCollector()
	c.Record(outcomeGood, 10*time.Millisecond)
	c.RecordSaturation()
	c.RecordSaturation()

	in := buildLoginInputs(100, time.Second, c)

	assert.Equal(t, 2, in.Saturation)
	assert.Equal(t, 0, in.FailedOps)
	assert.Equal(t, 1, in.AttemptedOps)
}

// Events the pacer could not release on schedule are a load-box limit. They
// must reach EmitUnderrun (which explains an INCONCLUSIVE) and never FailedOps.
func TestBuildLoginInputs_UnderrunIsNotFailure(t *testing.T) {
	c := newLoginCollector()
	c.Record(outcomeGood, 10*time.Millisecond)
	c.RecordUnderrun(17)

	in := buildLoginInputs(100, time.Second, c)

	assert.Equal(t, 17, in.EmitUnderrun)
	assert.Equal(t, 0, in.FailedOps)
}

func TestDefaultSteps_LoginRampsLowerThanMessages(t *testing.T) {
	// Login happens once per session, not per message; ramping it like the
	// message path would just measure JWT signing throughput.
	assert.Equal(t, "50,100,200,500,1000", defaultSteps("login"))
	assert.Equal(t, "500,1000,2000,5000,10000", defaultSteps("messages"))
}

func TestNewLoginWorkload_RequiresAuthURL(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	_, _, err := newLoginWorkload(&config{SiteID: "site-local"}, &p, 42, "", time.Second, 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth-url")
}

func TestLoginKeyPool_ReusesKeysAndYieldsValidNKeys(t *testing.T) {
	pool, err := newLoginKeyPool(4)
	require.NoError(t, err)

	seen := map[string]int{}
	for i := range 12 {
		pk := pool.next(i)
		require.NotEmpty(t, pk)
		assert.Equal(t, byte('U'), pk[0], "must be a NATS user public key")
		seen[pk]++
	}
	assert.Len(t, seen, 4, "the pool is reused rather than regenerating per request")
}

func TestLoginKeyPool_RejectsEmptyPool(t *testing.T) {
	_, err := newLoginKeyPool(0)
	require.Error(t, err)
}

func TestLoginRequester_PostsDevAuthShape(t *testing.T) {
	var gotPath, gotAccount, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		var body struct {
			Account       string `json:"account"`
			NATSPublicKey string `json:"natsPublicKey"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		gotAccount, gotKey = body.Account, body.NATSPublicKey
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newLoginRequester(srv.URL, 5*time.Second)
	outcome, _ := r.login(t.Context(), "alice", "UABC")

	assert.Equal(t, outcomeGood, outcome)
	assert.Equal(t, "/api/v1/auth", gotPath)
	assert.Equal(t, "alice", gotAccount)
	assert.Equal(t, "UABC", gotKey)
}

func TestLoginRequester_ClassifiesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := newLoginRequester(srv.URL, 5*time.Second)
	outcome, _ := r.login(t.Context(), "alice", "UABC")
	assert.Equal(t, outcomeFailed, outcome)
}

// pacedDispatch cancels the run context at the step deadline and hands that
// same context to every in-flight request. Those cancellations are the harness
// closing the window, not the service failing: counting them would put up to
// MaxInFlight failures into every step and trip the error-rate SLO on a healthy
// service.
func TestLoginRequester_StepDeadlineCancellationIsExcluded(t *testing.T) {
	// The handler returns on its own rather than blocking on the request
	// context, so srv.Close() cannot deadlock waiting for it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Cancelled well before the client's own timeout would fire, so the error
	// can only come from the caller's context.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	r := newLoginRequester(srv.URL, 30*time.Second)
	outcome, _ := r.login(ctx, "alice", "UABC")
	assert.Equal(t, outcomeExcluded, outcome, "a cancelled window edge is not a service failure")
}

// A timeout of the request itself is still ours — the service did not answer
// within the budget, which is exactly what the SLO is meant to catch.
func TestLoginRequester_RequestTimeoutIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newLoginRequester(srv.URL, 20*time.Millisecond)
	outcome, _ := r.login(context.Background(), "alice", "UABC")
	assert.Equal(t, outcomeFailed, outcome)
}

func TestLoginRequester_UnreachableHostIsFailure(t *testing.T) {
	r := newLoginRequester("http://127.0.0.1:1", 200*time.Millisecond)
	outcome, _ := r.login(t.Context(), "alice", "UABC")
	assert.Equal(t, outcomeFailed, outcome)
}
