package health

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeBody(t *testing.T, rr *httptest.ResponseRecorder) response {
	t.Helper()
	var body response
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	return body
}

func TestLivenessHandler_AlwaysOK(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	LivenessHandler()(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Equal(t, "ok", decodeBody(t, rr).Status)
}

func TestReadinessHandler_NoChecks_OK(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	ReadinessHandler(time.Second)(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "ok", decodeBody(t, rr).Status)
}

func TestReadinessHandler_AllChecksPass_OK(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	checks := []Check{
		{Name: "mongo", Probe: func(context.Context) error { return nil }},
		{Name: "nats", Probe: func(context.Context) error { return nil }},
	}
	ReadinessHandler(time.Second, checks...)(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	body := decodeBody(t, rr)
	assert.Equal(t, "ok", body.Status)
	assert.Equal(t, "ok", body.Checks["mongo"])
	assert.Equal(t, "ok", body.Checks["nats"])
}

func TestReadinessHandler_FailingCheck_503(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	checks := []Check{
		{Name: "mongo", Probe: func(context.Context) error { return nil }},
		{Name: "cassandra", Probe: func(context.Context) error { return errors.New("connection refused") }},
	}
	ReadinessHandler(time.Second, checks...)(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	body := decodeBody(t, rr)
	assert.Equal(t, "not ready", body.Status)
	assert.Equal(t, "ok", body.Checks["mongo"])
	assert.Contains(t, body.Checks["cassandra"], "connection refused")
}

func TestReadinessHandler_SlowCheckTimesOut_503(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	checks := []Check{
		{Name: "slow", Probe: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	ReadinessHandler(20*time.Millisecond, checks...)(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	body := decodeBody(t, rr)
	assert.Equal(t, "not ready", body.Status)
	assert.NotEqual(t, "ok", body.Checks["slow"])
}

// A probe that ignores context cancellation must not be able to hang the
// readiness response past the timeout.
func TestReadinessHandler_ProbeIgnoringContext_StillReturns(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	checks := []Check{
		{Name: "stuck", Probe: func(context.Context) error { <-release; return nil }},
	}

	done := make(chan struct{})
	go func() {
		ReadinessHandler(20*time.Millisecond, checks...)(rr, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("readiness handler hung on a context-ignoring probe")
	}
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestNewServer_ServesBothEndpoints(t *testing.T) {
	srv := NewServer(":0", time.Second,
		Check{Name: "dep", Probe: func(context.Context) error { return nil }},
	)
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err, "path %s", path)
		assert.Equal(t, http.StatusOK, resp.StatusCode, "path %s", path)
		require.NoError(t, resp.Body.Close())
	}
}

func TestNewServer_HardenedTimeouts(t *testing.T) {
	srv := NewServer(":8081", time.Second)

	assert.Equal(t, ":8081", srv.Addr)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Positive(t, srv.ReadTimeout)
	assert.Positive(t, srv.WriteTimeout)
}

func TestServeListener_ServesThenStops(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + l.Addr().String() + "/healthz"

	stop := ServeListener(l, time.Second,
		Check{Name: "dep", Probe: func(context.Context) error { return nil }},
	)

	resp, err := http.Get(url) // #nosec G107 -- test requests its own in-process httptest listener URL, not attacker-controlled
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	require.NoError(t, stop(context.Background()))

	// After shutdown the listener is closed, so the request must fail.
	resp, err = http.Get(url) // #nosec G107 -- test requests its own in-process httptest listener URL, not attacker-controlled
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	assert.Error(t, err)
}

func TestServe_BindsFreePortThenStops(t *testing.T) {
	stop, err := Serve("127.0.0.1:0", time.Second)
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
}

func TestServe_PortInUseReturnsError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	_, err = Serve(l.Addr().String(), time.Second)
	assert.Error(t, err)
}

func TestRegister_MountsOnExistingMux(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, time.Second)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}
