//go:build integration

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// newFakeTokenServer returns an httptest server that answers every request with
// a static OAuth2 token, mimicking the Azure token endpoint. Registered for
// cleanup on t.
func newFakeTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Write error is irrelevant for a test fixture: a client disconnect
		// surfaces as the run's own request error, which the test asserts on.
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}
