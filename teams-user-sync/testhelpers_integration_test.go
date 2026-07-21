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

// setRunEnv wires the environment run() reads: the required Teams credentials
// (via setRequiredEnv), both Mongo URIs and DB name pointed at an isolated test
// database, and the Graph/token endpoint overrides.
func setRunEnv(t *testing.T, mongoURI, dbName, graphURL, tokenURL string) {
	t.Helper()
	setRequiredEnv(t)
	t.Setenv("MONGO_URI", mongoURI)
	t.Setenv("MONGO_DB", dbName)
	t.Setenv("GRAPH_BASE_URL", graphURL)
	t.Setenv("GRAPH_TOKEN_URL", tokenURL)
}
