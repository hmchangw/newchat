//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRun_OneShotEndToEnd drives the run-once binary path: env pointed at the
// shared test Mongo and a fake Graph, a single run() invocation syncs the
// tenant and returns nil (exit 0 for the Kubernetes Job).
func TestRun_OneShotEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync_run")
	ctx := context.Background()

	_, err := db.Collection("hr").InsertOne(ctx, bson.M{
		"accountName": "alice", "locationURL": "https://site-a.mysite.com",
		"engName": "Alice Smith", "mail": "alice@corp.example",
	})
	require.NoError(t, err)

	tokenSrv := newFakeTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"value":[{"id":"id-alice","userPrincipalName":"Alice@corp.example"}]}`))
	}))
	t.Cleanup(graphSrv.Close)

	setRunEnv(t, testutil.MongoURI(t), db.Name(), graphSrv.URL, tokenSrv.URL)

	require.NoError(t, run())

	var doc model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "id-alice"}).Decode(&doc))
	assert.Equal(t, model.TeamsUser{
		ID: "id-alice", UPN: "Alice@corp.example", Account: "alice",
		SiteID: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example",
	}, doc)
}

// TestRun_GraphFailureReturnsError verifies a failed sync surfaces as a
// non-nil error (exit 1) so the Kubernetes Job records the failure.
func TestRun_GraphFailureReturnsError(t *testing.T) {
	tokenSrv := newFakeTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(graphSrv.Close)

	// Per-test isolated database even though the run fails before writing —
	// no test may point at the shared default DB name.
	db := testutil.MongoDB(t, "teams_user_sync_err")
	setRunEnv(t, testutil.MongoURI(t), db.Name(), graphSrv.URL, tokenSrv.URL)

	err := run()
	require.ErrorContains(t, err, "update users")
}
