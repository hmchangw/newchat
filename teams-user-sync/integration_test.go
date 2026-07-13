//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestUpdateUsers_EndToEnd drives the full pipeline: a fake two-page Graph
// tenant against a real Mongo (one database standing in for both the read
// and write clients), asserting the merged writes and idempotent rerun.
func TestUpdateUsers_EndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync_e2e")
	ctx := context.Background()

	_, err := db.Collection("hr").InsertMany(ctx, []any{
		bson.M{"accountName": "alice", "siteID": "site-a"},
		bson.M{"accountName": "old", "siteID": "site-a"},
	})
	require.NoError(t, err)
	_, err = db.Collection("teams_user").InsertOne(ctx,
		bson.M{"_id": "id-existing", "upn": "old@corp.example", "account": "old", "siteId": "site-a"})
	require.NoError(t, err)

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]string{
				{"id": "id-existing", "userPrincipalName": "old@corp.example"},
				{"id": "id-guest", "userPrincipalName": "guest#EXT#@other.example"},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []map[string]string{
				{"id": "id-alice", "userPrincipalName": "Alice@corp.example"},
				{"id": "id-carol", "userPrincipalName": "carol@corp.example"},
			},
			"@odata.nextLink": graphSrv.URL + "/users?page=2",
		})
	}))
	t.Cleanup(graphSrv.Close)

	lister := msgraph.NewUserListerClient(
		msgraph.Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		msgraph.WithBaseURL(graphSrv.URL), msgraph.WithTokenURL(tokenSrv.URL),
	)
	syncer := NewSyncer(newMongoStore(db, db), lister, "corp.example", 500)

	stats, err := syncer.UpdateUsers(ctx)
	require.NoError(t, err)
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 1, DomainSkipped: 1, HRUnmatched: 1, Upserted: 1,
	}, stats)

	var doc model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "id-alice"}).Decode(&doc))
	assert.Equal(t, model.TeamsUser{
		ID: "id-alice", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a",
	}, doc)

	// rerun: everything either exists, is domain-skipped, or is still HR-unmatched
	stats2, err := syncer.UpdateUsers(ctx)
	require.NoError(t, err)
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 2, DomainSkipped: 1, HRUnmatched: 1, Upserted: 0,
	}, stats2)

	n, err := db.Collection("teams_user").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}
