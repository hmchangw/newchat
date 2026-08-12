//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestUserInfo_FailoverOverMongo(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// Short TTL so a later flip-back would be observable within the test.
	reader := newFailoverReader(store, 20*time.Millisecond)
	h := NewPortalHandler(cacheWith(alice), false, "site-local", "ws://localhost:9222",
		testSitesWithBackup, testSettings,
		WithFailoverReader(reader), WithBackupSiteID("_backup"))
	r := setupRouter(t, h)

	// Fail site-a over -> userInfo returns backup coords, home siteId.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))

	w := getUserInfo(t, r, "alice")
	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "https://backup.example.com", resp.BaseURL)
	assert.Equal(t, "site-a", resp.SiteID)
}
