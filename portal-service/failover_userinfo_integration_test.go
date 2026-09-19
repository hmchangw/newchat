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

	// Drive the reader's clock rather than sleeping, so the TTL lapse is
	// deterministic. Every request is served on this goroutine, so a plain
	// variable is race-free here.
	clock := time.UnixMilli(1000)
	reader := newFailoverReader(store, 5*time.Second)
	reader.now = func() time.Time { return clock }
	h := NewPortalHandler(cacheWith(alice), false, "site-local", "ws://localhost:9222",
		testSitesWithBackup, testSettings,
		WithFailoverReader(reader), WithBackupSiteID("_backup"))
	r := setupRouter(t, h)

	// Fail site-a over -> userInfo returns backup coords, home siteId.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))

	failedOver := getUserInfo(t, r, "alice")
	require.Equal(t, http.StatusOK, failedOver.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(failedOver.Body.Bytes(), &resp))
	assert.Equal(t, "https://backup.example.com", resp.BaseURL)
	assert.Equal(t, "wss://nats.backup.example.com", resp.NATSURL)
	assert.Equal(t, "site-a", resp.SiteID)

	// Resume site-a -> once the reader's TTL lapses, routing returns home. The
	// reader unit tests cover TTL refresh directly; this covers the same flip
	// end-to-end through PortalHandler, which those cannot see.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 2, Since: 2, Timestamp: 2}))
	clock = clock.Add(6 * time.Second) // past the reader TTL

	healthy := getUserInfo(t, r, "alice")
	require.Equal(t, http.StatusOK, healthy.Code)
	var back userInfoResponse
	require.NoError(t, json.Unmarshal(healthy.Body.Bytes(), &back))
	assert.Equal(t, "https://site-a.example.com", back.BaseURL, "baseUrl returns home")
	assert.Equal(t, "site-a", back.SiteID)
}
