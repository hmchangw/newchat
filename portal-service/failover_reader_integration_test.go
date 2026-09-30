//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestFailoverReader_OverMongo(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	r := newFailoverReader(store, time.Minute)

	// No document yet => healthy => home.
	assert.Equal(t, ServingHome, r.ServingTarget(ctx, "site-a"))

	// Fail the site over; a fresh reader (empty cache) sees backup.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	r2 := newFailoverReader(store, time.Minute)
	assert.Equal(t, ServingBackup, r2.ServingTarget(ctx, "site-a"))
}
