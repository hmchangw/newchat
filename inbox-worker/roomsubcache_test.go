package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// countingStore counts HasRoomSubscription reads so the tests can assert the
// cache actually removes them.
type countingStore struct {
	*stubInboxStore
	reads int
}

func (c *countingStore) HasRoomSubscription(ctx context.Context, roomID string) (bool, error) {
	c.reads++
	return c.stubInboxStore.HasRoomSubscription(ctx, roomID)
}

func activityData(t *testing.T, roomID string) []byte {
	t.Helper()
	b, err := json.Marshal(model.RoomActivityEvent{
		RoomID: roomID, SiteID: "site-A", LastMsgAt: 1740000000000, Timestamp: 1740000000000,
	})
	require.NoError(t, err)
	return b
}

func TestRoomSubCache_SecondRefreshServesFromMemory(t *testing.T) {
	store := &countingStore{stubInboxStore: &stubInboxStore{roomSubscribed: map[string]bool{"r1": true}}}
	h := NewHandler(store, WithRoomSubCache(100, time.Minute))

	for range 5 {
		require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	}
	assert.Equal(t, 1, store.reads, "only the first refresh reads through")
	assert.Len(t, store.remoteRoomActivity, 5, "every refresh still applies")
}

func TestRoomSubCache_NegativeEntriesAreCachedToo(t *testing.T) {
	// Rooms this site has no member in are the common case and the whole point:
	// they must not cost a read per refresh.
	store := &countingStore{stubInboxStore: &stubInboxStore{}}
	h := NewHandler(store, WithRoomSubCache(100, time.Minute))

	for range 5 {
		require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r-elsewhere")))
	}
	assert.Equal(t, 1, store.reads)
	assert.Empty(t, store.remoteRoomActivity)
}

func TestRoomSubCache_MemberAddedPromotesWithoutARead(t *testing.T) {
	// A room cached negative must not stay invisible for the TTL once this site
	// joins it — member_added updates the entry in the same process.
	store := &countingStore{stubInboxStore: &stubInboxStore{users: []model.User{{ID: "u_bob", Account: "bob", SiteID: "site-B"}}}}
	h := NewHandler(store, WithRoomSubCache(100, time.Minute))

	require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	require.Empty(t, store.remoteRoomActivity, "not a member yet")

	at := int64(1740000000000)
	require.NoError(t, h.HandleEvent(context.Background(), memberAddedEventData(t, &at)))

	store.reads = 0
	require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	assert.Equal(t, 0, store.reads, "member_added already promoted the entry")
	assert.NotEmpty(t, store.remoteRoomActivity, "and the refresh now applies")
}

func TestRoomSubCache_MemberRemovedForcesAReadThrough(t *testing.T) {
	// Removal does not prove the site has no members left, so drop the entry and
	// let the next refresh re-resolve it rather than caching a guess.
	store := &countingStore{stubInboxStore: &stubInboxStore{roomSubscribed: map[string]bool{"r1": true}}}
	h := NewHandler(store, WithRoomSubCache(100, time.Minute))

	require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	require.Equal(t, 1, store.reads)

	evt, err := json.Marshal(model.InboxEvent{Type: "member_removed", Payload: mustJSON(t, model.MemberRemoveEvent{
		Type: "member_removed", RoomID: "r1", Accounts: []string{"bob"},
	})})
	require.NoError(t, err)
	require.NoError(t, h.HandleEvent(context.Background(), evt))

	require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	assert.Equal(t, 2, store.reads, "the invalidated room is re-resolved")
}

func TestRoomSubCache_DisabledAlwaysReadsThrough(t *testing.T) {
	store := &countingStore{stubInboxStore: &stubInboxStore{roomSubscribed: map[string]bool{"r1": true}}}
	h := NewHandler(store) // no cache option

	for range 3 {
		require.NoError(t, h.HandleRoomActivity(context.Background(), activityData(t, "r1")))
	}
	assert.Equal(t, 3, store.reads)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
