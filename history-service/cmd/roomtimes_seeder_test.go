package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingTier captures what the seeder stores.
type recordingTier struct {
	stored map[string]time.Time
}

func newRecordingTier() *recordingTier { return &recordingTier{stored: map[string]time.Time{}} }

func (r *recordingTier) Store(_ context.Context, roomID string, createdAt time.Time) {
	r.stored[roomID] = createdAt
}

func (r *recordingTier) Fallback(context.Context, string) (time.Time, bool) {
	return time.Time{}, false
}

func TestRoomTimesSeeder_StoresCreatedAtOnSuccess(t *testing.T) {
	created := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	repo := &stubRoomRepo{last: created.Add(time.Hour), created: created}
	tier := newRecordingTier()

	_, got, err := roomTimesSeeder{RoomRepository: repo, times: tier}.
		GetRoomTimes(context.Background(), "r1")

	require.NoError(t, err)
	assert.Equal(t, created, got)
	assert.Equal(t, created, tier.stored["r1"], "only the immutable time is cacheable")
}

// A failed read has no createdAt to cache, and storing a zero would hand a later
// degraded walk a floor of the epoch.
func TestRoomTimesSeeder_DoesNotStoreOnError(t *testing.T) {
	repo := &stubRoomRepo{err: errors.New("mongo down")}
	tier := newRecordingTier()

	_, _, err := roomTimesSeeder{RoomRepository: repo, times: tier}.
		GetRoomTimes(context.Background(), "r1")

	require.Error(t, err)
	assert.Empty(t, tier.stored)
}

// The batch path feeds the room list, which never walks Cassandra per room, so
// seeding from it would write one key per listed room for no reader.
func TestRoomTimesSeeder_BatchPathDoesNotSeed(t *testing.T) {
	repo := &stubRoomRepo{}
	tier := newRecordingTier()

	_, err := roomTimesSeeder{RoomRepository: repo, times: tier}.
		GetRoomTimesByIDs(context.Background(), []string{"r1", "r2"})

	require.NoError(t, err)
	assert.Equal(t, 1, repo.calls)
	assert.Empty(t, tier.stored)
}
