package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSoakCatalog_PublishDoesNotAdmitUntilGatekeeperAccepts(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	candidate := soakCatalogCandidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadReplyLimit: 50,
	}

	require.NoError(t, catalog.TrackPublished(&candidate))
	assert.Equal(t, 0, catalog.Size())
	_, ok := catalog.PickEligible("r-1", "alice", soakCatalogEdit)
	assert.False(t, ok)

	assert.True(t, catalog.Accept("r-1", "m-1"))
	assert.Equal(t, 1, catalog.Size())
	_, ok = catalog.PickEligible("r-1", "alice", soakCatalogEdit)
	assert.False(t, ok, "persist grace has not elapsed")

	clock.Advance(10 * time.Second)
	got, ok := catalog.PickEligible("r-1", "alice", soakCatalogEdit)
	require.True(t, ok)
	assert.Equal(t, candidate.ID, got.ID)
	assert.Equal(t, clock.Now().Add(-10*time.Second), got.AcceptedAt)
}

func TestSoakCatalog_RejectRemovesPendingPublish(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
	}))

	assert.True(t, catalog.Reject("r-1", "m-1"))
	assert.False(t, catalog.Accept("r-1", "m-1"))
	assert.Zero(t, catalog.Size())
}

func TestSoakCatalog_PendingPublishesStayWithinGlobalMemoryCap(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 1, 0, clock)
	for _, messageID := range []string{"oldest", "newest"} {
		require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
			ID: messageID, RoomID: "r-1", Author: "alice",
			CreatedAt: clock.Now(),
		}))
	}

	assert.False(t, catalog.Accept("r-1", "oldest"))
	assert.True(t, catalog.Accept("r-1", "newest"))
	assert.Equal(t, 1, catalog.Size())
}

func TestSoakCatalog_EditAndDeleteAreAuthorOnly(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	_, ok := catalog.PickEligible("r-1", "bob", soakCatalogEdit)
	assert.False(t, ok)
	_, ok = catalog.PickEligible("r-1", "bob", soakCatalogDelete)
	assert.False(t, ok)
	_, ok = catalog.PickEligible("r-1", "alice", soakCatalogEdit)
	assert.True(t, ok)
	_, ok = catalog.PickEligible("r-1", "bob", soakCatalogPin)
	assert.True(t, ok, "pin is not author-only")
}

func TestSoakCatalog_StateTransitions(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	assert.True(t, catalog.MarkEdited("r-1", "m-1", "edited"))
	assert.True(t, catalog.SetPinned("r-1", "m-1", true))
	assert.True(t, catalog.SetReaction("r-1", "m-1", "party", "bob", true))
	assert.True(t, catalog.IncrementThreadReplies("r-1", "m-1"))

	got, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, got.Edited)
	assert.Equal(t, "edited", got.Content)
	assert.True(t, got.Pinned)
	assert.Equal(t, map[string][]string{"party": {"bob"}}, got.Reactions)
	assert.Equal(t, 1, got.ThreadReplies)

	assert.True(t, catalog.SetReaction("r-1", "m-1", "party", "bob", false))
	assert.True(t, catalog.SetPinned("r-1", "m-1", false))
	assert.True(t, catalog.MarkDeleted("r-1", "m-1"))
	got, ok = catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, got.Deleted)
	assert.False(t, got.Pinned)
	assert.Empty(t, got.Reactions)
	_, ok = catalog.PickEligible("r-1", "alice", soakCatalogThreadParent)
	assert.False(t, ok)
}

func TestSoakCatalog_ThreadReplyBudget(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
		ThreadReplyLimit: 2,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))

	assert.True(t, catalog.IncrementThreadReplies("r-1", "m-1"))
	assert.True(t, catalog.IncrementThreadReplies("r-1", "m-1"))
	assert.False(t, catalog.IncrementThreadReplies("r-1", "m-1"))
	_, ok := catalog.PickEligible("r-1", "alice", soakCatalogThreadParent)
	assert.False(t, ok)
}

func TestSoakCatalog_PerRoomAndGlobalEviction(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))

	perRoom := newSoakCatalog(2, 10, 0, clock)
	acceptCatalogIDs(t, perRoom, clock, "r-1", "m-1", "m-2", "m-3")
	assert.Equal(t, 2, perRoom.Size())
	_, ok := perRoom.Get("r-1", "m-1")
	assert.False(t, ok)

	global := newSoakCatalog(3, 3, 0, clock)
	acceptCatalogIDs(t, global, clock, "r-1", "m-1", "m-2")
	acceptCatalogIDs(t, global, clock, "r-2", "m-3", "m-4")
	assert.Equal(t, 3, global.Size())
	_, ok = global.Get("r-1", "m-1")
	assert.False(t, ok)
}

func TestSoakCatalog_EvictionRetainsPinnedMessages(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(2, 2, 0, clock)
	acceptCatalogIDs(t, catalog, clock, "r-1", "m-1", "m-2")
	require.True(t, catalog.SetPinned("r-1", "m-1", true))

	acceptCatalogIDs(t, catalog, clock, "r-1", "m-3")

	pinned, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, pinned.Pinned)
	assert.Equal(t, 1, catalog.PinnedCount("r-1"))
	assert.LessOrEqual(t, catalog.Size(), 2)
}

func TestSoakCatalog_AllPinnedEntriesStillRespectHardBounds(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(2, 2, 0, clock)
	for _, messageID := range []string{"m-1", "m-2", "m-3"} {
		assert.True(t, catalog.ObservePinned(&soakWireMessage{
			RoomID: "r-1", MessageID: messageID,
			Sender:    cassandra.Participant{Account: "alice"},
			CreatedAt: clock.Now(),
		}))
		clock.Advance(time.Millisecond)
	}

	assert.Equal(t, 2, catalog.Size())
	_, oldestExists := catalog.Get("r-1", "m-1")
	assert.False(t, oldestExists)
	assert.Equal(t, 2, catalog.PinnedCount("r-1"))
}

func TestSoakCatalog_AllPinnedRoomsRespectGlobalHardBound(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(4, 2, 0, clock)
	for index, roomID := range []string{"r-1", "r-2", "r-3"} {
		assert.True(t, catalog.ObservePinned(&soakWireMessage{
			RoomID: roomID, MessageID: fmt.Sprintf("m-%d", index),
			Sender:    cassandra.Participant{Account: "alice"},
			CreatedAt: clock.Now(),
		}))
		clock.Advance(time.Millisecond)
	}

	assert.Equal(t, 2, catalog.Size())
	_, oldestExists := catalog.Get("r-1", "m-0")
	assert.False(t, oldestExists)
}

func TestSoakCatalog_HistoryVerificationExcludesThreadReplies(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "top-level", RoomID: "r-1", Author: "alice",
		CreatedAt: clock.Now(),
	}))
	require.True(t, catalog.Accept("r-1", "top-level"))
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "thread-reply", RoomID: "r-1", Author: "alice",
		CreatedAt: clock.Now(), ThreadParentID: "top-level",
	}))
	require.True(t, catalog.Accept("r-1", "thread-reply"))

	candidate, ok := catalog.PickHistoryVerificationCandidate("r-1")
	require.True(t, ok)
	assert.Equal(t, "top-level", candidate.ID)
}

func TestSoakCatalog_ObservePinnedValidatesAndReconcilesExistingMessage(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	assert.False(t, catalog.ObservePinned(nil))
	assert.False(t, catalog.ObservePinned(&soakWireMessage{
		RoomID: "r-1", MessageID: "invalid",
	}))
	before, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.False(t, before.Pinned)

	assert.True(t, catalog.ObservePinned(&soakWireMessage{
		RoomID: "r-1", MessageID: "m-1",
		Sender: cassandra.Participant{Account: "alice"},
	}))

	message, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, message.Pinned)
	assert.Equal(t, 1, catalog.Size(), "warmup updates instead of duplicating")
}

func TestSoakCatalog_ConcurrentAccessStaysBounded(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 64, 0, clock)

	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			roomID := fmt.Sprintf("r-%02d", worker%4)
			for i := range 50 {
				messageID := fmt.Sprintf("m-%02d-%03d", worker, i)
				err := catalog.TrackPublished(&soakCatalogCandidate{
					ID: messageID, RoomID: roomID, Author: "alice",
					Content: messageID, CreatedAt: clock.Now(), ThreadReplyLimit: 50,
				})
				if err == nil && catalog.Accept(roomID, messageID) {
					catalog.SetPinned(roomID, messageID, i%2 == 0)
					catalog.SetReaction(roomID, messageID, "party", "bob", true)
					catalog.Get(roomID, messageID)
				}
			}
		}(worker)
	}
	wg.Wait()

	assert.LessOrEqual(t, catalog.Size(), 32, "four rooms times per-room cap")
	assert.LessOrEqual(t, catalog.Size(), 64)
}

func acceptedCatalogMessage(t *testing.T, clock *fakeSoakClock, grace time.Duration) *soakCatalog {
	t.Helper()
	catalog := newSoakCatalog(8, 100, grace, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadReplyLimit: 50,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))
	return catalog
}

func acceptCatalogIDs(
	t *testing.T,
	catalog *soakCatalog,
	clock *fakeSoakClock,
	roomID string,
	messageIDs ...string,
) {
	t.Helper()
	for _, messageID := range messageIDs {
		require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
			ID: messageID, RoomID: roomID, Author: "alice",
			CreatedAt: clock.Now(), ThreadReplyLimit: 50,
		}))
		require.True(t, catalog.Accept(roomID, messageID))
		clock.Advance(time.Millisecond)
	}
}

type fakeSoakClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeSoakClock(now time.Time) *fakeSoakClock {
	return &fakeSoakClock{now: now}
}

func (c *fakeSoakClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSoakClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
