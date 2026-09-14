package catalog

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

func TestSoakCatalog_PublishDoesNotAdmitUntilGatekeeperAccepts(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 10*time.Second, clock)
	candidate := Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadReplyLimit: 50,
	}

	require.NoError(t, catalog.TrackPublished(&candidate))
	assert.Equal(t, 0, catalog.Size())
	_, ok := catalog.PickEligible("r-1", "alice", ActionEdit)
	assert.False(t, ok)

	assert.True(t, catalog.Accept("r-1", "m-1"))
	assert.Equal(t, 1, catalog.Size())
	_, ok = catalog.PickEligible("r-1", "alice", ActionEdit)
	assert.False(t, ok, "persist grace has not elapsed")

	clock.Advance(10 * time.Second)
	got, ok := catalog.PickEligible("r-1", "alice", ActionEdit)
	require.True(t, ok)
	assert.Equal(t, candidate.ID, got.ID)
	assert.Equal(t, clock.Now().Add(-10*time.Second), got.AcceptedAt)
}

func TestSoakCatalog_ThreadRecipientSetSurvivesReplyEviction(t *testing.T) {
	catalog := New(2, 100, 0, nil)
	// #nosec G601 -- go.mod requires go 1.25; since 1.22 each iteration has its own loop variable
	// nosemgrep: gosec.G601-1
	for _, candidate := range []Candidate{
		{ID: "parent", RoomID: "room-1", Author: "alice", Content: "parent"},
		{ID: "reply", RoomID: "room-1", Author: "bob", Content: "reply", ThreadParentID: "parent"},
	} {
		require.NoError(t, catalog.TrackPublished(&candidate))
		require.True(t, catalog.Accept(candidate.RoomID, candidate.ID))
	}
	require.True(t, catalog.SetPinned("room-1", "parent", true))
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "other", RoomID: "room-1", Author: "dave", Content: "other",
	}))
	require.True(t, catalog.Accept("room-1", "other"))

	recipients, complete := catalog.ThreadRecipientSet("room-1", "parent", "carol")

	assert.Equal(t, []string{"alice", "bob", "carol"}, recipients)
	assert.True(t, complete)
}

func TestSoakCatalog_ExternallyObservedParentHasIncompleteFollowerSet(t *testing.T) {
	catalog := New(8, 100, 0, nil)
	require.True(t, catalog.ObservePinned(&wire.Message{
		RoomID: "room-1", MessageID: "parent",
		Sender: cassandra.Participant{Account: "alice"},
	}))

	recipients, complete := catalog.ThreadRecipientSet("room-1", "parent", "carol")

	assert.Equal(t, []string{"alice", "carol"}, recipients)
	assert.False(t, complete)
}

func TestSoakCatalog_RejectRemovesPendingPublish(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
	}))

	assert.True(t, catalog.Reject("r-1", "m-1"))
	assert.False(t, catalog.Accept("r-1", "m-1"))
	assert.Zero(t, catalog.Size())
}

func TestSoakCatalog_PendingPublishesStayWithinGlobalMemoryCap(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 1, 0, clock)
	for _, messageID := range []string{"oldest", "newest"} {
		require.NoError(t, catalog.TrackPublished(&Candidate{
			ID: messageID, RoomID: "r-1", Author: "alice",
			CreatedAt: clock.Now(),
		}))
	}

	assert.False(t, catalog.Accept("r-1", "oldest"))
	assert.True(t, catalog.Accept("r-1", "newest"))
	assert.Equal(t, 1, catalog.Size())
}

func TestSoakCatalog_EditAndDeleteAreAuthorOnly(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	_, ok := catalog.PickEligible("r-1", "bob", ActionEdit)
	assert.False(t, ok)
	_, ok = catalog.PickEligible("r-1", "bob", ActionDelete)
	assert.False(t, ok)
	_, ok = catalog.PickEligible("r-1", "alice", ActionEdit)
	assert.True(t, ok)
	_, ok = catalog.PickEligible("r-1", "bob", ActionPin)
	assert.True(t, ok, "pin is not author-only")
}

func TestSoakCatalog_StateTransitions(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	assert.True(t, catalog.MarkEdited("r-1", "m-1", "edited"))
	assert.True(t, catalog.SetPinned("r-1", "m-1", true))
	assert.True(t, catalog.SetReaction("r-1", "m-1", "party", "bob", true))
	assert.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	assert.True(t, catalog.ConfirmThreadReply("r-1", "m-1"))

	got, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, got.Edited)
	assert.Equal(t, ContentDigest("edited"), got.ContentSHA256)
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
	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadParent)
	assert.False(t, ok)
}

func TestSoakCatalog_ThreadReplyBudget(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
		ThreadReplyLimit: 2,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))

	assert.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	assert.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	assert.False(t, catalog.ReserveThreadReply("r-1", "m-1"))
	_, ok := catalog.PickEligible("r-1", "alice", ActionThreadParent)
	assert.False(t, ok)
}

func TestSoakCatalog_PerRoomAndGlobalEviction(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))

	perRoom := New(2, 10, 0, clock)
	acceptCatalogIDs(t, perRoom, clock, "r-1", "m-1", "m-2", "m-3")
	assert.Equal(t, 2, perRoom.Size())
	_, ok := perRoom.Get("r-1", "m-1")
	assert.False(t, ok)

	global := New(3, 3, 0, clock)
	acceptCatalogIDs(t, global, clock, "r-1", "m-1", "m-2")
	acceptCatalogIDs(t, global, clock, "r-2", "m-3", "m-4")
	assert.Equal(t, 3, global.Size())
	_, ok = global.Get("r-1", "m-1")
	assert.False(t, ok)
}

func TestSoakCatalog_EvictionRetainsPinnedMessages(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(2, 2, 0, clock)
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
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(2, 2, 0, clock)
	for _, messageID := range []string{"m-1", "m-2", "m-3"} {
		assert.True(t, catalog.ObservePinned(&wire.Message{
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
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(4, 2, 0, clock)
	for index, roomID := range []string{"r-1", "r-2", "r-3"} {
		assert.True(t, catalog.ObservePinned(&wire.Message{
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
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "top-level", RoomID: "r-1", Author: "alice",
		CreatedAt: clock.Now(),
	}))
	require.True(t, catalog.Accept("r-1", "top-level"))
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "thread-reply", RoomID: "r-1", Author: "alice",
		CreatedAt: clock.Now(), ThreadParentID: "top-level",
	}))
	require.True(t, catalog.Accept("r-1", "thread-reply"))

	candidate, ok := catalog.PickHistoryVerificationCandidate("r-1")
	require.True(t, ok)
	assert.Equal(t, "top-level", candidate.ID)
}

func TestSoakCatalog_ObservePinnedValidatesAndReconcilesExistingMessage(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := acceptedCatalogMessage(t, clock, 0)

	assert.False(t, catalog.ObservePinned(nil))
	assert.False(t, catalog.ObservePinned(&wire.Message{
		RoomID: "r-1", MessageID: "invalid",
	}))
	before, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.False(t, before.Pinned)

	assert.True(t, catalog.ObservePinned(&wire.Message{
		RoomID: "r-1", MessageID: "m-1",
		Sender: cassandra.Participant{Account: "alice"},
	}))

	message, ok := catalog.Get("r-1", "m-1")
	require.True(t, ok)
	assert.True(t, message.Pinned)
	assert.Equal(t, 1, catalog.Size(), "warmup updates instead of duplicating")
}

func TestSoakCatalog_ConcurrentAccessStaysBounded(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 64, 0, clock)

	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			roomID := fmt.Sprintf("r-%02d", worker%4)
			for i := range 50 {
				messageID := fmt.Sprintf("m-%02d-%03d", worker, i)
				err := catalog.TrackPublished(&Candidate{
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

func acceptedCatalogMessage(t *testing.T, clock *fakeClock, grace time.Duration) *Catalog {
	t.Helper()
	catalog := New(8, 100, grace, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadReplyLimit: 50,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))
	return catalog
}

func acceptCatalogIDs(
	t *testing.T,
	catalog *Catalog,
	clock *fakeClock,
	roomID string,
	messageIDs ...string,
) {
	t.Helper()
	for _, messageID := range messageIDs {
		require.NoError(t, catalog.TrackPublished(&Candidate{
			ID: messageID, RoomID: roomID, Author: "alice",
			CreatedAt: clock.Now(), ThreadReplyLimit: 50,
		}))
		require.True(t, catalog.Accept(roomID, messageID))
		clock.Advance(time.Millisecond)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

// A thread that has never been replied to has no thread room: message-worker
// creates one when the first reply lands. Reading such a "thread" makes
// history-service log `empty thread_room_id` and short-circuit before it ever
// touches the Cassandra thread partition — so the sample is a fast no-op that
// drags the GetThreadMessages percentiles down. The read side therefore needs
// its own predicate, not the write side's.
func TestSoakCatalog_ThreadReadRequiresAnExistingReply(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 10*time.Second, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
		ThreadReplyLimit: 3,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))
	clock.Advance(10 * time.Second)

	// Writable: a zero-reply message is exactly where a new thread starts.
	_, ok := catalog.PickEligible("r-1", "alice", ActionThreadParent)
	require.True(t, ok, "a zero-reply message must stay available as a new thread's parent")

	// Not readable: there is no thread room to read yet.
	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.False(t, ok, "a zero-reply message has no thread room to read")

	require.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.False(t, ok, "a pending reply reservation is not a persisted thread")

	require.True(t, catalog.ConfirmThreadReply("r-1", "m-1"))
	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.False(t, ok, "an accepted reply still needs persistence grace")

	clock.Advance(10 * time.Second)
	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.True(t, ok, "a confirmed reply is readable after persistence grace")
}

// A parent at its reply budget can take no more replies but still has a thread
// to read — the two predicates must not collapse back into one.
func TestSoakCatalog_ThreadReadStaysEligibleAtReplyLimit(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
		ThreadReplyLimit: 1,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))
	require.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	require.True(t, catalog.ConfirmThreadReply("r-1", "m-1"))

	_, ok := catalog.PickEligible("r-1", "alice", ActionThreadParent)
	require.False(t, ok, "budget spent: no more replies may be attached")

	_, ok = catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.True(t, ok, "the thread still exists and must remain readable")
}

func TestSoakCatalog_ThreadReadSkipsDeleted(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "m-1", RoomID: "r-1", Author: "alice", CreatedAt: clock.Now(),
		ThreadReplyLimit: 3,
	}))
	require.True(t, catalog.Accept("r-1", "m-1"))
	require.True(t, catalog.ReserveThreadReply("r-1", "m-1"))
	require.True(t, catalog.ConfirmThreadReply("r-1", "m-1"))
	require.True(t, catalog.MarkDeleted("r-1", "m-1"))

	_, ok := catalog.PickEligible("r-1", "alice", ActionThreadRead)
	require.False(t, ok)
}

func TestSoakCatalog_RejectsInvalidAndRepeatedTransitions(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(1, 1, -time.Second, clock)
	require.Error(t, catalog.TrackPublished(nil))
	require.Error(t, catalog.TrackPublished(&Candidate{}))
	assert.False(t, catalog.Accept("missing", "missing"))
	assert.False(t, catalog.Reject("missing", "missing"))

	candidate := &Candidate{
		ID: "message-1", RoomID: "room-1", Author: "alice",
		CreatedAt: clock.Now(),
	}
	require.NoError(t, catalog.TrackPublished(candidate))
	require.Error(t, catalog.TrackPublished(candidate))
	acceptedAt := clock.Now().Add(time.Second)
	require.True(t, catalog.AcceptAt("room-1", "message-1", acceptedAt))
	assert.False(t, catalog.Accept("room-1", "message-1"))
	require.Error(t, catalog.TrackPublished(candidate))

	for _, action := range []Action{
		ActionEdit, ActionDelete, ActionThreadParent, ActionThreadRead,
		ActionPin, ActionReaction, ActionReadReceipt, Action("invalid"),
	} {
		_, ok := catalog.PickAnyEligible("missing", action)
		assert.False(t, ok)
	}
	_, ok := catalog.PickEligible("missing", "alice", ActionEdit)
	assert.False(t, ok)
	_, ok = catalog.GetEligible("missing", "message-1", ActionEdit)
	assert.False(t, ok)
	_, ok = catalog.PickPinCandidate("missing", false)
	assert.False(t, ok)
	assert.Zero(t, catalog.PinnedCount("missing"))
	_, ok = catalog.PickVerificationCandidate("missing", false)
	assert.False(t, ok)
	_, ok = catalog.PickHistoryVerificationCandidate("missing")
	assert.False(t, ok)
	_, ok = catalog.GetVerificationCandidate("missing", "message-1")
	assert.False(t, ok)
	_, ok = catalog.Get("missing", "message-1")
	assert.False(t, ok)
	assert.Empty(t, catalog.ThreadRecipients("missing", "message-1", "bob"))

	assert.False(t, catalog.MarkEdited("missing", "message-1", "edited"))
	assert.False(t, catalog.MarkDeleted("missing", "message-1"))
	assert.False(t, catalog.SetPinned("missing", "message-1", true))
	assert.False(t, catalog.SetReaction("missing", "message-1", "wave", "bob", true))
	assert.False(t, catalog.ReserveThreadReply("missing", "message-1"))
	assert.False(t, catalog.ReleaseThreadReplyReservation("room-1", "message-1"))
	assert.False(t, catalog.ConfirmThreadReply("room-1", "message-1"))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "", "bob", true))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "wave", "", true))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "wave", "bob", false))
	assert.True(t, catalog.ReserveThreadReply("room-1", "message-1"))
	assert.True(t, catalog.ReleaseThreadReplyReservation("room-1", "message-1"))
	assert.True(t, catalog.MarkDeleted("room-1", "message-1"))
	assert.False(t, catalog.MarkDeleted("room-1", "message-1"))
	assert.False(t, catalog.MarkEdited("room-1", "message-1", "edited"))
	assert.False(t, catalog.SetPinned("room-1", "message-1", true))
	assert.False(t, catalog.ReserveThreadReply("room-1", "message-1"))
}

func TestSoakCatalog_SelectorsExposeOnlyEligibleState(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	// #nosec G601 -- go.mod requires go 1.25; since 1.22 each iteration has its own loop variable
	// nosemgrep: gosec.G601-1
	for _, candidate := range []Candidate{
		{ID: "clean", RoomID: "room-1", Author: "alice", Content: "clean"},
		{ID: "mutated", RoomID: "room-1", Author: "bob", Content: "before"},
	} {
		require.NoError(t, catalog.TrackPublished(&candidate))
		require.True(t, catalog.Accept(candidate.RoomID, candidate.ID))
	}
	require.True(t, catalog.MarkEdited("room-1", "mutated", "after"))

	message, ok := catalog.PickAnyEligible("room-1", ActionReaction)
	require.True(t, ok)
	assert.Equal(t, "mutated", message.ID)
	_, ok = catalog.PickAnyEligible("room-1", Action("invalid"))
	assert.False(t, ok)

	message, ok = catalog.GetEligible("room-1", "clean", ActionEdit)
	require.True(t, ok)
	assert.Equal(t, "clean", message.ID)
	_, ok = catalog.GetEligible("room-1", "missing", ActionEdit)
	assert.False(t, ok)

	message, ok = catalog.PickPinCandidate("room-1", false)
	require.True(t, ok)
	assert.Equal(t, "mutated", message.ID)
	require.True(t, catalog.SetPinned("room-1", "mutated", true))
	message, ok = catalog.PickPinCandidate("room-1", true)
	require.True(t, ok)
	assert.Equal(t, "mutated", message.ID)
	require.True(t, catalog.SetPinned("room-1", "clean", true))
	_, ok = catalog.PickPinCandidate("room-1", false)
	assert.False(t, ok)

	message, ok = catalog.PickVerificationCandidate("room-1", true)
	require.True(t, ok)
	assert.Equal(t, "mutated", message.ID)
	message, ok = catalog.PickVerificationCandidate("room-1", false)
	require.True(t, ok)
	assert.Equal(t, "clean", message.ID)
	message, ok = catalog.GetVerificationCandidate("room-1", "clean")
	require.True(t, ok)
	assert.Equal(t, "clean", message.ID)
	_, ok = catalog.GetVerificationCandidate("room-1", "missing")
	assert.False(t, ok)

	fallback := New(8, 100, 0, clock)
	require.NoError(t, fallback.TrackPublished(&Candidate{
		ID: "only-clean", RoomID: "room-2", Author: "alice",
	}))
	require.True(t, fallback.Accept("room-2", "only-clean"))
	message, ok = fallback.PickVerificationCandidate("room-2", true)
	require.True(t, ok)
	assert.Equal(t, "only-clean", message.ID)

	waiting := New(8, 100, time.Second, clock)
	require.NoError(t, waiting.TrackPublished(&Candidate{
		ID: "waiting", RoomID: "room-3", Author: "alice",
	}))
	require.True(t, waiting.Accept("room-3", "waiting"))
	_, ok = waiting.PickVerificationCandidate("room-3", false)
	assert.False(t, ok)
	_, ok = waiting.PickHistoryVerificationCandidate("room-3")
	assert.False(t, ok)
	_, ok = waiting.GetVerificationCandidate("room-3", "waiting")
	assert.False(t, ok)
}

func TestSoakCatalog_ThreadStateRejectsMissingAndInvalidParents(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	catalog := New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&Candidate{
		ID: "parent", RoomID: "room-1", Author: "alice",
	}))
	require.True(t, catalog.Accept("room-1", "parent"))

	assert.Empty(t, catalog.ThreadRecipients("room-1", "missing", "bob"))
	require.True(t, catalog.ObservePinned(&wire.Message{
		MessageID: "reply", RoomID: "room-1", ThreadParentID: "parent",
		Sender: cassandra.Participant{Account: "bob"},
	}))
	recipients, complete := catalog.ThreadRecipientSet("room-1", "parent", "carol")
	assert.Equal(t, []string{"alice", "bob", "carol"}, recipients)
	assert.True(t, complete)

	require.True(t, catalog.ReserveThreadReply("room-1", "parent"))
	require.True(t, catalog.MarkDeleted("room-1", "parent"))
	assert.False(t, catalog.ConfirmThreadReply("room-1", "parent"))
}
