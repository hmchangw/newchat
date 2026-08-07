package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// captured records one publish call.
type captured struct {
	subj string
	evt  model.TeamsRoomCreateEvent
}

// recorder is a thread-safe publishFunc that decodes and stores each batch.
// fail is keyed by subject: a batch whose subject is present fails to publish.
func recorder(mu *sync.Mutex, out *[]captured, fail map[string]bool) publishFunc {
	return func(_ context.Context, subj string, data []byte) error {
		var e model.TeamsRoomCreateEvent
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		if fail[subj] {
			return errors.New("boom")
		}
		mu.Lock()
		defer mu.Unlock()
		*out = append(*out, captured{subj: subj, evt: e})
		return nil
	}
}

// chatUpdatedAt is the UpdatedAt stamp on every test chat, threaded into the
// RoomCreatedRef the runner passes to MarkRoomsCreated (the CAS token).
var chatUpdatedAt = time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

func chat(id, site string) model.TeamsChat {
	return model.TeamsChat{
		ID: id, Name: "n-" + id, SiteID: site, UpdatedAt: chatUpdatedAt,
		CreatedDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		Members: []model.TeamsChatMember{{
			ID: "m-" + id, Account: "acct-" + id,
			VisibleHistoryStartDateTime: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
}

func TestRunner_GroupsBatchesAndFlipsOnAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	chats := []model.TeamsChat{
		chat("a1", "site-a"), chat("a2", "site-a"), chat("a3", "site-a"),
		chat("b1", "site-b"),
	}
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 50).Return(chats, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "b1", 50).Return(nil, nil),
	)

	var markMu sync.Mutex
	marked := map[string]bool{}
	store.EXPECT().MarkRoomsCreated(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, refs []RoomCreatedRef) error {
			markMu.Lock()
			defer markMu.Unlock()
			for _, r := range refs {
				marked[r.ID] = true
			}
			return nil
		}).AnyTimes()

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{
		BatchSize: 2, PageSize: 50, MaxWorkers: 4, Now: func() time.Time { return time.UnixMilli(1700) },
	})
	require.NoError(t, r.run(context.Background()))

	// site-a (3 chats, batch 2) -> 2 batches; site-b -> 1 batch. Total 3.
	subjA := subject.RoomTeamsCanonicalCreate("site-a")
	subjB := subject.RoomTeamsCanonicalCreate("site-b")
	assert.Len(t, got, 3)
	bySubj := map[string]int{}
	for _, c := range got {
		assert.Equal(t, int64(1700), c.evt.Timestamp)
		assert.LessOrEqual(t, len(c.evt.Chats), 2)
		for _, ch := range c.evt.Chats {
			assert.Equal(t, "acct-"+ch.ID, ch.Members[0].Account)
			assert.Equal(t, "n-"+ch.ID, ch.Name)
		}
		bySubj[c.subj]++
	}
	assert.Equal(t, 2, bySubj[subjA], "site-a: 3 chats / batch 2 -> 2 batches")
	assert.Equal(t, 1, bySubj[subjB], "site-b: 1 batch")
	assert.True(t, marked["a1"] && marked["a2"] && marked["a3"] && marked["b1"])
	assert.Len(t, marked, 4)
}

// TestRunner_PagesUntilEmpty drives two full pages plus the terminating empty
// page, and asserts each page is fully published before the next page is
// fetched (the second/third list calls observe all prior publishes).
func TestRunner_PagesUntilEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	var mu sync.Mutex
	var got []captured

	page1 := []model.TeamsChat{chat("a1", "site-a"), chat("a2", "site-a")}
	page2 := []model.TeamsChat{chat("a3", "site-a"), chat("b1", "site-b")}
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 2).Return(page1, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "a2", 2).DoAndReturn(
			func(context.Context, string, int) ([]model.TeamsChat, error) {
				mu.Lock()
				defer mu.Unlock()
				assert.Len(t, got, 1, "page 1 (one batch) must be published before page 2 is fetched")
				return page2, nil
			}),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "b1", 2).DoAndReturn(
			func(context.Context, string, int) ([]model.TeamsChat, error) {
				mu.Lock()
				defer mu.Unlock()
				assert.Len(t, got, 3, "page 2 (two batches) must be published before the final fetch")
				return nil, nil
			}),
	)

	var markMu sync.Mutex
	marked := map[string]bool{}
	store.EXPECT().MarkRoomsCreated(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, refs []RoomCreatedRef) error {
			markMu.Lock()
			defer markMu.Unlock()
			for _, r := range refs {
				marked[r.ID] = true
			}
			return nil
		}).AnyTimes()

	r := newRunner(store, recorder(&mu, &got, nil), runConfig{
		BatchSize: 10, PageSize: 2, MaxWorkers: 4, Now: time.Now,
	})
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, got, 3, "page 1: 1 batch (site-a); page 2: 2 batches (site-a, site-b)")
	assert.True(t, marked["a1"] && marked["a2"] && marked["a3"] && marked["b1"])
	assert.Len(t, marked, 4)
}

// TestRunner_CursorAdvancesPastFailedBatch: a failed publish leaves its chats
// flagged for the next CronJob run, but the page cursor still advances so the
// current run terminates instead of refetching the same page.
func TestRunner_CursorAdvancesPastFailedBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 1).Return(
			[]model.TeamsChat{chat("a1", "site-a")}, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "a1", 1).Return(nil, nil),
	)
	// Publish fails, so MarkRoomsCreated must never be called.

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, map[string]bool{subject.RoomTeamsCanonicalCreate("site-a"): true}), runConfig{
		BatchSize: 10, PageSize: 1, MaxWorkers: 2, Now: time.Now,
	})
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, got)
}

func TestRunner_FailedBatchNotFlipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 50).Return(
			[]model.TeamsChat{chat("a1", "site-a"), chat("b1", "site-b")}, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "b1", 50).Return(nil, nil),
	)
	// Only site-a's chats may be flipped; site-b publish fails.
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []RoomCreatedRef{{ID: "a1", UpdatedAt: chatUpdatedAt}}).Return(nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, map[string]bool{subject.RoomTeamsCanonicalCreate("site-b"): true}), runConfig{
		BatchSize: 10, PageSize: 50, MaxWorkers: 2, Now: time.Now,
	})
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, got, 1)
	assert.Equal(t, subject.RoomTeamsCanonicalCreate("site-a"), got[0].subj)
}

func TestRunner_MarkErrorLoggedNotFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 50).Return(
			[]model.TeamsChat{chat("a1", "site-a")}, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "a1", 50).Return(nil, nil),
	)
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []RoomCreatedRef{{ID: "a1", UpdatedAt: chatUpdatedAt}}).Return(errors.New("mark boom"))

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 10, PageSize: 50, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(context.Background())) // mark failure logged, not fatal
	assert.Len(t, got, 1)                           // publish still happened
	assert.Equal(t, subject.RoomTeamsCanonicalCreate("site-a"), got[0].subj)
}

func TestRunner_EmptyListNoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 50).Return(nil, nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 5, PageSize: 50, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, got)
}

func TestRunner_ListErrorReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 50).Return(nil, errors.New("db down"))

	r := newRunner(store, recorder(new(sync.Mutex), &[]captured{}, nil), runConfig{BatchSize: 5, PageSize: 50, MaxWorkers: 2, Now: time.Now})
	require.Error(t, r.run(context.Background()))
}

// TestRunner_ListErrorOnLaterPageReturned: a list failure mid-run aborts with
// an error; pages already processed keep their published batches.
func TestRunner_ListErrorOnLaterPageReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "", 1).Return(
			[]model.TeamsChat{chat("a1", "site-a")}, nil),
		store.EXPECT().ListChatsNeedingRoom(gomock.Any(), "a1", 1).Return(nil, errors.New("db down")),
	)
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []RoomCreatedRef{{ID: "a1", UpdatedAt: chatUpdatedAt}}).Return(nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 5, PageSize: 1, MaxWorkers: 2, Now: time.Now})
	require.Error(t, r.run(context.Background()))
	assert.Len(t, got, 1, "page 1 was published before the failing fetch")
}

func TestBuildEvent_MapsMembers(t *testing.T) {
	e := buildEvent([]model.TeamsChat{chat("a1", "site-a")}, time.UnixMilli(42))
	require.Len(t, e.Chats, 1)
	require.Len(t, e.Chats[0].Members, 1)
	assert.Equal(t, "m-a1", e.Chats[0].Members[0].ID, "the member's user id is carried")
	assert.Equal(t, "acct-a1", e.Chats[0].Members[0].Account)
	assert.Equal(t, int64(42), e.Timestamp)
}
