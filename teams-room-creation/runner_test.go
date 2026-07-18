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
	subj  string
	dedup string
	evt   model.TeamsRoomCreateEvent
}

// recorder is a thread-safe publishFunc that decodes and stores each batch.
// fail is keyed by subject: a batch whose subject is present fails to publish.
func recorder(mu *sync.Mutex, out *[]captured, fail map[string]bool) publishFunc {
	return func(_ context.Context, subj string, data []byte, dedup string) error {
		var e model.TeamsRoomCreateEvent
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		if fail[subj] {
			return errors.New("boom")
		}
		mu.Lock()
		defer mu.Unlock()
		*out = append(*out, captured{subj: subj, dedup: dedup, evt: e})
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
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(chats, nil)

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
		BatchSize: 2, MaxWorkers: 4, Now: func() time.Time { return time.UnixMilli(1700) },
	})
	require.NoError(t, r.run(context.Background()))

	// site-a (3 chats, batch 2) -> 2 batches; site-b -> 1 batch. Total 3.
	subjA := subject.RoomCanonicalTeamsCreate("site-a")
	subjB := subject.RoomCanonicalTeamsCreate("site-b")
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

func TestRunner_FailedBatchNotFlipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(
		[]model.TeamsChat{chat("a1", "site-a"), chat("b1", "site-b")}, nil)
	// Only site-a's chats may be flipped; site-b publish fails.
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []RoomCreatedRef{{ID: "a1", UpdatedAt: chatUpdatedAt}}).Return(nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, map[string]bool{subject.RoomCanonicalTeamsCreate("site-b"): true}), runConfig{
		BatchSize: 10, MaxWorkers: 2, Now: time.Now,
	})
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, got, 1)
	assert.Equal(t, subject.RoomCanonicalTeamsCreate("site-a"), got[0].subj)
}

func TestRunner_MarkErrorLoggedNotFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(
		[]model.TeamsChat{chat("a1", "site-a")}, nil)
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []RoomCreatedRef{{ID: "a1", UpdatedAt: chatUpdatedAt}}).Return(errors.New("mark boom"))

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 10, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(context.Background())) // mark failure logged, not fatal
	assert.Len(t, got, 1)                           // publish still happened
	assert.Equal(t, subject.RoomCanonicalTeamsCreate("site-a"), got[0].subj)
}

func TestRunner_EmptyListNoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(nil, nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 5, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, got)
}

func TestRunner_ListErrorReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(nil, errors.New("db down"))

	r := newRunner(store, recorder(new(sync.Mutex), &[]captured{}, nil), runConfig{BatchSize: 5, MaxWorkers: 2, Now: time.Now})
	require.Error(t, r.run(context.Background()))
}

func TestBuildEvent_MapsMembersDropsID(t *testing.T) {
	e := buildEvent([]model.TeamsChat{chat("a1", "site-a")}, time.UnixMilli(42))
	require.Len(t, e.Chats, 1)
	require.Len(t, e.Chats[0].Members, 1)
	assert.Equal(t, "acct-a1", e.Chats[0].Members[0].Account)
	assert.Equal(t, int64(42), e.Timestamp)
}
