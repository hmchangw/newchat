package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuiltinHistoryPreset(t *testing.T) {
	cases := []string{"history-small", "history-medium", "history-large", "history-sparse"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinHistoryPreset(name)
			require.True(t, ok, "preset %s not registered", name)
			assert.Equal(t, name, p.Name)
			assert.Greater(t, p.Users, 0)
			assert.Greater(t, p.Rooms, 0)
			assert.Greater(t, p.MessagesPerRoom, 0)
			assert.Greater(t, p.MessageSpanDays, 0)
			assert.GreaterOrEqual(t, p.ThreadRate, 0.0)
			assert.LessOrEqual(t, p.ThreadRate, 1.0)
		})
	}
	if _, ok := BuiltinHistoryPreset("nope"); ok {
		t.Fatalf("nope should not be a preset")
	}
}

func TestBuildHistoryFixtures_Deterministic(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	a := BuildHistoryFixtures(&p, 42, "site-a", now)
	b := BuildHistoryFixtures(&p, 42, "site-a", now)
	assert.Equal(t, a.Fixtures.Users, b.Fixtures.Users)
	assert.Equal(t, a.Fixtures.Rooms, b.Fixtures.Rooms)
	assert.Equal(t, a.Fixtures.Subscriptions, b.Fixtures.Subscriptions)
	assert.Equal(t, a.ThreadParents, b.ThreadParents)
	aPlan := a.FullPlan()
	bPlan := b.FullPlan()
	require.Equal(t, len(aPlan.Messages), len(bPlan.Messages))
	for i := range aPlan.Messages {
		assert.Equal(t, aPlan.Messages[i].MessageID, bPlan.Messages[i].MessageID, "msg[%d]", i)
		assert.Equal(t, aPlan.Messages[i].CreatedAt, bPlan.Messages[i].CreatedAt, "msg[%d]", i)
	}
}

func TestBuildHistoryFixtures_MessageCountPerRoom(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 42, "site-a", now)
	plan := res.FullPlan()

	counts := map[string]int{}
	for i := range plan.Messages {
		counts[plan.Messages[i].RoomID]++
	}
	// Plan includes top-level + thread replies. Per-room top-level = MessagesPerRoom.
	// Per-room total = MessagesPerRoom + thread replies.
	// Top-level messages are identified by an absent ThreadParentID. Thread
	// parents also count as top-level even though they carry a ThreadRoomID;
	// using ThreadRoomID=="" here would silently break if ThreadRate were
	// raised on this preset.
	topLevelByRoom := map[string]int{}
	for i := range plan.Messages {
		if plan.Messages[i].ThreadParentID == "" {
			topLevelByRoom[plan.Messages[i].RoomID]++
		}
	}
	require.Equal(t, p.Rooms, len(topLevelByRoom))
	for _, room := range res.Fixtures.Rooms {
		assert.Equal(t, p.MessagesPerRoom, topLevelByRoom[room.ID], "room %s", room.ID)
	}
}

func TestBuildHistoryFixtures_MessageTimestampsInSpan(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 1, "site-a", now)
	plan := res.FullPlan()
	spanStart := now.Add(-time.Duration(p.MessageSpanDays) * 24 * time.Hour)

	for i := range plan.Messages {
		msg := &plan.Messages[i]
		assert.False(t, msg.CreatedAt.Before(spanStart), "msg[%d] %s predates span start", i, msg.CreatedAt)
		assert.False(t, msg.CreatedAt.After(now), "msg[%d] %s postdates now", i, msg.CreatedAt)
	}
}

func TestBuildHistoryFixtures_ThreadParents(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 1, "site-a", now)
	plan := res.FullPlan()

	// Every thread reply must reference a known parent and ThreadRoomID; every
	// parent recorded in ThreadParents must exist as a top-level message
	// (top-level = ThreadParentID == ""; thread parents themselves are
	// top-level and carry a ThreadRoomID for downstream queries).
	topLevelByID := map[string]*plannedMessage{}
	for i := range plan.Messages {
		if plan.Messages[i].ThreadParentID == "" {
			topLevelByID[plan.Messages[i].MessageID] = &plan.Messages[i]
		}
	}
	for i := range plan.Messages {
		msg := &plan.Messages[i]
		if msg.ThreadParentID == "" {
			continue
		}
		parent, ok := topLevelByID[msg.ThreadParentID]
		require.True(t, ok, "reply %s references unknown parent %s", msg.MessageID, msg.ThreadParentID)
		require.NotEmpty(t, parent.ThreadRoomID, "parent %s missing ThreadRoomID", parent.MessageID)
		assert.Equal(t, parent.ThreadRoomID, msg.ThreadRoomID, "reply %s thread room", msg.MessageID)
	}

	// ThreadParents map round-trip: every roomID -> parent IDs exists in plan.
	for roomID, parents := range res.ThreadParents {
		for _, parentID := range parents {
			parent, ok := topLevelByID[parentID.MessageID]
			require.True(t, ok, "parent %s in room %s missing from plan", parentID.MessageID, roomID)
			assert.Equal(t, roomID, parent.RoomID)
			assert.NotEmpty(t, parent.ThreadRoomID)
		}
	}
}

func TestBuildHistoryFixtures_ThreadReplyTimestampNearParent(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 1, "site-a", now)
	plan := res.FullPlan()

	parentByID := map[string]time.Time{}
	for i := range plan.Messages {
		m := &plan.Messages[i]
		if m.ThreadRoomID != "" && m.ThreadParentID == "" {
			parentByID[m.MessageID] = m.CreatedAt
		}
	}
	for i := range plan.Messages {
		msg := &plan.Messages[i]
		if msg.ThreadParentID == "" {
			continue
		}
		parentAt, ok := parentByID[msg.ThreadParentID]
		require.True(t, ok)
		delta := msg.CreatedAt.Sub(parentAt)
		assert.GreaterOrEqual(t, delta, time.Minute, "reply %s too close to parent", msg.MessageID)
		assert.LessOrEqual(t, delta, 10*time.Minute, "reply %s too far from parent", msg.MessageID)
	}
}

func TestBuildHistoryFixtures_RoomLastMsgAtMatchesLatest(t *testing.T) {
	// Room.LastMsgAt must equal the latest top-level (non-reply) message
	// CreatedAt for that room. History-service caps `before` by lastMsgAt+1ms
	// so a stale or zero value would either clip the walk or empty it.
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 7, "site-a", now)
	plan := res.FullPlan()

	latest := map[string]time.Time{}
	for i := range plan.Messages {
		msg := &plan.Messages[i]
		if msg.ThreadParentID != "" {
			continue
		}
		if t, ok := latest[msg.RoomID]; !ok || msg.CreatedAt.After(t) {
			latest[msg.RoomID] = msg.CreatedAt
		}
	}
	for i := range res.Fixtures.Rooms {
		room := &res.Fixtures.Rooms[i]
		require.NotNil(t, room.LastMsgAt, "room %s missing LastMsgAt", room.ID)
		assert.Equal(t, latest[room.ID].UTC(), room.LastMsgAt.UTC())
	}
}

func TestBuildHistoryFixtures_SenderIsRoomMember(t *testing.T) {
	// Sender of every planned message must be a subscriber of the room so the
	// fixture stays internally consistent.
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 11, "site-a", now)
	plan := res.FullPlan()

	membersByRoom := map[string]map[string]bool{}
	for i := range res.Fixtures.Subscriptions {
		s := &res.Fixtures.Subscriptions[i]
		if membersByRoom[s.RoomID] == nil {
			membersByRoom[s.RoomID] = map[string]bool{}
		}
		membersByRoom[s.RoomID][s.User.Account] = true
	}
	for i := range plan.Messages {
		msg := &plan.Messages[i]
		assert.True(t, membersByRoom[msg.RoomID][msg.SenderAccount],
			"sender %s not a member of room %s", msg.SenderAccount, msg.RoomID)
	}
}

func TestIterateRoomMessages_OneBatchPerRoomMatchesFullPlan(t *testing.T) {
	// IterateRoomMessages must yield exactly one batch per room, each batch
	// containing only that room's messages, in the same order FullPlan
	// produces by concatenation. Streaming + materialization must agree
	// row-for-row or the seed path diverges from what tests assert.
	p, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 3, "site-a", now)

	var batches [][]plannedMessage
	require.NoError(t, res.IterateRoomMessages(func(msgs []plannedMessage) error {
		batches = append(batches, append([]plannedMessage(nil), msgs...))
		return nil
	}))
	require.Equal(t, p.Rooms, len(batches))
	for i, b := range batches {
		require.NotEmpty(t, b, "batch %d empty", i)
		want := res.Fixtures.Rooms[i].ID
		for j := range b {
			require.Equal(t, want, b[j].RoomID, "batch %d msg %d wrong room", i, j)
		}
	}
	full := res.FullPlan()
	concat := make([]plannedMessage, 0, len(full.Messages))
	for _, b := range batches {
		concat = append(concat, b...)
	}
	require.Equal(t, len(full.Messages), len(concat))
	for i := range full.Messages {
		assert.Equal(t, full.Messages[i].MessageID, concat[i].MessageID, "msg[%d]", i)
		assert.Equal(t, full.Messages[i].CreatedAt, concat[i].CreatedAt, "msg[%d]", i)
	}
}

func TestIterateRoomMessages_PropagatesError(t *testing.T) {
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 5, "site-a", now)

	calls := 0
	sentinel := fmt.Errorf("stop")
	err := res.IterateRoomMessages(func(_ []plannedMessage) error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, calls, "iterator should stop after first error")
}

func TestSummarizeRoomPlan_MatchesFullBuild(t *testing.T) {
	// The cheap metadata walk and the full per-room build share the same
	// structural RNG sequence — their parent IDs and latest top-level
	// CreatedAt must agree. If they diverge, BuildHistoryFixtures sets the
	// wrong LastMsgAt / ThreadParents and the seed path silently corrupts
	// downstream fixtures.
	p, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	res := BuildHistoryFixtures(&p, 9, "site-a", now)

	for i, roomID := range res.roomIDs {
		members := res.membersByRoom[i]
		fullMsgs := buildRoomMessages(&p, roomID, members,
			res.now, time.Duration(p.MessageSpanDays)*24*time.Hour, res.roomSeeds[i])

		var latestFromFull time.Time
		var parentsFromFull []ThreadParentRef
		for j := range fullMsgs {
			m := &fullMsgs[j]
			if m.ThreadParentID != "" {
				continue
			}
			if m.CreatedAt.After(latestFromFull) {
				latestFromFull = m.CreatedAt
			}
			if m.ThreadRoomID != "" {
				parentsFromFull = append(parentsFromFull, ThreadParentRef{
					MessageID: m.MessageID, ThreadRoomID: m.ThreadRoomID,
				})
			}
		}

		latestFromSummary, parentsFromSummary := summarizeRoomPlan(&p, roomID, len(members),
			res.now, time.Duration(p.MessageSpanDays)*24*time.Hour, res.roomSeeds[i].structural)

		assert.Equal(t, latestFromFull, latestFromSummary, "room %s latest", roomID)
		assert.Equal(t, parentsFromFull, parentsFromSummary, "room %s parents", roomID)
	}
}
