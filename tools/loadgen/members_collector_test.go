package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestMemberCollector_E1Roundtrip(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	t0 := time.Unix(0, 0)
	c.RecordPublish("corr-1", "room-1", []string{"a", "b"}, t0)
	c.RecordReply("corr-1", "", t0.Add(5*time.Millisecond))

	samples := c.E1Samples()
	require.Len(t, samples, 1)
	assert.Equal(t, 5*time.Millisecond, samples[0])
	assert.Equal(t, 1, c.E1Count())
}

func TestMemberCollector_E1RoomServiceError(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	body, _ := json.Marshal(map[string]string{"error": "room is at maximum capacity"})
	t0 := time.Unix(0, 0)
	c.RecordPublish("corr-2", "room-1", []string{"a"}, t0)
	c.RecordReply("corr-2", string(body), t0.Add(1*time.Millisecond))

	require.Len(t, c.E1Samples(), 1)
	assert.Equal(t, 1, c.RoomServiceErrorCount())
}

func TestMemberCollector_E2MatchByRoomAndAccounts(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	t0 := time.Unix(0, 0)
	c.RecordPublish("corr-1", "room-1", []string{"b", "a"}, t0) // unsorted input
	c.RecordMemberEvent("room-1", []string{"a", "b"}, t0.Add(20*time.Millisecond))

	samples := c.E2Samples()
	require.Len(t, samples, 1)
	assert.Equal(t, 20*time.Millisecond, samples[0])
	assert.Equal(t, 1, c.E2Count())
}

func TestMemberCollector_E2NoMatchDropped(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	c.RecordPublish("corr-1", "room-1", []string{"a"}, time.Unix(0, 0))
	c.RecordMemberEvent("room-2", []string{"a"}, time.Unix(0, 0)) // wrong room
	assert.Equal(t, 0, c.E2Count())
}

func TestMemberCollector_Finalize(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	t0 := time.Unix(0, 0)
	c.RecordPublish("corr-1", "room-1", []string{"a"}, t0)
	c.RecordPublish("corr-2", "room-2", []string{"b"}, t0)
	c.RecordReply("corr-1", "", t0.Add(time.Millisecond))
	c.RecordMemberEvent("room-2", []string{"b"}, t0.Add(time.Millisecond))

	missingReplies, missingEvents := c.Finalize()
	assert.Equal(t, 1, missingReplies, "corr-2 had no reply")
	assert.Equal(t, 1, missingEvents, "room-1 had no member event")
}

func TestMemberCollector_DiscardBefore(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	t0 := time.Unix(0, 0)
	c.RecordPublish("c1", "room-1", []string{"a"}, t0)
	c.RecordReply("c1", "", t0.Add(time.Millisecond))
	c.RecordPublish("c2", "room-2", []string{"b"}, t0.Add(10*time.Second))
	c.RecordReply("c2", "", t0.Add(10*time.Second+time.Millisecond))

	c.DiscardBefore(t0.Add(5 * time.Second))
	assert.Equal(t, 1, c.E1Count())
}

func TestParseMemberAddEvent(t *testing.T) {
	evt := model.MemberAddEvent{
		Type: "member_added", RoomID: "room-1",
		Accounts: []string{"a", "b"},
	}
	data, _ := json.Marshal(evt)
	roomID, accounts, ok := ParseMemberAddEvent(data)
	require.True(t, ok)
	assert.Equal(t, "room-1", roomID)
	assert.Equal(t, []string{"a", "b"}, accounts)

	_, _, ok = ParseMemberAddEvent([]byte("not json"))
	assert.False(t, ok)

	evt.Type = "member_removed"
	bad, _ := json.Marshal(evt)
	_, _, ok = ParseMemberAddEvent(bad)
	assert.False(t, ok, "non-added events must be filtered")
}

func TestMemberCollector_OnMemberEventCallback(t *testing.T) {
	m := NewMetrics()
	c := NewMemberCollector(m, "p", InjectFrontdoor)
	seen := make(chan string, 4)
	c.OnMemberEvent(func(roomID string, accounts []string) {
		seen <- roomID
	})
	c.RecordPublish("c1", "room-1", []string{"a"}, time.Unix(0, 0))
	c.RecordMemberEvent("room-1", []string{"a"}, time.Unix(0, time.Millisecond.Nanoseconds()))
	select {
	case got := <-seen:
		assert.Equal(t, "room-1", got)
	case <-time.After(time.Second):
		t.Fatal("callback never fired")
	}
}
