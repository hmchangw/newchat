package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func soakUsers(n int) []model.User {
	users := make([]model.User, n)
	return users
}

// The derived per-user rate is the point of the summary: 55 sends/s spread
// over 2,000 accounts is 2,376 messages per person per day, which no human
// sustains. Printed at startup it is impossible to miss; derived by hand from
// three separate knobs it went unnoticed for the life of the run.
func TestSummarizeSoakTopology_DerivesThePerUserSendRate(t *testing.T) {
	topology := &soakTopology{
		BorrowedUsers: soakUsers(11883),
		ActiveUsers:   soakUsers(2000),
		Rooms:         make([]model.Room, 10000),
		Subscriptions: make([]model.Subscription, 768800),
	}

	got := summarizeSoakTopology(topology, 55)

	assert.Equal(t, 11883, got.BorrowedUsers)
	assert.Equal(t, 2000, got.ActiveUsers)
	assert.Equal(t, 10000, got.Rooms)
	assert.Equal(t, 768800, got.Subscriptions)
	assert.InDelta(t, 64.7, got.SubsPerUser, 0.01)
	assert.InDelta(t, 2376, got.MessagesPerActiveUserPerDay, 1)
}

// A summary that divides by zero would render as null and read as "not
// measured" rather than "nothing to measure".
func TestSummarizeSoakTopology_HandlesAnEmptyTopology(t *testing.T) {
	got := summarizeSoakTopology(&soakTopology{}, 55)

	assert.Zero(t, got.SubsPerUser)
	assert.Zero(t, got.MessagesPerActiveUserPerDay)
}

func TestSummarizeSoakTopology_HandlesAZeroSendRate(t *testing.T) {
	topology := &soakTopology{ActiveUsers: soakUsers(10), BorrowedUsers: soakUsers(10)}

	got := summarizeSoakTopology(topology, 0)

	assert.Zero(t, got.MessagesPerActiveUserPerDay)
}

func TestSummarizeSoakTopology_HandlesANilTopology(t *testing.T) {
	assert.Equal(t, soakTopologySummary{}, summarizeSoakTopology(nil, 55))
}

func TestSoakTopologySummary_LogValuesArePairs(t *testing.T) {
	values := summarizeSoakTopology(&soakTopology{
		BorrowedUsers: soakUsers(4), ActiveUsers: soakUsers(2),
		Rooms: make([]model.Room, 3), Subscriptions: make([]model.Subscription, 8),
	}, 1).LogValues()

	assert.Zero(t, len(values)%2)
	got := attrMap(t, values)
	assert.Equal(t, 4, got["borrowed_users"])
	assert.Equal(t, 2.0, got["subs_per_user"])
	assert.InDelta(t, 43200, got["messages_per_active_user_per_day"], 1)
}
