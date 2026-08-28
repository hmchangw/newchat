package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ordering and the withinDays window key off the USER activity position:
// lastUserMsgAt when present, else lastMsgAt (pre-deploy rooms), else createdAt.
func TestBuildListRows_SortAtPrefersLastUserMsgAt(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	userAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sysAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC) // newer system bump

	rows := buildListRows(
		[]subLite{{ID: "s1", RoomID: "r1"}, {ID: "s2", RoomID: "r2"}, {ID: "s3", RoomID: "r3"}},
		map[string]roomSortKey{
			"r1": {LastUserMsgAt: &userAt, LastMsgAt: &sysAt, CreatedAt: &created},
			"r2": {LastMsgAt: &sysAt, CreatedAt: &created}, // pre-deploy room: falls back to lastMsgAt
			"r3": {CreatedAt: &created},                    // no messages at all
		},
		"alice", false, nil,
	)
	require.Len(t, rows, 3)
	byRoom := map[string]*time.Time{}
	for _, r := range rows {
		byRoom[r.sub.RoomID] = r.sortAt
	}
	assert.True(t, byRoom["r1"].Equal(userAt), "user activity outranks the newer system bump")
	assert.True(t, byRoom["r2"].Equal(sysAt), "no lastUserMsgAt yet: lastMsgAt keeps today's ordering")
	assert.True(t, byRoom["r3"].Equal(created))
}

func TestBuildListRows_WindowUsesUserActivity(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	userAt := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sysAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	rows := buildListRows(
		[]subLite{{ID: "s1", RoomID: "r1"}},
		map[string]roomSortKey{"r1": {LastUserMsgAt: &userAt, LastMsgAt: &sysAt, CreatedAt: &created}},
		"alice", false, &cutoff,
	)
	assert.Empty(t, rows, "a rename must not resurface a dormant room inside the activity window")
}
