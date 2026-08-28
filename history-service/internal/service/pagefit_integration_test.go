//go:build integration

package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/pagefit"
)

// Walking a room whose pages are trimmed must yield every message exactly once.
// A cursor derived from the wrong row would show up here as a gap or a repeat,
// which no unit test over mocked rows can prove against real Cassandra ordering.
func TestLoadHistory_TrimmedPagination_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)

	// A budget that admits only a handful of these rows, so the walk takes
	// several trimmed pages rather than one.
	svc := closeOnCleanupIn(t, New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, &recordingPublisher{}, nil, nil, nil, nil,
		&config.Config{MessageHistoryFloorDays: 730, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true},
		WithPageBudget(pagefit.NewBudget(8<<10, 0))))

	const (
		roomID  = "r-pagefit"
		total   = 40
		bodyLen = 1024
	)
	sender := models.Participant{ID: "u1", Account: "alice"}
	bucket := msgbucket.New(24 * time.Hour)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Duration(total) * time.Minute)

	want := make(map[string]bool, total)
	for i := range total {
		id := fmt.Sprintf("m-%03d", i)
		at := base.Add(time.Duration(i) * time.Minute)
		require.NoError(t, session.Query(
			`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			roomID, bucket.Of(at), at, id, sender, strings.Repeat("x", bodyLen), "",
		).Exec())
		want[id] = true
	}

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	seen := map[string]int{}
	var before *int64
	pages := 0

	for {
		resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{Before: before, Limit: 100})
		require.NoError(t, err)
		pages++
		require.Less(t, pages, total+2, "the walk must terminate")

		// The store's HasNext is a resumability hint (the bucket walk may stop
		// on budget over a silent gap), so a final empty page is legitimate.
		if len(resp.Messages) == 0 {
			require.False(t, resp.HasNext, "an empty page must never claim more — the client would have no position to page from")
			break
		}

		for _, m := range resp.Messages {
			seen[m.MessageID]++
			assert.False(t, m.Truncated, "no row here is large enough to need blanking")
		}
		if !resp.HasNext {
			break
		}
		oldest := resp.Messages[len(resp.Messages)-1].CreatedAt.UnixMilli()
		before = &oldest
	}

	assert.Greater(t, pages, 1, "the budget must actually have forced multiple pages")
	for id := range want {
		assert.Equal(t, 1, seen[id], "message %s must appear exactly once across the walk", id)
	}
	assert.Len(t, seen, total, "the walk must return every seeded message and nothing else")
}

// The reviewer's case, against the real strict `created_at < ?` bound: rows
// sharing one millisecond must survive a trim that would otherwise cut through
// them. A skipped row shows up here as a missing ID, which no mocked-store test
// can prove against Cassandra's real clustering order.
func TestLoadHistory_EqualTimestampsSurviveTrimming_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	svc := closeOnCleanupIn(t, New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, &recordingPublisher{}, nil, nil, nil, nil,
		&config.Config{MessageHistoryFloorDays: 730, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true},
		WithPageBudget(pagefit.NewBudget(6<<10, 0))))

	const (
		roomID  = "r-ties"
		groups  = 8
		perTick = 3
		bodyLen = 1024
	)
	sender := models.Participant{ID: "u1", Account: "alice"}
	bucket := msgbucket.New(24 * time.Hour)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Hour)

	want := map[string]bool{}
	for g := range groups {
		// Every row in a group shares one millisecond, so any budget that cuts
		// mid-group would drop the rest of it permanently.
		at := base.Add(time.Duration(g) * time.Minute)
		for i := range perTick {
			id := fmt.Sprintf("m-%02d-%d", g, i)
			require.NoError(t, session.Query(
				`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				roomID, bucket.Of(at), at, id, sender, strings.Repeat("x", bodyLen), "",
			).Exec())
			want[id] = true
		}
	}

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	seen := map[string]int{}
	var before *int64
	for pages := 0; ; pages++ {
		require.Less(t, pages, groups*perTick+2, "the walk must terminate")
		resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{Before: before, Limit: 100})
		require.NoError(t, err)
		if len(resp.Messages) == 0 {
			require.False(t, resp.HasNext)
			break
		}
		for _, m := range resp.Messages {
			seen[m.MessageID]++
		}
		if !resp.HasNext {
			break
		}
		oldest := resp.Messages[len(resp.Messages)-1].CreatedAt.UnixMilli()
		before = &oldest
	}

	for id := range want {
		assert.Equal(t, 1, seen[id], "message %s must appear exactly once — a tied row was skipped", id)
	}
	assert.Len(t, seen, len(want))
}
