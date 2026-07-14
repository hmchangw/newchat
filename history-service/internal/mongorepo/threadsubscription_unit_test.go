package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// countStages returns how many pipeline stages carry the given operator key.
func countStages(t *testing.T, pipeline bson.A, op string) int {
	t.Helper()
	n := 0
	for _, raw := range pipeline {
		stage, ok := raw.(bson.D)
		if !ok {
			continue
		}
		for _, e := range stage {
			if e.Key == op {
				n++
			}
		}
	}
	return n
}

func TestUserThreadSubscriptionsPipeline_FirstPageHasNoCursorMatch(t *testing.T) {
	p := userThreadSubscriptionsPipeline("alice", nil, "", 20)
	// First page: userAccount $match + membership $match (sub != []), no value-cursor $match.
	assert.Equal(t, 2, countStages(t, p, "$match"))
	// Two joins: thread_rooms (sort/cursor) and subscriptions (membership). roomName
	// and roomType both ride in on the membership join, so there is no rooms lookup.
	assert.Equal(t, 2, countStages(t, p, "$lookup"))
	// Only thread_rooms is unwound; the membership join uses {$ne: []}, not $unwind.
	assert.Equal(t, 1, countStages(t, p, "$unwind"))
	assert.Equal(t, 1, countStages(t, p, "$sort"))
	// Only the outer page $limit is top-level; the membership join's inner $limit:1
	// is nested inside the $lookup pipeline and not counted here.
	assert.Equal(t, 1, countStages(t, p, "$limit"))
}

func TestUserThreadSubscriptionsPipeline_NextPageAddsCursorMatch(t *testing.T) {
	ts := time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC)
	p := userThreadSubscriptionsPipeline("alice", &ts, "thr-9", 20)
	// Next page: userAccount $match + value-cursor $match + membership $match.
	assert.Equal(t, 3, countStages(t, p, "$match"))
}
