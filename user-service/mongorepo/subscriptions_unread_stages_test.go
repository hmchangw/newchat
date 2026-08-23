package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// unreadEnrichStages must carry ONLY what the unread count reads from the room —
// room.lastMsgAt, surfaced to the top level — plus the ^Del- soft-delete drop.
// It must NOT move the room metadata, counts, or the E2E key blob (encKey.priv)
// that the full roomsEnrichStages carries: the unread path reads none of them,
// and pulling key material into the process for every active subscription (up to
// MAX_SUBSCRIPTION_LIMIT rows) is both wasteful and needless key exposure.
func TestUnreadEnrichStages_LeanRoomProjection(t *testing.T) {
	stages := unreadEnrichStages()

	proj := lookupRoomProjection(t, stages)
	assert.ElementsMatch(t, []string{"name", "lastMsgAt"}, mapKeys(proj),
		"the rooms $lookup must project only name (for the Del- drop) + lastMsgAt (surfaced)")
	for _, banned := range []string{
		"encKey.priv", "encKey.ver", "userCount", "appCount", "lastMsgId",
		"lastMentionAllAt", "minUserLastSeenAt", "createdAt", "crossSite",
	} {
		_, ok := proj[banned]
		assert.Falsef(t, ok, "the unread projection must not carry %q", banned)
	}

	assert.True(t, stagesSurfaceLastMsgAt(stages), "lastMsgAt must be surfaced to the top level via $addFields")
	assert.True(t, stagesDropDeleted(stages), "a ^Del- room.name $match must drop locally soft-deleted rooms")
}

// lookupRoomProjection returns the $project document inside the first $lookup
// stage's correlated subpipeline.
func lookupRoomProjection(t *testing.T, stages bson.A) bson.M {
	t.Helper()
	for _, st := range stages {
		m, ok := st.(bson.M)
		if !ok {
			continue
		}
		lookup, ok := m["$lookup"].(bson.M)
		if !ok {
			continue
		}
		pipeline, ok := lookup["pipeline"].(bson.A)
		require.True(t, ok, "$lookup must use a correlated subpipeline")
		for _, sub := range pipeline {
			sm, ok := sub.(bson.M)
			if !ok {
				continue
			}
			if proj, ok := sm["$project"].(bson.M); ok {
				return proj
			}
		}
	}
	t.Fatalf("no $lookup subpipeline $project found in stages")
	return nil
}

func mapKeys(m bson.M) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// stagesSurfaceLastMsgAt reports whether an $addFields stage lifts room.lastMsgAt
// to the top-level lastMsgAt field the unread check reads.
func stagesSurfaceLastMsgAt(stages bson.A) bool {
	for _, st := range stages {
		m, ok := st.(bson.M)
		if !ok {
			continue
		}
		add, ok := m["$addFields"].(bson.M)
		if !ok {
			continue
		}
		if v, ok := add["lastMsgAt"].(string); ok && v == "$room.lastMsgAt" {
			return true
		}
	}
	return false
}

// stagesDropDeleted reports whether a $match drops rooms whose name matches the
// soft-delete regex.
func stagesDropDeleted(stages bson.A) bool {
	for _, st := range stages {
		m, ok := st.(bson.M)
		if !ok {
			continue
		}
		match, ok := m["$match"].(bson.M)
		if !ok {
			continue
		}
		if _, ok := match["room.name"]; ok {
			return true
		}
	}
	return false
}
