package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// stageNames returns each pipeline stage's operator, in order.
func stageNames(t *testing.T, p []bson.D) []string {
	t.Helper()
	names := make([]string, 0, len(p))
	for _, stage := range p {
		require.Len(t, stage, 1, "each aggregation stage carries exactly one operator")
		names = append(names, stage[0].Key)
	}
	return names
}

func indexOf(names []string, want string) int {
	for i, n := range names {
		if n == want {
			return i
		}
	}
	return -1
}

// The two correlated $lookups run once per matched user, so every byte carried
// into them is multiplied. Narrowing the document before the joins is what keeps
// a 5k-person department from dragging 5k full user documents through both.
func TestOrgMembersPipeline_NarrowsDocumentBeforeLookups(t *testing.T) {
	names := stageNames(t, orgMembersPipeline("room-1", "org-1"))

	firstProject := indexOf(names, "$project")
	firstLookup := indexOf(names, "$lookup")

	require.NotEqual(t, -1, firstProject, "pipeline must project")
	require.NotEqual(t, -1, firstLookup, "pipeline must join room_members")
	assert.Less(t, firstProject, firstLookup,
		"an early $project must narrow the user document before the correlated lookups: got order %v", names)
	assert.Equal(t, "$match", names[0], "the indexed $match selects the org before anything else runs")
}

// The early projection is only safe if it keeps every field the later stages
// read; dropping one would silently blank a name or break the sibling-org join.
func TestOrgMembersPipeline_EarlyProjectionKeepsFieldsLaterStagesRead(t *testing.T) {
	p := orgMembersPipeline("room-1", "org-1")
	names := stageNames(t, p)
	early, ok := p[indexOf(names, "$project")][0].Value.(bson.M)
	require.True(t, ok, "early $project is a bson.M")

	// _id feeds the individual-membership lookup; sectId/deptId feed the
	// sibling-org lookup and the isDept/name/tcName computations.
	for _, field := range []string{"account", "siteId", "sectId", "deptId", "deptName", "sectName", "deptTCName", "sectTCName"} {
		assert.Contains(t, early, field, "early projection must keep %q for a later stage", field)
	}
	assert.NotContains(t, early, "_id", "_id is retained implicitly; excluding it would break the membership lookup")
}
