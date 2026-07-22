package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestBuildOrgQuery_BasicShape(t *testing.T) {
	req := model.SearchOrgsRequest{Query: "engineering", Size: 10, Offset: 0}
	raw, err := buildOrgQuery(req)
	require.NoError(t, err)

	q := parseQuery(t, raw)
	assert.Equal(t, float64(0), q["from"])
	assert.Equal(t, float64(10), q["size"])
	assert.Equal(t, true, q["track_total_hits"])
}

func TestBuildOrgQuery_MultiMatchOverOrgFields(t *testing.T) {
	req := model.SearchOrgsRequest{Query: "engineering"}
	raw, err := buildOrgQuery(req)
	require.NoError(t, err)

	q := parseQuery(t, raw)
	must := q["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	require.Len(t, must, 1)
	mm := must[0].(map[string]any)["multi_match"].(map[string]any)
	assert.Equal(t, "engineering", mm["query"], "Query field value must be passed as the multi_match query")
	assert.Equal(t, "bool_prefix", mm["type"])
	assert.Equal(t, "AND", mm["operator"])

	fields := mm["fields"].([]any)
	got := make([]string, 0, len(fields))
	for _, f := range fields {
		got = append(got, f.(string))
	}
	assert.ElementsMatch(t,
		[]string{"sectName", "sectTCName", "deptName", "deptTCName", "divisionId"}, got,
		"org search must match the human-name fields (+divisionId), not the id/description fields")
}

func TestBuildOrgQuery_NoAccountFilter(t *testing.T) {
	// The spotlight-org index is company-wide (the doc has no userAccount
	// field), so the query must carry NO bool.filter — unlike search.rooms.
	req := model.SearchOrgsRequest{Query: "x"}
	raw, err := buildOrgQuery(req)
	require.NoError(t, err)

	boolq := parseQuery(t, raw)["query"].(map[string]any)["bool"].(map[string]any)
	_, hasFilter := boolq["filter"]
	assert.False(t, hasFilter, "org query must not filter by account — the index is company-wide")
}

func TestBuildOrgQuery_SortByScoreOnly(t *testing.T) {
	// No doc-values field exists on the org index to tiebreak on, and sorting
	// on _id is avoided (fielddata may be disabled on hardened clusters), so
	// the sort is _score only.
	req := model.SearchOrgsRequest{Query: "x"}
	raw, err := buildOrgQuery(req)
	require.NoError(t, err)

	sort := parseQuery(t, raw)["sort"].([]any)
	require.Len(t, sort, 1)
	assert.Equal(t, "_score", sort[0])
}

func TestBuildOrgQuery_PaginationFlows(t *testing.T) {
	req := model.SearchOrgsRequest{Query: "x", Size: 15, Offset: 30}
	raw, err := buildOrgQuery(req)
	require.NoError(t, err)

	q := parseQuery(t, raw)
	assert.Equal(t, float64(30), q["from"])
	assert.Equal(t, float64(15), q["size"])
}
