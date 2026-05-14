package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBuildSearchAppsPipeline_MatchesNameRegex(t *testing.T) {
	got := buildSearchAppsPipeline("weather", "alice", nil, 0, 25)
	require.NotEmpty(t, got)

	match, ok := got[0]["$match"].(bson.M)
	require.True(t, ok, "first stage must be $match")
	name, ok := match["name"].(bson.M)
	require.True(t, ok, "$match.name must be a bson.M holding $regex/$options")
	assert.Equal(t, "weather", name["$regex"], "regex literal must be the raw query (after QuoteMeta)")
	assert.Equal(t, "i", name["$options"], "regex must be case-insensitive")
}

func TestBuildSearchAppsPipeline_EscapesRegexMetacharacters(t *testing.T) {
	got := buildSearchAppsPipeline("a.b*c", "alice", nil, 0, 25)
	match, _ := got[0]["$match"].(bson.M)
	name, _ := match["name"].(bson.M)
	assert.Equal(t, `a\.b\*c`, name["$regex"], "regex metacharacters must be escaped via regexp.QuoteMeta")
}

func TestBuildSearchAppsPipeline_AssistantEnabledNil_NoFilter(t *testing.T) {
	got := buildSearchAppsPipeline("weather", "alice", nil, 0, 25)
	match, _ := got[0]["$match"].(bson.M)
	_, has := match["assistant.enabled"]
	assert.False(t, has, "nil AssistantEnabled must not add a match filter")
}

func TestBuildSearchAppsPipeline_AssistantEnabledTrue(t *testing.T) {
	enabled := true
	got := buildSearchAppsPipeline("weather", "alice", &enabled, 0, 25)
	match, _ := got[0]["$match"].(bson.M)
	assert.Equal(t, true, match["assistant.enabled"], "AssistantEnabled=true must add strict-equality filter")
}

func TestBuildSearchAppsPipeline_AssistantEnabledFalse(t *testing.T) {
	enabled := false
	got := buildSearchAppsPipeline("weather", "alice", &enabled, 0, 25)
	match, _ := got[0]["$match"].(bson.M)
	assert.Equal(t, false, match["assistant.enabled"], "AssistantEnabled=false must add strict-equality filter")
}

func TestBuildSearchAppsPipeline_LimitStagePresent(t *testing.T) {
	got := buildSearchAppsPipeline("weather", "alice", nil, 0, 42)
	// Walk the pipeline and find the $limit stage; assert it equals 42.
	var found bool
	for _, stage := range got {
		if n, ok := stage["$limit"]; ok {
			assert.EqualValues(t, 42, n, "$limit must equal the supplied limit")
			found = true
			break
		}
	}
	assert.True(t, found, "pipeline must contain a $limit stage")
}

func TestBuildSearchAppsPipeline_SkipStagePresent(t *testing.T) {
	got := buildSearchAppsPipeline("weather", "alice", nil, 20, 10)
	// Walk the pipeline and find the $skip stage; assert it equals 20.
	var found bool
	for _, stage := range got {
		if n, ok := stage["$skip"]; ok {
			assert.EqualValues(t, 20, n, "$skip must equal the supplied offset")
			found = true
			break
		}
	}
	assert.True(t, found, "pipeline must contain a $skip stage")
}
