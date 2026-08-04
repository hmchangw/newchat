package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
)

// Compile-time check: searchWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*searchWorkload)(nil)

func TestParseSearchMix(t *testing.T) {
	mix, err := ParseSearchMix("messages:60,rooms:30,users:10")
	require.NoError(t, err)
	assert.Equal(t, 60, mix.Messages)
	assert.Equal(t, 30, mix.Rooms)
	assert.Equal(t, 10, mix.Users)
}

func TestParseSearchMix_Rejects(t *testing.T) {
	tests := []struct{ name, in string }{
		{"empty", ""},
		{"all zero", "messages:0,rooms:0,users:0"},
		{"unknown key", "messages:50,elasticsearch:50"},
		{"missing key", "messages:100"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSearchMix(tc.in)
			require.Error(t, err)
		})
	}
}

// The mix must pick each endpoint in proportion; otherwise the SLI is measured
// against a workload shape nobody agreed to.
func TestSearchMix_PicksInProportion(t *testing.T) {
	mix := SearchMix{Messages: 60, Rooms: 30, Users: 10}
	counts := map[searchEndpoint]int{}
	for i := range 100 {
		counts[mix.pick(i)]++
	}
	assert.Equal(t, 60, counts[searchMessages])
	assert.Equal(t, 30, counts[searchRooms])
	assert.Equal(t, 10, counts[searchUsers])
}

func TestBuildSearchInputs(t *testing.T) {
	c := newSearchCollector()
	for range 80 {
		c.Record(searchMessages, outcomeGood, 40*time.Millisecond)
	}
	for range 5 {
		c.Record(searchMessages, outcomeFailed, 0)
	}
	for range 12 {
		c.Record(searchRooms, outcomeGood, 20*time.Millisecond)
	}
	for range 30 {
		c.Record(searchUsers, outcomeExcluded, 0)
	}

	in := buildSearchInputs(500, 10*time.Second, c)

	// Excluded attempts leave the denominator (o11y-slo.md §0.1).
	assert.Equal(t, 97, in.AttemptedOps)
	assert.Equal(t, 5, in.FailedOps)
	// Search is synchronous request/reply — no JetStream consumer behind it.
	assert.Empty(t, in.Pending)

	// Each endpoint gates as its own latency series: an ES query over messages
	// and a Mongo-backed room lookup have different cost models, so one bound
	// across both would be meaningless.
	names := make([]string, 0, len(in.Latencies))
	for _, s := range in.Latencies {
		names = append(names, s.Name)
	}
	assert.ElementsMatch(t, []string{"messages", "rooms", "users"}, names)
}

func TestSearchCollector_OnlyTimesSuccesses(t *testing.T) {
	c := newSearchCollector()
	c.Record(searchMessages, outcomeGood, 30*time.Millisecond)
	c.Record(searchMessages, outcomeFailed, 5*time.Second)
	c.Record(searchMessages, outcomeExcluded, 5*time.Second)

	assert.Equal(t, []time.Duration{30 * time.Millisecond}, c.Samples(searchMessages))
}

func TestClassifySearchReply_OK(t *testing.T) {
	body, err := json.Marshal(map[string]any{"messages": []any{}, "total": 0})
	require.NoError(t, err)
	assert.Equal(t, outcomeGood, classifySearchReply(body, nil))
}

// A no-reply (request timeout) is a failure, never an absent sample — the same
// rule the messages workload now applies to dropped broadcasts.
func TestClassifySearchReply_TimeoutIsFailure(t *testing.T) {
	assert.Equal(t, outcomeFailed, classifySearchReply(nil, assert.AnError))
}

func TestClassifySearchReply_ErrorEnvelope(t *testing.T) {
	tests := []struct {
		code errcode.Code
		want sloOutcome
	}{
		{errcode.CodeBadRequest, outcomeExcluded},
		{errcode.CodeForbidden, outcomeExcluded},
		{errcode.CodeInternal, outcomeFailed},
		{errcode.CodeUnavailable, outcomeFailed},
		{errcode.CodeTooManyRequests, outcomeFailed},
	}
	for _, tc := range tests {
		t.Run(string(tc.code), func(t *testing.T) {
			body, err := json.Marshal(map[string]string{
				"code":  string(tc.code),
				"error": "boom",
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, classifySearchReply(body, nil))
		})
	}
}

func TestBuildSearchRequest_ShapesPerEndpoint(t *testing.T) {
	g := newSearchQueryGen(42)

	t.Run("messages", func(t *testing.T) {
		var req struct {
			Query string `json:"query"`
			Size  int    `json:"size"`
		}
		require.NoError(t, json.Unmarshal(g.build(searchMessages, 0), &req))
		assert.NotEmpty(t, req.Query)
		assert.Positive(t, req.Size)
	})

	t.Run("rooms carries a valid roomType", func(t *testing.T) {
		var req struct {
			Query    string `json:"query"`
			RoomType string `json:"roomType"`
		}
		require.NoError(t, json.Unmarshal(g.build(searchRooms, 0), &req))
		assert.NotEmpty(t, req.Query)
		// "app" and unknown values are rejected with bad_request by the service.
		assert.Contains(t, []string{"", "all", "channel", "dm"}, req.RoomType)
	})

	t.Run("users uses limit not size", func(t *testing.T) {
		var req struct {
			Query string `json:"query"`
			Limit int    `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(g.build(searchUsers, 0), &req))
		assert.NotEmpty(t, req.Query)
		assert.Positive(t, req.Limit)
	})
}

// A query that is always the same term would be answered from ES's query cache
// after the first hit and measure nothing.
func TestSearchQueryGen_VariesQueries(t *testing.T) {
	g := newSearchQueryGen(42)
	seen := map[string]struct{}{}
	for i := range 50 {
		var req struct {
			Query string `json:"query"`
		}
		require.NoError(t, json.Unmarshal(g.build(searchMessages, i), &req))
		seen[req.Query] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "queries must vary across requests")
}

func TestDefaultSteps_SearchMatchesWorkloadModel(t *testing.T) {
	// R_search is ~5 ops/user/day, far below the message path.
	assert.Equal(t, "100,200,500,1000,2000", defaultSteps("search"))
}

func TestSearchSubjectFor(t *testing.T) {
	assert.Equal(t, "chat.user.alice.request.search.site-local.messages",
		searchSubjectFor(searchMessages, "alice", "site-local"))
	assert.Equal(t, "chat.user.alice.request.search.site-local.rooms",
		searchSubjectFor(searchRooms, "alice", "site-local"))
	assert.Equal(t, "chat.user.alice.request.search.site-local.users",
		searchSubjectFor(searchUsers, "alice", "site-local"))
}
