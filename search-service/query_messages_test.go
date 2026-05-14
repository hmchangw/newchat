package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func parseQuery(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func filterClauses(t *testing.T, q map[string]any) []any {
	t.Helper()
	query := q["query"].(map[string]any)
	b := query["bool"].(map[string]any)
	return b["filter"].([]any)
}

func shouldClauses(t *testing.T, q map[string]any) []any {
	t.Helper()
	filters := filterClauses(t, q)
	// filters = [range createdAt, bool { should: ... }]
	roomAccess := filters[1].(map[string]any)["bool"].(map[string]any)
	return roomAccess["should"].([]any)
}

func TestBuildMessageQuery_GlobalUnrestricted(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "hello", Size: 25, Offset: 0}
	raw, err := buildMessageQuery(req, "alice", nil, 365*24*time.Hour, "user-room")
	require.NoError(t, err)

	q := parseQuery(t, raw)
	assert.Equal(t, float64(0), q["from"])
	assert.Equal(t, float64(25), q["size"])
	assert.Equal(t, true, q["track_total_hits"])

	shoulds := shouldClauses(t, q)
	require.Len(t, shoulds, 1, "unrestricted-only → exactly one terms-lookup clause")
	terms := shoulds[0].(map[string]any)["terms"].(map[string]any)
	lookup := terms["roomId"].(map[string]any)
	assert.Equal(t, "user-room", lookup["index"])
	assert.Equal(t, "alice", lookup["id"])
	assert.Equal(t, "rooms", lookup["path"])
}

func TestBuildMessageQuery_GlobalWithRestricted(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "hi", Size: 10, Offset: 5}
	restricted := map[string]int64{
		"room-b": 1_700_000_000_000,
		"room-a": 1_600_000_000_000,
	}
	raw, err := buildMessageQuery(req, "alice", restricted, 24*time.Hour, "user-room")
	require.NoError(t, err)

	q := parseQuery(t, raw)
	shoulds := shouldClauses(t, q)
	// 1 terms-lookup + 2 clauses per restricted room (A + B) = 5
	require.Len(t, shoulds, 5)

	// deterministic ordering — alphabetical by rid, Clause A before Clause B
	clauseA := shoulds[1].(map[string]any)["bool"].(map[string]any)
	aMust := clauseA["must"].([]any)
	term := aMust[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "room-a", term["roomId"])
	// range.createdAt.gte is ISO8601 of the HSS
	rng := aMust[1].(map[string]any)["range"].(map[string]any)["createdAt"].(map[string]any)
	assert.Equal(t, time.UnixMilli(1_600_000_000_000).UTC().Format(time.RFC3339Nano), rng["gte"])

	// Clause B for room-a: roomId + createdAt >= hss (shared base must —
	// the outer security gate that prevents pre-HSS tshow replies from
	// leaking) + threadParent exists + (tshow=true OR parent.createdAt >= hss).
	clauseB := shoulds[2].(map[string]any)["bool"].(map[string]any)
	bMust := clauseB["must"].([]any)
	require.Len(t, bMust, 4, "clauseB must have term, createdAt-range, exists, inner-bool")
	assert.Equal(t, "room-a", bMust[0].(map[string]any)["term"].(map[string]any)["roomId"])

	// The reply itself must be at or after HSS.
	createdAtRange := bMust[1].(map[string]any)["range"].(map[string]any)["createdAt"].(map[string]any)
	assert.Equal(t, time.UnixMilli(1_600_000_000_000).UTC().Format(time.RFC3339Nano), createdAtRange["gte"])

	exists := bMust[2].(map[string]any)["exists"].(map[string]any)
	assert.Equal(t, "threadParentMessageId", exists["field"])

	innerBool := bMust[3].(map[string]any)["bool"].(map[string]any)
	assert.EqualValues(t, 1, innerBool["minimum_should_match"])
	innerShould := innerBool["should"].([]any)
	require.Len(t, innerShould, 2)
	assert.Equal(t, true, innerShould[0].(map[string]any)["term"].(map[string]any)["tshow"])
	parentRange := innerShould[1].(map[string]any)["range"].(map[string]any)["threadParentMessageCreatedAt"].(map[string]any)
	assert.Equal(t, time.UnixMilli(1_600_000_000_000).UTC().Format(time.RFC3339Nano), parentRange["gte"])
}

// inlineTermsAndLookup digs into the bool-filter wrapper that
// scopedAccessClauses now produces for the unrestricted subset. Returns
// the roomIds the inline terms clause matches and the account the
// terms-lookup gates against.
func inlineTermsAndLookup(t *testing.T, clause any) (roomIDs []any, account string) {
	t.Helper()
	filter := clause.(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	require.Len(t, filter, 2, "scoped unrestricted clause must be inline-terms AND terms-lookup")
	inline := filter[0].(map[string]any)["terms"].(map[string]any)["roomId"].([]any)
	lookup := filter[1].(map[string]any)["terms"].(map[string]any)["roomId"].(map[string]any)
	return inline, lookup["id"].(string)
}

func TestBuildMessageQuery_ScopedInlineTerms(t *testing.T) {
	req := model.SearchMessagesRequest{
		Query:   "hi",
		RoomIDs: []string{"r1", "r2", "r3"},
	}
	raw, err := buildMessageQuery(req, "alice", nil, time.Hour, "user-room")
	require.NoError(t, err)

	shoulds := shouldClauses(t, parseQuery(t, raw))
	require.Len(t, shoulds, 1)
	inline, account := inlineTermsAndLookup(t, shoulds[0])
	assert.ElementsMatch(t, []any{"r1", "r2", "r3"}, inline)
	assert.Equal(t, "alice", account, "inline terms must be gated by the user-room lookup")
}

func TestBuildMessageQuery_ScopedMixed(t *testing.T) {
	req := model.SearchMessagesRequest{
		Query:   "hi",
		RoomIDs: []string{"r1", "restricted-r2", "r3"},
	}
	restricted := map[string]int64{"restricted-r2": 1_600_000_000_000}
	raw, err := buildMessageQuery(req, "alice", restricted, time.Hour, "user-room")
	require.NoError(t, err)

	shoulds := shouldClauses(t, parseQuery(t, raw))
	require.Len(t, shoulds, 3) // 1 inline terms (r1, r3) + 2 restricted clauses (A + B)

	inline, account := inlineTermsAndLookup(t, shoulds[0])
	assert.ElementsMatch(t, []any{"r1", "r3"}, inline)
	assert.Equal(t, "alice", account)

	// Clause A for the restricted room
	clauseA := shoulds[1].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	assert.Equal(t, "restricted-r2", clauseA[0].(map[string]any)["term"].(map[string]any)["roomId"])
	// Clause A must explicitly exclude thread replies via must_not exists.
	clauseABool := shoulds[1].(map[string]any)["bool"].(map[string]any)
	mustNot := clauseABool["must_not"].([]any)
	require.Len(t, mustNot, 1)
	exists := mustNot[0].(map[string]any)["exists"].(map[string]any)
	assert.Equal(t, "threadParentMessageId", exists["field"])
}

func TestBuildMessageQuery_UserRoomIndexOverride(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "hi"}
	raw, err := buildMessageQuery(req, "alice", nil, time.Hour, "custom-user-room")
	require.NoError(t, err)

	shoulds := shouldClauses(t, parseQuery(t, raw))
	lookup := shoulds[0].(map[string]any)["terms"].(map[string]any)["roomId"].(map[string]any)
	assert.Equal(t, "custom-user-room", lookup["index"])
}

func TestBuildMessageQuery_ScopedAllRestricted(t *testing.T) {
	req := model.SearchMessagesRequest{
		Query:   "hi",
		RoomIDs: []string{"ra"},
	}
	restricted := map[string]int64{"ra": 1_700_000_000_000}
	raw, err := buildMessageQuery(req, "alice", restricted, time.Hour, "user-room")
	require.NoError(t, err)

	shoulds := shouldClauses(t, parseQuery(t, raw))
	require.Len(t, shoulds, 2) // Clause A + Clause B, no inline terms
	_, hasTermsLookup := shoulds[0].(map[string]any)["terms"]
	assert.False(t, hasTermsLookup)
}

func TestBuildMessageQuery_RecentWindow(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "hi"}
	raw, err := buildMessageQuery(req, "alice", nil, 48*time.Hour, "user-room")
	require.NoError(t, err)

	filters := filterClauses(t, parseQuery(t, raw))
	rng := filters[0].(map[string]any)["range"].(map[string]any)["createdAt"].(map[string]any)
	// Single-unit date-math required by ES — `48h`, NOT `48h0m0s`.
	assert.Equal(t, "now-48h", rng["gte"])
}

func TestBuildMessageQuery_RecentWindowDefault(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "hi"}
	raw, err := buildMessageQuery(req, "alice", nil, 0, "user-room")
	require.NoError(t, err)

	filters := filterClauses(t, parseQuery(t, raw))
	rng := filters[0].(map[string]any)["range"].(map[string]any)["createdAt"].(map[string]any)
	// Zero / negative window defaults to 1 year (365 * 24h = 8760h).
	assert.Equal(t, "now-8760h", rng["gte"])
}

func TestRecentWindowToGte_Units(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"hours", 8760 * time.Hour, "8760h"},
		{"minutes", 90 * time.Minute, "90m"},
		{"exact seconds", 45 * time.Second, "45s"},
		{"sub-second rounds up to seconds", 1500 * time.Millisecond, "2s"},
		{"fractional minute falls to seconds", 90*time.Second + 500*time.Millisecond, "91s"},
		{"zero defaults to 1y", 0, "8760h"},
		{"negative defaults to 1y", -time.Hour, "8760h"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, recentWindowToGte(tc.d))
		})
	}
}
