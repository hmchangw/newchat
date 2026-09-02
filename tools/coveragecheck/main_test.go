package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadCoverageProfile_FiltersAndAggregatesStatements(t *testing.T) {
	profile := strings.NewReader(`mode: atomic
github.com/hmchangw/chat/tools/loadgen/soak_catalog.go:10.1,12.2 3 1
github.com/hmchangw/chat/tools/loadgen/soak_catalog.go:14.1,16.2 2 0
github.com/hmchangw/chat/tools/loadgen/soak_main.go:20.1,21.2 4 1
github.com/hmchangw/chat/tools/loadgen/daily.go:30.1,31.2 10 1
`)

	stats, err := readCoverageProfile(
		profile,
		[]string{"tools/loadgen/soak_"},
		map[string]struct{}{"soak_main.go": {}},
	)

	require.NoError(t, err)
	assert.Equal(t, 3, stats.covered)
	assert.Equal(t, 5, stats.total)
	assert.InDelta(t, 60, stats.percent(), 0.001)
}

func TestReadCoverageProfile_MatchesAnyIncludePattern(t *testing.T) {
	profile := strings.NewReader(`mode: atomic
github.com/hmchangw/chat/tools/loadgen/soak_catalog.go:10.1,12.2 3 1
github.com/hmchangw/chat/tools/loadgen/internal/soak/userread/userread.go:14.1,16.2 2 1
github.com/hmchangw/chat/tools/loadgen/daily.go:30.1,31.2 10 1
`)

	stats, err := readCoverageProfile(
		profile,
		[]string{"tools/loadgen/soak_", "tools/loadgen/internal/soak/userread/"},
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, 5, stats.covered)
	assert.Equal(t, 5, stats.total)
}

func TestReadCoverageProfile_RejectsMalformedAndEmptyScope(t *testing.T) {
	_, err := readCoverageProfile(
		strings.NewReader("mode: atomic\nmalformed\n"),
		[]string{"soak_"},
		nil,
	)
	require.Error(t, err)

	_, err = readCoverageProfile(
		strings.NewReader("mode: atomic\nother.go:1.1,2.1 1 1\n"),
		[]string{"soak_"},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "matched no statements")
}

func TestCoverageStats_MeetsThreshold(t *testing.T) {
	stats := coverageStats{covered: 8, total: 10}
	assert.True(t, stats.meets(80))
	assert.False(t, stats.meets(80.1))
}

// A live include must not vouch for a dead one. Repeatable -include is how a
// phase that splits one prefix into several keeps measuring both; if a stale or
// misspelled sibling could ride along on the surviving pattern, the gate would
// report passing coverage for a scope it never looked at.
func TestReadCoverageProfile_RejectsAnIncludeThatMatchedNothing(t *testing.T) {
	profile := "mode: atomic\nsoak_read.go:1.1,2.1 4 1\n"

	stats, err := readCoverageProfile(
		strings.NewReader(profile),
		[]string{"soak_"},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 4, stats.total)

	_, err = readCoverageProfile(
		strings.NewReader(profile),
		[]string{"soak_", "internal/soak/userread/"},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "matched no statements")
	assert.Contains(t, err.Error(), "internal/soak/userread/")
	assert.NotContains(t, err.Error(), `"soak_,`)
}

// An include whose every match is excluded is a scoping mistake, not a 0%
// score. percent() would fail the threshold regardless; this keeps the message
// pointed at the cause.
func TestReadCoverageProfile_RejectsAScopeLeftEmptyByExcludes(t *testing.T) {
	_, err := readCoverageProfile(
		strings.NewReader("mode: atomic\nsoak_main.go:1.1,2.1 4 1\n"),
		[]string{"soak_"},
		map[string]struct{}{"soak_main.go": {}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "matched only excluded files")
}
