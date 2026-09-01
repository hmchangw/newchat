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
