package main

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
)

func TestSoakRoomPicker_IsReproducibleAndHot(t *testing.T) {
	a, err := newSoakRoomPicker(42, 1000, soakDefaultRoomZipfS, soakDefaultRoomZipfV)
	require.NoError(t, err)
	b, err := newSoakRoomPicker(42, 1000, soakDefaultRoomZipfS, soakDefaultRoomZipfV)
	require.NoError(t, err)

	const samples = 20000
	sequenceA := make([]int, samples)
	sequenceB := make([]int, samples)
	hot := 0
	for i := range samples {
		sequenceA[i] = a.Next()
		sequenceB[i] = b.Next()
		require.Less(t, sequenceA[i], 1000)
		if sequenceA[i] < 100 {
			hot++
		}
	}

	assert.Equal(t, sequenceA, sequenceB)
	assert.Greater(t, float64(hot)/samples, 0.60)
}

func TestNewSoakRoomPicker_RejectsEmptyRoomSet(t *testing.T) {
	_, err := newSoakRoomPicker(1, 0, soakDefaultRoomZipfS, soakDefaultRoomZipfV)
	require.Error(t, err)
}

func TestSoakPayloadSizer_MatchesEncryptedMedianAndP95(t *testing.T) {
	sizer, err := newSoakPayloadSizer(42, 1024, 2048, 10240)
	require.NoError(t, err)

	const samples = 100000
	sizes := make([]int, samples)
	for i := range sizes {
		contentBytes := sizer.NextContentBytes()
		require.Positive(t, contentBytes)
		require.LessOrEqual(t, contentBytes, soakMaxClientContentBytes)
		sizes[i] = modeledEncryptedPayloadBytes(contentBytes)
		require.LessOrEqual(t, sizes[i], 10240)
	}
	sort.Ints(sizes)

	assert.InDelta(t, 1024, percentileInt(sizes, 0.50), 80)
	assert.InDelta(t, 2048, percentileInt(sizes, 0.95), 180)
}

func TestModeledEncryptedPayloadBytes_IncludesJSONAndGCMTag(t *testing.T) {
	const content = "plain-ascii"
	serialized, err := json.Marshal(atrest.EncryptedFields{Msg: content})
	require.NoError(t, err)

	assert.Equal(t, len(serialized)+soakGCMTagBytes, modeledEncryptedPayloadBytes(len(content)))
	assert.Equal(t, strings.Repeat("x", len(content)), soakContentOfSize(len(content)))
}

func TestNewSoakPayloadSizer_RejectsGatekeeperOverflow(t *testing.T) {
	_, err := newSoakPayloadSizer(
		1,
		1024,
		2048,
		soakMaxClientContentBytes+soakEncryptedContentOverhead+1,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gatekeeper")
}

func TestNewSoakPayloadSizer_RejectsInvalidPercentileOrdering(t *testing.T) {
	tests := []struct {
		name   string
		median int
		p95    int
		max    int
	}{
		{
			name: "median does not exceed encryption overhead",
			p95:  2048,
			max:  4096,
		},
		{
			name:   "p95 below median",
			median: 2048,
			p95:    1024,
			max:    4096,
		},
		{
			name:   "maximum below p95",
			median: 1024,
			p95:    2048,
			max:    1500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newSoakPayloadSizer(1, tt.median, tt.p95, tt.max)
			require.Error(t, err)
		})
	}

	assert.Equal(t, soakGCMTagBytes+2, modeledEncryptedPayloadBytes(0))
	assert.Empty(t, soakContentOfSize(-1))
}

func TestSoakThreadBudgetSampler_TargetsP99AndHardCap(t *testing.T) {
	sampler := newSoakThreadBudgetSampler(42)
	const samples = 100000
	budgets := make([]int, samples)
	for i := range budgets {
		budgets[i] = sampler.Next()
		require.Positive(t, budgets[i])
		require.LessOrEqual(t, budgets[i], soakThreadReplyHardCap)
	}
	sort.Ints(budgets)

	assert.InDelta(t, soakThreadReplyP99, percentileInt(budgets, 0.99), 5)
}

func TestSoakThreadBudget_NeverExceedsBudgetOrHardCap(t *testing.T) {
	budget := newSoakThreadBudget(900)
	assert.Equal(t, soakThreadReplyHardCap, budget.Limit())

	for range soakThreadReplyHardCap {
		assert.True(t, budget.TryConsume())
	}
	assert.False(t, budget.TryConsume())
	assert.Equal(t, soakThreadReplyHardCap, budget.Used())
}

func percentileInt(sorted []int, quantile float64) int {
	index := int(float64(len(sorted)-1) * quantile)
	return sorted[index]
}
