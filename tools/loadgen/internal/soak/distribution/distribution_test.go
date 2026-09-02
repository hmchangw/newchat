package distribution

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
)

func TestSoakDistribution_RoomPickerIsReproducibleAndHot(t *testing.T) {
	a, err := NewRoomPicker(42, 1000, DefaultRoomZipfS, DefaultRoomZipfV)
	require.NoError(t, err)
	b, err := NewRoomPicker(42, 1000, DefaultRoomZipfS, DefaultRoomZipfV)
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

func TestSoakDistribution_RoomPickerRejectsEmptyRoomSet(t *testing.T) {
	_, err := NewRoomPicker(1, 0, DefaultRoomZipfS, DefaultRoomZipfV)
	require.Error(t, err)
}

func TestSoakDistribution_PayloadSizerMatchesEncryptedMedianAndP95(t *testing.T) {
	sizer, err := NewPayloadSizer(42, 1024, 2048, 10240)
	require.NoError(t, err)

	const samples = 100000
	sizes := make([]int, samples)
	for i := range sizes {
		contentBytes := sizer.NextContentBytes()
		require.Positive(t, contentBytes)
		require.LessOrEqual(t, contentBytes, maxClientContentBytes)
		sizes[i] = modeledEncryptedPayloadBytes(contentBytes)
		require.LessOrEqual(t, sizes[i], 10240)
	}
	sort.Ints(sizes)

	assert.InDelta(t, 1024, percentileInt(sizes, 0.50), 80)
	assert.InDelta(t, 2048, percentileInt(sizes, 0.95), 180)
}

func TestSoakDistribution_ModeledEncryptedPayloadIncludesJSONAndGCMTag(t *testing.T) {
	const content = "plain-ascii"
	serialized, err := json.Marshal(atrest.EncryptedFields{Msg: content})
	require.NoError(t, err)

	assert.Equal(t, len(serialized)+gcmTagBytes, modeledEncryptedPayloadBytes(len(content)))
	assert.Equal(t, strings.Repeat("x", len(content)), ContentOfSize(len(content)))
}

func TestSoakDistribution_PayloadSizerRejectsGatekeeperOverflow(t *testing.T) {
	_, err := NewPayloadSizer(
		1,
		1024,
		2048,
		maxClientContentBytes+encryptedContentOverhead+1,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gatekeeper")
}

func TestSoakDistribution_PayloadSizerRejectsInvalidPercentileOrdering(t *testing.T) {
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewPayloadSizer(1, test.median, test.p95, test.max)
			require.Error(t, err)
		})
	}

	assert.Equal(t, gcmTagBytes+2, modeledEncryptedPayloadBytes(0))
	assert.Empty(t, ContentOfSize(-1))
}

func TestSoakDistribution_ThreadBudgetSamplerTargetsP99AndHardCap(t *testing.T) {
	sampler := NewThreadBudgetSampler(42)
	const samples = 100000
	budgets := make([]int, samples)
	for i := range budgets {
		budgets[i] = sampler.Next()
		require.Positive(t, budgets[i])
		require.LessOrEqual(t, budgets[i], ThreadReplyHardCap)
	}
	sort.Ints(budgets)

	assert.InDelta(t, threadReplyP99, percentileInt(budgets, 0.99), 5)
}

func TestSoakDistribution_ThreadBudgetNeverExceedsBudgetOrHardCap(t *testing.T) {
	budget := newThreadBudget(900)
	assert.Equal(t, ThreadReplyHardCap, budget.Limit())

	for range ThreadReplyHardCap {
		assert.True(t, budget.TryConsume())
	}
	assert.False(t, budget.TryConsume())
	assert.Equal(t, ThreadReplyHardCap, budget.Used())
}

func TestSoakDistribution_RoomPickerRejectsUnsupportedExponent(t *testing.T) {
	for _, exponent := range []float64{1.0, 0.8, 0} {
		_, err := NewRoomPicker(1, 100, exponent, 1.0)
		require.Error(t, err, "s=%v must be refused", exponent)
		assert.Contains(t, err.Error(), "greater than 1")
	}
}

func TestSoakDistribution_RoomPickerRejectsOffsetBelowOne(t *testing.T) {
	_, err := NewRoomPicker(1, 100, 1.2, 0.5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 1")
}

func TestSoakDistribution_RoomPickerAcceptsDefaults(t *testing.T) {
	picker, err := NewRoomPicker(1, 100, DefaultRoomZipfS, DefaultRoomZipfV)
	require.NoError(t, err)
	require.NotNil(t, picker)
	assert.Less(t, picker.Next(), 100)
}

func TestSoakDistribution_HigherRoomOffsetSpreadsTheLoad(t *testing.T) {
	const rooms, draws = 1000, 20000
	hot := func(offset float64) int {
		picker, err := NewRoomPicker(7, rooms, 1.2, offset)
		require.NoError(t, err)
		count := 0
		for range draws {
			if picker.Next() == 0 {
				count++
			}
		}
		return count
	}

	assert.Greater(t, hot(1.0), hot(10.0),
		"v=1 must concentrate more traffic on the hottest room than v=10")
}

func TestSoakDistribution_RoomZipfDefaultsMatchReplacedConstants(t *testing.T) {
	assert.Equal(t, 1.2, DefaultRoomZipfS)
	assert.Equal(t, 1.0, DefaultRoomZipfV)
}

func TestSoakDistribution_RoomPickerRejectsNonFiniteParameters(t *testing.T) {
	for name, test := range map[string]struct{ exponent, offset float64 }{
		"exponent NaN":  {math.NaN(), 1.0},
		"exponent +Inf": {math.Inf(1), 1.0},
		"offset NaN":    {1.2, math.NaN()},
		"offset +Inf":   {1.2, math.Inf(1)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewRoomPicker(1, 100, test.exponent, test.offset)
			require.Error(t, err)
		})
	}
}

func percentileInt(sorted []int, quantile float64) int {
	index := int(float64(len(sorted)-1) * quantile)
	return sorted[index]
}
