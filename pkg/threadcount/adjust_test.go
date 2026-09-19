package threadcount

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeepNewer(t *testing.T) {
	early := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	late := early.Add(time.Minute)

	tests := []struct {
		name    string
		saved   *time.Time
		replyAt *time.Time
		want    *time.Time
	}{
		{name: "nothing saved takes the reply's time", saved: nil, replyAt: &late, want: &late},
		{name: "a newer reply moves it forward", saved: &early, replyAt: &late, want: &late},
		{name: "an older reply never drags it back", saved: &late, replyAt: &early, want: &late},
		{name: "the same time keeps it", saved: &late, replyAt: &late, want: &late},
		{name: "no reply time leaves the saved one alone", saved: &late, replyAt: nil, want: &late},
		{name: "no reply time and nothing saved stays empty", saved: nil, replyAt: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := keepNewer(tt.saved, tt.replyAt)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, *tt.want, *got)
		})
	}
}

// The property that keeps re-anchoring affordable: the chance of anchoring
// falls as the thread grows, so the EXPECTED rows scanned per reply stays flat
// at DefaultReanchorBudget no matter how long the thread gets. A fixed "every Nth
// reply" rule would instead cost O(thread length) per reply, amortized.
func TestShouldReanchor_ExpectedRowsPerReplyIsFlat(t *testing.T) {
	// The anchor count is binomial, so its spread is set by how many anchors
	// are expected, not by how many samples are drawn: sampling a fixed number
	// of times gives the longest thread the fewest anchors and the widest
	// relative error. Scaling samples with `stamped` holds the expectation at
	// anchorsPerCase for every case, which puts the 15% band at ~4.7 sigma —
	// about a one-in-a-million false failure instead of one in eight.
	const anchorsPerCase = 1000
	for _, stamped := range []int{1000, 10000, 100000} {
		samples := anchorsPerCase * stamped / DefaultReanchorBudget
		anchors := 0
		for i := 0; i < samples; i++ {
			if ShouldReanchor(stamped, DefaultReanchorBudget) {
				anchors++
			}
		}
		// Each anchor scans `stamped` rows, so rows/reply = rate * stamped.
		rowsPerReply := float64(anchors) / float64(samples) * float64(stamped)
		assert.InEpsilonf(t, float64(DefaultReanchorBudget), rowsPerReply, 0.15,
			"stamped=%d: expected ~%d rows/reply, got %.1f", stamped, DefaultReanchorBudget, rowsPerReply)
	}
}

func TestShouldReanchor_Edges(t *testing.T) {
	assert.False(t, ShouldReanchor(0, DefaultReanchorBudget), "an unstamped parent has nothing to re-anchor from")
	assert.False(t, ShouldReanchor(-1, DefaultReanchorBudget), "a negative count is nonsense, not a trigger")
	// At or under the budget a full scan is cheaper than the sampling it would
	// replace, so it always runs.
	assert.True(t, ShouldReanchor(1, DefaultReanchorBudget))
	assert.True(t, ShouldReanchor(DefaultReanchorBudget, DefaultReanchorBudget))
	assert.False(t, ShouldReanchor(1000, 0), "a zero budget disables re-anchoring outright")
}
