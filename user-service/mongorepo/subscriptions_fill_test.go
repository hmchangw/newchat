package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// fillBatchSize sizes a refill from how many rows are still missing, not the
// whole page: a page that lost one row tops up with a small read.
func TestFillBatchSize(t *testing.T) {
	tests := []struct {
		name       string
		need       int64
		collected  int64
		candidates int64
		want       int64
	}{
		{
			name: "first round of a large page reads the whole over-read",
			need: 1001, collected: 0, candidates: 5000, want: 1001,
		},
		{
			name: "refill after one dropped row tops up at the floor, not a second full page",
			need: 1001, collected: 1000, candidates: 4000, want: minFillBatch,
		},
		{
			name: "refill with a wide gap reads only the remainder",
			need: 1001, collected: 100, candidates: 4000, want: 901,
		},
		{
			name: "small page is floored so a dead prefix costs one read, not one per row",
			need: 6, collected: 0, candidates: 100, want: minFillBatch,
		},
		{
			name: "degenerate limit 0 still reads a floor-sized batch",
			need: 1, collected: 0, candidates: 100, want: minFillBatch,
		},
		{
			name: "never reads past the remaining candidates",
			need: 1001, collected: 0, candidates: 10, want: 10,
		},
		{
			name: "floor is clamped by a short candidate tail",
			need: 6, collected: 0, candidates: 3, want: 3,
		},
		{
			name: "exact mid-size page reads exactly the over-read",
			need: 40, collected: 0, candidates: 100, want: 40,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, fillBatchSize(tt.need, tt.collected, tt.candidates))
		})
	}
}
