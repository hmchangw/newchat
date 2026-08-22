package pagefit

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rows of a known encoded width: json.Marshal("aaa") is `"aaa"`, so a row of n
// characters costs n+2 bytes and the tests can name exact budgets.
func rows(n, width int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strings.Repeat("a", width)
	}
	return out
}

func encodedLen(t *testing.T, v any) int {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return len(b)
}

func TestNewBudget(t *testing.T) {
	tests := []struct {
		name        string
		brokerMax   int64
		reserve     int
		wantEnabled bool
		wantBytes   int
	}{
		{"typical", 128 << 10, 4096, true, (128 << 10) - 4096},
		{"no reserve", 1000, 0, true, 1000},
		{"broker cap unknown", 0, 4096, false, 0},
		{"broker cap negative", -1, 0, false, 0},
		{"reserve swallows the cap", 4096, 4096, false, 0},
		{"reserve exceeds the cap", 100, 4096, false, 0},
		{"negative reserve", 1000, -1, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBudget(tt.brokerMax, tt.reserve)
			assert.Equal(t, tt.wantEnabled, b.Enabled())
			assert.Equal(t, tt.wantBytes, b.Bytes())
		})
	}
}

func TestPrefix(t *testing.T) {
	// Each row encodes to 12 bytes; n rows cost 12n + (n-1) separators.
	const width = 10
	const rowCost = width + 2

	tests := []struct {
		name     string
		items    []string
		budget   Budget
		envelope int
		want     int
	}{
		{"disabled budget keeps everything", rows(5, width), Budget{}, 0, 5},
		{"empty slice", nil, NewBudget(1000, 0), 0, 0},
		{"all fit", rows(3, width), NewBudget(1000, 0), 0, 3},
		{"exact fit", rows(3, width), NewBudget(int64(3*rowCost+2), 0), 0, 3},
		{"one byte short drops the last", rows(3, width), NewBudget(int64(3*rowCost+1), 0), 0, 2},
		{"envelope consumes the budget", rows(3, width), NewBudget(int64(3*rowCost+2), 0), 10, 2},
		{"single row alone overflows still returns one", rows(4, width), NewBudget(1, 0), 0, 1},
		{"only the first row fits", rows(4, width), NewBudget(int64(rowCost), 0), 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Prefix(tt.items, tt.budget, tt.envelope))
		})
	}
}

// Forward progress is the whole point of the minimum: a caller that got 0 rows
// back with "more" set would have no position to page from.
func TestPrefix_NeverReturnsZeroForNonEmptyInput(t *testing.T) {
	for _, n := range []int{1, 2, 50} {
		assert.GreaterOrEqual(t, Prefix(rows(n, 4096), NewBudget(8, 0), 0), 1)
	}
}

func TestPrefix_ResultAlwaysFitsWhenAnyRowFits(t *testing.T) {
	items := rows(20, 10)
	b := NewBudget(100, 0)
	n := Prefix(items, b, 0)
	assert.LessOrEqual(t, encodedLen(t, items[:n]), b.Bytes()+2, "kept prefix must fit (array brackets aside)")
}

func TestWindow(t *testing.T) {
	const width = 10
	const rowCost = width + 2

	tests := []struct {
		name   string
		items  []string
		pivot  int
		budget Budget
		wantLo int
		wantHi int
	}{
		{"disabled budget keeps everything", rows(5, width), 2, Budget{}, 0, 5},
		{"empty slice", nil, 0, NewBudget(1000, 0), 0, 0},
		{"all fit", rows(5, width), 2, NewBudget(1000, 0), 0, 5},
		{"pivot only", rows(5, width), 2, NewBudget(int64(rowCost), 0), 2, 3},
		{"grows symmetrically around the pivot", rows(5, width), 2, NewBudget(int64(3*rowCost+2), 0), 1, 4},
		{"pivot at the start grows forward only", rows(5, width), 0, NewBudget(int64(2*rowCost+1), 0), 0, 2},
		{"pivot at the end grows backward only", rows(5, width), 4, NewBudget(int64(2*rowCost+1), 0), 3, 5},
		{"pivot below range is clamped", rows(3, width), -5, NewBudget(int64(rowCost), 0), 0, 1},
		{"pivot above range is clamped", rows(3, width), 99, NewBudget(int64(rowCost), 0), 2, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi := Window(tt.items, tt.pivot, tt.budget, 0)
			assert.Equal(t, tt.wantLo, lo, "lo")
			assert.Equal(t, tt.wantHi, hi, "hi")
		})
	}
}

// The pivot is the one row the caller cannot lose — it is the message they asked to centre on.
func TestWindow_AlwaysIncludesPivotEvenWhenItAloneOverflows(t *testing.T) {
	lo, hi := Window(rows(5, 4096), 3, NewBudget(8, 0), 0)
	assert.Equal(t, 3, lo)
	assert.Equal(t, 4, hi)
}

func BenchmarkPrefix_100Rows(bench *testing.B) {
	items := rows(100, 512)
	b := NewBudget(128<<10, 4096)
	bench.ReportAllocs()
	for bench.Loop() {
		_ = Prefix(items, b, 256)
	}
}

// A row that cannot be marshalled would break the response anyway; charge it
// as maximal so it is never silently counted as free and waved past a budget.
func TestPrefix_UnmarshalableRowIsChargedAsOversize(t *testing.T) {
	items := []any{"ok", make(chan int), "ok"}
	assert.Equal(t, 1, Prefix(items, NewBudget(1000, 0), 0))
}
