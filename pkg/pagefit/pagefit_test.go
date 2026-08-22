package pagefit

import (
	"encoding/json"
	"math"
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

func encodedSize(t *testing.T, v any) int {
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

func TestFit(t *testing.T) {
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
			kept, _, _ := Fit(tt.items, tt.budget, tt.envelope)
			assert.Len(t, kept, tt.want)
		})
	}
}

// Forward progress is the whole point of the minimum: a caller that got 0 rows
// back with "more" set would have no position to page from.
func TestFit_NeverReturnsZeroRowsForNonEmptyInput(t *testing.T) {
	for _, n := range []int{1, 2, 50} {
		kept, _, oversize := Fit(rows(n, 4096), NewBudget(8, 0), 0)
		assert.Len(t, kept, 1)
		assert.True(t, oversize, "the caller must be told the kept row still overflows")
	}
}

// dropped drives the caller's "more" flag; oversize drives whether the kept row
// needs degrading. Conflating them loses one of the two decisions.
func TestFit_ReportsDroppedAndOversizeSeparately(t *testing.T) {
	tests := []struct {
		name         string
		items        []string
		budget       Budget
		wantDropped  bool
		wantOversize bool
	}{
		{"everything fits", rows(3, 10), NewBudget(1000, 0), false, false},
		{"some rows dropped", rows(10, 10), NewBudget(40, 0), true, false},
		{"lone row overflows", rows(1, 4096), NewBudget(16, 0), false, true},
		{"lone row overflows with others behind it", rows(5, 4096), NewBudget(16, 0), true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, dropped, oversize := Fit(tt.items, tt.budget, 0)
			assert.Equal(t, tt.wantDropped, dropped, "dropped")
			assert.Equal(t, tt.wantOversize, oversize, "oversize")
		})
	}
}

func TestFit_ResultAlwaysFitsWhenAnyRowFits(t *testing.T) {
	items := rows(20, 10)
	b := NewBudget(100, 0)
	kept, _, _ := Fit(items, b, 0)
	assert.LessOrEqual(t, encodedSize(t, kept), b.Bytes()+2, "kept prefix must fit (array brackets aside)")
}

func TestFitWindow(t *testing.T) {
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
			lo, hi, _ := FitWindow(tt.items, tt.pivot, tt.budget, 0)
			assert.Equal(t, tt.wantLo, lo, "lo")
			assert.Equal(t, tt.wantHi, hi, "hi")
		})
	}
}

// The pivot is the one row the caller cannot lose — it is the message they asked to centre on.
func TestFitWindow_AlwaysIncludesPivotEvenWhenItAloneOverflows(t *testing.T) {
	lo, hi, oversize := FitWindow(rows(5, 4096), 3, NewBudget(8, 0), 0)
	assert.Equal(t, 3, lo)
	assert.Equal(t, 4, hi)
	assert.True(t, oversize, "the caller must be told the pivot alone overflows")
}

// The common case: a page that fits must not pay the per-row marshal.
func BenchmarkFit_100RowsThatFit(bench *testing.B) {
	items := rows(100, 512)
	b := NewBudget(128<<10, 4096)
	bench.ReportAllocs()
	for bench.Loop() {
		_, _, _ = Fit(items, b, 256)
	}
}

func BenchmarkFit_100RowsTrimmed(bench *testing.B) {
	items := rows(100, 512)
	b := NewBudget(8<<10, 0)
	bench.ReportAllocs()
	for bench.Loop() {
		_, _, _ = Fit(items, b, 256)
	}
}

// A row that cannot be marshalled would break the response anyway; charge it
// as maximal so it is never silently counted as free and waved past a budget.
func TestFit_UnmarshalableRowIsChargedAsOversize(t *testing.T) {
	items := []any{"ok", make(chan int), "ok"}
	kept, dropped, _ := Fit(items, NewBudget(1000, 0), 0)
	assert.Len(t, kept, 1)
	assert.True(t, dropped)
}

// A page that fits must never be reported as dropped or oversize.
func TestFit_WholePageFastPath(t *testing.T) {
	const width = 10
	const rowCost = width + 2

	tests := []struct {
		name     string
		items    []string
		budget   Budget
		envelope int
		want     bool
	}{
		{"disabled budget always fits", rows(3, width), Budget{}, 0, true},
		{"empty always fits", nil, NewBudget(1, 0), 0, true},
		{"exact fit", rows(1, width), NewBudget(int64(rowCost), 0), 0, true},
		{"one byte over", rows(1, width), NewBudget(int64(rowCost-1), 0), 0, false},
		{"envelope tips it over", rows(1, width), NewBudget(int64(rowCost), 0), 1, false},
		{"multiple rows fit", rows(3, width), NewBudget(1000, 0), 0, true},
		{"multiple rows do not fit", rows(3, width), NewBudget(int64(rowCost), 0), 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, dropped, oversize := Fit(tt.items, tt.budget, tt.envelope)
			if tt.want {
				assert.Len(t, kept, len(tt.items))
				assert.False(t, dropped)
				assert.False(t, oversize)
			} else {
				assert.True(t, dropped || oversize, "an overflowing page must report why")
			}
		})
	}
}

// Resolve is the startup rule shared by every service: an explicit override
// wins, otherwise the broker's advertised cap is used.
func TestResolve(t *testing.T) {
	tests := []struct {
		name      string
		override  int64
		brokerMax int64
		reserve   int
		wantBytes int
	}{
		{"override wins", 50_000, 128 << 10, 0, 50_000},
		{"falls back to the broker cap", 0, 10_000, 0, 10_000},
		{"negative override falls back", -5, 10_000, 0, 10_000},
		{"reserve applies to the broker cap", 0, 10_000, 1_000, 9_000},
		{"reserve applies to the override", 20_000, 128 << 10, 1_000, 19_000},
		{"neither known disables trimming", 0, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantBytes, Resolve(tt.override, tt.brokerMax, tt.reserve).Bytes())
		})
	}
}

// An unmarshalable row is charged MaxInt32. Summing that into the running total
// overflows int on a 32-bit build and flips the comparison, so the row would
// read as fitting. These pin the subtraction-based comparisons that avoid it.
func TestFit_OversizeChargeNeverWrapsAround(t *testing.T) {
	bad := make(chan int)

	t.Run("unmarshalable first row is still charged", func(t *testing.T) {
		kept, _, oversize := Fit([]any{bad, "ok"}, NewBudget(math.MaxInt32, 0), 0)
		assert.Len(t, kept, 1)
		assert.True(t, oversize, "an unmarshalable row must never be counted as fitting")
	})

	t.Run("unmarshalable row stops the prefix", func(t *testing.T) {
		kept, dropped, _ := Fit([]any{"ok", bad, "ok"}, NewBudget(math.MaxInt32, 0), 0)
		assert.Len(t, kept, 1, "the scan must stop at the row it cannot size")
		assert.True(t, dropped)
	})

	t.Run("unmarshalable pivot is reported oversize", func(t *testing.T) {
		lo, hi, oversize := FitWindow([]any{"ok", bad, "ok"}, 1, NewBudget(math.MaxInt32, 0), 0)
		assert.Equal(t, 1, lo)
		assert.Equal(t, 2, hi)
		assert.True(t, oversize)
	})
}

// assembleSurrounding derives the pivot as len(beforePage) — which is one past
// the end when there is no central row and nothing after it. Callers rely on
// the clamp rather than guarding, so pin it.
func TestFitWindow_PivotOnePastTheEndIsClamped(t *testing.T) {
	items := rows(3, 10)
	lo, hi, oversize := FitWindow(items, len(items), NewBudget(1<<20, 0), 0)
	assert.Equal(t, 0, lo)
	assert.Equal(t, 3, hi)
	assert.False(t, oversize)
}
