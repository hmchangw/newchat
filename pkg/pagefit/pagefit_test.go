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
	const width = 10
	// Budgets are derived from real encoded sizes, so the table states intent
	// rather than re-deriving the package's own separator/bracket arithmetic.
	exactly := func(n int) Budget { return NewBudget(int64(encodedSize(t, rows(n, width))), 0) }

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
		{"exact fit", rows(3, width), exactly(3), 0, 3},
		{"one byte short drops the last", rows(3, width), NewBudget(int64(encodedSize(t, rows(3, width))-1), 0), 0, 2},
		{"envelope consumes the budget", rows(3, width), exactly(3), 10, 2},
		{"single row alone overflows still returns one", rows(4, width), NewBudget(1, 0), 0, 1},
		{"only the first row fits", rows(4, width), exactly(1), 0, 1},
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
		kept, oversize, _ := Fit(rows(n, 4096), NewBudget(8, 0), 0)
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
			kept, oversize, _ := Fit(tt.items, tt.budget, 0)
			assert.Equal(t, tt.wantDropped, len(kept) < len(tt.items), "dropped")
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
	exactly := func(n int) Budget { return NewBudget(int64(encodedSize(t, rows(n, width))), 0) }

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
		{"pivot only", rows(5, width), 2, exactly(1), 2, 3},
		{"grows symmetrically around the pivot", rows(5, width), 2, exactly(3), 1, 4},
		{"pivot at the start grows forward only", rows(5, width), 0, exactly(2), 0, 2},
		{"pivot at the end grows backward only", rows(5, width), 4, exactly(2), 3, 5},
		{"pivot below range is clamped", rows(3, width), -5, exactly(1), 0, 1},
		{"pivot above range is clamped", rows(3, width), 99, exactly(1), 2, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi, _, _ := FitWindow(tt.items, tt.pivot, tt.budget, 0)
			assert.Equal(t, tt.wantLo, lo, "lo")
			assert.Equal(t, tt.wantHi, hi, "hi")
		})
	}
}

// The pivot is the one row the caller cannot lose — it is the message they asked to centre on.
func TestFitWindow_AlwaysIncludesPivotEvenWhenItAloneOverflows(t *testing.T) {
	lo, hi, oversize, _ := FitWindow(rows(5, 4096), 3, NewBudget(8, 0), 0)
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

// A page that fits must never be reported as dropped or oversize.
func TestFit_WholePageFastPath(t *testing.T) {
	const width = 10
	exactly := func(n int) Budget { return NewBudget(int64(encodedSize(t, rows(n, width))), 0) }

	tests := []struct {
		name     string
		items    []string
		budget   Budget
		envelope int
		want     bool
	}{
		{"disabled budget always fits", rows(3, width), Budget{}, 0, true},
		{"empty always fits", nil, NewBudget(1, 0), 0, true},
		{"exact fit", rows(1, width), exactly(1), 0, true},
		{"one byte over", rows(1, width), NewBudget(int64(encodedSize(t, rows(1, width))-1), 0), 0, false},
		{"envelope tips it over", rows(1, width), exactly(1), 1, false},
		{"multiple rows fit", rows(3, width), NewBudget(1000, 0), 0, true},
		{"multiple rows do not fit", rows(3, width), exactly(1), 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, oversize, _ := Fit(tt.items, tt.budget, tt.envelope)
			if tt.want {
				assert.Len(t, kept, len(tt.items))
				assert.False(t, oversize)
			} else {
				assert.True(t, len(kept) < len(tt.items) || oversize, "an overflowing page must report why")
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

// assembleSurrounding derives the pivot as len(beforePage) — which is one past
// the end when there is no central row and nothing after it. Callers rely on
// the clamp rather than guarding, so pin it.
func TestFitWindow_PivotOnePastTheEndIsClamped(t *testing.T) {
	items := rows(3, 10)
	lo, hi, oversize, _ := FitWindow(items, len(items), NewBudget(1<<20, 0), 0)
	assert.Equal(t, 0, lo)
	assert.Equal(t, 3, hi)
	assert.False(t, oversize)
}

// An override must never raise the ceiling above what the broker will accept —
// a reply sized to it would be refused on the wire.
func TestResolve_OverrideNeverExceedsTheBrokerCap(t *testing.T) {
	tests := []struct {
		name      string
		override  int64
		brokerMax int64
		wantBytes int
	}{
		{"override below broker wins", 50_000, 128 << 10, 50_000},
		{"override above broker is clamped to broker", 10 << 20, 128 << 10, 128 << 10},
		{"override equal to broker", 1000, 1000, 1000},
		{"no broker cap known, override honoured", 50_000, 0, 50_000},
		{"no override falls back to broker", 0, 10_000, 10_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantBytes, Resolve(tt.override, tt.brokerMax, 0).Bytes())
		})
	}
}

// A row that will not encode is a server fault, not a too-large row. Reporting
// it as oversize would blank it, mark it truncated, and return success — hiding
// the failure and skipping the row for good.
func TestFit_MarshalFailureIsAnErrorNotAnOversizeRow(t *testing.T) {
	_, oversize, err := Fit([]any{"ok", make(chan int)}, NewBudget(8, 0), 0)
	require.Error(t, err)
	assert.False(t, oversize, "an encoding failure must not masquerade as truncation")
}

func TestFitWindow_MarshalFailureIsAnErrorNotAnOversizeRow(t *testing.T) {
	_, _, oversize, err := FitWindow([]any{"ok", make(chan int), "ok"}, 1, NewBudget(8, 0), 0)
	require.Error(t, err)
	assert.False(t, oversize)
}

func TestFit_NoErrorWhenEverythingEncodes(t *testing.T) {
	_, _, err := Fit(rows(5, 10), NewBudget(1000, 0), 0)
	assert.NoError(t, err)
}

// sizeRow is a row with the fields most likely to make a hand-rolled size
// calculation disagree with the real encoder: HTML metacharacters, multi-byte
// runes, pre-encoded JSON, and nested containers.
type sizeRow struct {
	Text   string          `json:"text"`
	Raw    json.RawMessage `json:"raw,omitempty"`
	List   []string        `json:"list,omitempty"`
	Lookup map[string]int  `json:"lookup,omitempty"`
}

// rowSizes derives every width and the array total from one streaming encode
// rather than marshalling the slice. That arithmetic is only safe while it
// agrees byte-for-byte with what the router will actually marshal — this pins
// the two together so an escaping or separator change cannot drift silently.
func TestRowSizes_AgreesWithJSONMarshal(t *testing.T) {
	wide := make([]sizeRow, 50)
	for i := range wide {
		wide[i] = sizeRow{Text: strings.Repeat("z", i)}
	}
	tests := map[string][]sizeRow{
		"empty":            {},
		"single":           {{Text: "x"}},
		"pair":             {{Text: "x"}, {Text: "yy"}},
		"html metachars":   {{Text: "<a href='x'>&</a>"}, {Text: "plain"}},
		"multi-byte runes": {{Text: "日本語 — 🎉"}, {Text: strings.Repeat("é", 50)}},
		"raw json":         {{Text: "x", Raw: json.RawMessage(`{"nested":[1,2,3]}`)}},
		"containers":       {{Text: "x", List: []string{"p", "q"}, Lookup: map[string]int{"k": 1}}},
		"fifty rows":       wide,
	}

	for name, items := range tests {
		t.Run(name, func(t *testing.T) {
			sizes, total, err := rowSizes(items)
			require.NoError(t, err)

			whole, err := json.Marshal(items)
			require.NoError(t, err)
			assert.Equal(t, len(whole), total, "array total must equal the encoded array")

			for i, item := range items {
				row, err := json.Marshal(item)
				require.NoError(t, err)
				assert.Equal(t, len(row), sizes[i], "row %d width", i)
			}
		})
	}
}

// The trim loop reuses those widths to decide where to cut, so the kept slice
// must both fit the budget and be the longest prefix that does.
func TestFit_KeptPrefixIsWithinBudgetAndMaximal(t *testing.T) {
	items := make([]sizeRow, 30)
	for i := range items {
		items[i] = sizeRow{Text: strings.Repeat("q", 10+i*7)}
	}

	for budget := 20; budget < 900; budget += 7 {
		kept, oversize, err := Fit(items, NewBudget(int64(budget), 0), 0)
		require.NoError(t, err)
		if oversize {
			require.Len(t, kept, 1, "budget %d: an oversize page keeps exactly one row", budget)
			continue
		}

		encoded, err := json.Marshal(kept)
		require.NoError(t, err)
		require.LessOrEqual(t, len(encoded), budget, "budget %d: kept %d rows over budget", budget, len(kept))

		if len(kept) < len(items) {
			next, err := json.Marshal(items[:len(kept)+1])
			require.NoError(t, err)
			assert.Greater(t, len(next), budget,
				"budget %d: kept %d rows but one more would also have fit", budget, len(kept))
		}
	}
}
