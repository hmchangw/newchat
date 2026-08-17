package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestGetPath(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": "x"}, "top": 1}
	v, ok := getPath(doc, "a.b")
	assert.True(t, ok)
	assert.Equal(t, "x", v)

	v, ok = getPath(doc, "top")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	_, ok = getPath(doc, "a.missing")
	assert.False(t, ok)
	_, ok = getPath(doc, "missing.b")
	assert.False(t, ok)

	// "top" is a scalar (not a map), so continuing the walk into "top.b" must
	// fail the type assertion rather than panic.
	_, ok = getPath(doc, "top.b")
	assert.False(t, ok)
}

func TestNormalize(t *testing.T) {
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"int", int(5), float64(5)},
		{"int8", int8(5), float64(5)},
		{"int16", int16(5), float64(5)},
		{"int32", int32(5), float64(5)},
		{"int64", int64(5), float64(5)},
		{"uint", uint(5), float64(5)},
		{"uint8", uint8(5), float64(5)},
		{"uint16", uint16(5), float64(5)},
		{"uint32", uint32(5), float64(5)},
		{"uint64", uint64(5), float64(5)},
		{"float32", float32(5.5), float64(float32(5.5))},
		{"time.Time", ts, float64(ts.UnixMilli())},
		{"[]byte", []byte("x"), "x"},
		{"string passthrough", "x", "x"},
		{"nested slice", []any{int64(1), "a"}, []any{float64(1), "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalize(tt.in))
		})
	}
}

func TestValuesEqual(t *testing.T) {
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"int32 vs int64", int32(5), int64(5), true},
		{"int vs float64", 5, float64(5), true},
		{"time vs unix ms", ts, ts.UnixMilli(), true},
		{"bytes vs string", []byte("x"), "x", true},
		{"nil vs nil", nil, nil, true},
		{"string mismatch", "a", "b", false},
		{"nil vs value", nil, "a", false},
		{"nested map equal", map[string]any{"k": int64(1)}, map[string]any{"k": float64(1)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, valuesEqual(tt.a, tt.b))
		})
	}
}

func TestIsZeroValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{"nil", nil, true},
		{"bool false", false, true},
		{"bool true", true, false},
		{"float64 zero", float64(0), true},
		{"float64 nonzero", float64(1), false},
		{"empty string", "", true},
		{"non-empty string", "x", false},
		{"unhandled type slice", []any{}, false},
		{"unhandled type map", map[string]any{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isZeroValue(tt.in))
		})
	}
}

func TestDiffFields(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	src := map[string]any{"msg": "hello", "ts": ts, "u": map[string]any{"_id": "u1"}}
	dst := map[string]any{"body": "hello", "created_at": ts.UnixMilli(), "sender_account": "u1"}

	pairs := []fieldPair{
		{SourcePaths: []string{"msg"}, DestField: "body"},
		{SourcePaths: []string{"ts"}, DestField: "created_at", Transform: "unixMilli"},
		{SourcePaths: []string{"u._id"}, DestField: "sender_account"},
	}
	assert.Empty(t, diffFields(src, dst, pairs, reg))

	dst["body"] = "tampered"
	diffs := diffFields(src, dst, pairs, reg)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "msg", diffs[0].SourcePath)
	assert.Equal(t, "hello", diffs[0].Want)
	assert.Equal(t, "tampered", diffs[0].Got)
}

func TestDiffFields_AbsentSemantics(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{}
	dst := map[string]any{}

	optional := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "gone"}}
	assert.Empty(t, diffFields(src, dst, optional, reg))

	required := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "gone", Required: true}}
	diffs := diffFields(src, dst, required, reg)
	assert.Len(t, diffs, 1)
	assert.Contains(t, diffs[0].Cause, "required")
}

func TestDiffFields_TransformError(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{"ts": "not-a-time"}
	dst := map[string]any{"created_at": int64(1)}
	diffs := diffFields(src, dst, []fieldPair{{SourcePaths: []string{"ts"}, DestField: "created_at", Transform: "unixMilli"}}, reg)
	assert.Len(t, diffs, 1)
	assert.Contains(t, diffs[0].Cause, "transform-error")
}

func TestDiffFields_ZeroValueSemantics(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{}

	tests := []struct {
		name      string
		dst       any
		required  bool
		wantDiffs int
	}{
		{"optional absent-source vs dest false", false, false, 0},
		{"optional absent-source vs dest zero int", 0, false, 0},
		{"optional absent-source vs dest empty string", "", false, 0},
		{"optional absent-source vs dest true", true, false, 1},
		{"optional absent-source vs dest nonzero int", 1, false, 1},
		{"optional absent-source vs dest nonempty string", "x", false, 1},
		{"required absent-source vs dest false", false, true, 1},
		{"required absent-source vs dest zero int", 0, true, 1},
		{"required absent-source vs dest empty string", "", true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := map[string]any{"field": tt.dst}
			pairs := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "field", Required: tt.required}}
			diffs := diffFields(src, dst, pairs, reg)
			assert.Len(t, diffs, tt.wantDiffs)
		})
	}
}

func TestDiffFields_AbsentSourcePresentDest(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{}
	dst := map[string]any{"field": "value"}

	// Optional pair: source absent, dest present non-nil → 1 diff with "absent in source, present in dest"
	optional := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "field", Required: false}}
	diffs := diffFields(src, dst, optional, reg)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "gone", diffs[0].SourcePath)
	assert.Equal(t, "field", diffs[0].DestField)
	assert.Equal(t, nil, diffs[0].Want)
	assert.Equal(t, "value", diffs[0].Got)
	assert.Equal(t, "absent in source, present in dest", diffs[0].Cause)

	// Required pair: source absent, dest present non-nil → 1 diff with "absent in source, present in dest"
	required := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "field", Required: true}}
	diffs = diffFields(src, dst, required, reg)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "gone", diffs[0].SourcePath)
	assert.Equal(t, "field", diffs[0].DestField)
	assert.Equal(t, nil, diffs[0].Want)
	assert.Equal(t, "value", diffs[0].Got)
	assert.Equal(t, "absent in source, present in dest", diffs[0].Cause)
}

func TestDiffVerbatim(t *testing.T) {
	src := map[string]any{"_id": "a", "n": int64(1), "_updatedAt": "x"}
	dst := map[string]any{"_id": "a", "n": float64(1), "_updatedAt": "y"}
	assert.Empty(t, diffVerbatim(src, dst, []string{"_updatedAt"}))

	dst["n"] = float64(2)
	diffs := diffVerbatim(src, dst, []string{"_updatedAt"})
	assert.Len(t, diffs, 1)
	assert.Equal(t, "n", diffs[0].SourcePath)

	dst["extra"] = true
	diffs = diffVerbatim(src, dst, []string{"_updatedAt", "n"})
	assert.Len(t, diffs, 1)
	assert.Equal(t, "extra", diffs[0].DestField)
}

// An explicit BSON null in the source counts as absent — transformers drop
// null fields, so a zero/absent dest must match instead of diffing nil vs "".
func TestDiffFields_ExplicitNullCountsAsAbsent(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{"f": nil}

	tests := []struct {
		name      string
		dst       map[string]any
		required  bool
		wantDiffs int
		wantCause string
	}{
		{"null source vs absent dest", map[string]any{}, false, 0, ""},
		{"null source vs empty-string dest", map[string]any{"field": ""}, false, 0, ""},
		{"null source vs zero-int dest", map[string]any{"field": 0}, false, 0, ""},
		{"null source vs present dest", map[string]any{"field": "x"}, false, 1, "absent in source, present in dest"},
		{"required null source vs zero dest", map[string]any{"field": ""}, true, 1, "required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs := []fieldPair{{SourcePaths: []string{"f"}, DestField: "field", Required: tt.required}}
			diffs := diffFields(src, tt.dst, pairs, reg)
			require.Len(t, diffs, tt.wantDiffs)
			if tt.wantCause != "" {
				assert.Contains(t, diffs[0].Cause, tt.wantCause)
			}
		})
	}
}

func TestSanitizeDiffValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"nil passes through", nil, nil},
		{"scalar passes through", int64(7), int64(7)},
		{"short string passes through", "hi", "hi"},
		{"bytes become string", []byte("raw"), "raw"},
		{"non-string map keys stringified", map[bool]string{true: "x"}, map[string]any{"true": "x"}},
		{"nested map sanitized", map[string]any{"m": map[int]int{1: 2}}, map[string]any{"m": map[string]any{"1": int(2)}}},
		{"slice elements sanitized", []any{map[bool]bool{false: true}}, []any{map[string]any{"false": true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeDiffValue(tt.in)
			assert.Equal(t, tt.want, got)
			_, err := json.Marshal(got)
			assert.NoError(t, err, "sanitized value must be JSON-encodable")
		})
	}

	t.Run("huge string truncated", func(t *testing.T) {
		got, ok := sanitizeDiffValue(strings.Repeat("a", 10*diffValueMaxString)).(string)
		require.True(t, ok)
		assert.LessOrEqual(t, len(got), diffValueMaxString+len("…(truncated)"))
		assert.True(t, strings.HasSuffix(got, "…(truncated)"))
	})

	t.Run("truncation never splits a rune", func(t *testing.T) {
		s := strings.Repeat("é", diffValueMaxString) // 2-byte rune straddles the cap
		got, ok := sanitizeDiffValue(s).(string)
		require.True(t, ok)
		assert.True(t, utf8.ValidString(got), "no split UTF-8 sequence at the cut point")
	})
}

// BSON permits non-finite doubles; encoding/json rejects them, which would
// silently kill the SSE frame — the sanitizer must stringify them.
func TestSanitizeDiffValue_NonFiniteFloats(t *testing.T) {
	tests := []struct {
		name string
		in   any
	}{
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"float32 NaN", float32(math.NaN())},
		{"nested in map", map[string]any{"v": math.Inf(1)}},
		{"nested in slice", []any{math.NaN()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeDiffValue(tt.in)
			_, err := json.Marshal(got)
			assert.NoError(t, err, "sanitized non-finite value must be JSON-encodable")
		})
	}

	t.Run("finite floats pass through unchanged", func(t *testing.T) {
		assert.Equal(t, 1.5, sanitizeDiffValue(1.5))
		assert.Equal(t, float32(2.5), sanitizeDiffValue(float32(2.5)))
	})
}

// A diff points at a mismatch — it must not retain a near-16MiB BSON container.
func TestSanitizeDiffValue_CapsContainers(t *testing.T) {
	t.Run("long slice truncated", func(t *testing.T) {
		big := make([]any, 500)
		for i := range big {
			big[i] = i
		}
		got, ok := sanitizeDiffValue(big).([]any)
		require.True(t, ok)
		assert.LessOrEqual(t, len(got), diffValueMaxElems+1, "elements capped plus a truncation marker")
		_, err := json.Marshal(got)
		assert.NoError(t, err)
	})

	t.Run("large map truncated", func(t *testing.T) {
		big := map[string]any{}
		for i := 0; i < 500; i++ {
			big[fmt.Sprintf("k%03d", i)] = i
		}
		got, ok := sanitizeDiffValue(big).(map[string]any)
		require.True(t, ok)
		assert.LessOrEqual(t, len(got), diffValueMaxElems+1)
		_, err := json.Marshal(got)
		assert.NoError(t, err)
	})

	t.Run("deep nesting flattened at the depth cap", func(t *testing.T) {
		deep := any("leaf")
		for i := 0; i < 3*diffValueMaxDepth; i++ {
			deep = map[string]any{"d": deep}
		}
		got := sanitizeDiffValue(deep)
		b, err := json.Marshal(got)
		require.NoError(t, err)
		assert.Less(t, len(b), 64*diffValueMaxDepth, "depth cap keeps the encoded diff small")
	})
}

// Diff values must be safe to hold and broadcast: json.Marshal of a stored
// diff can never fail, whatever driver types the lookup returned.
func TestDiffFields_ValuesAreJSONSafe(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{"f": "want"}
	dst := map[string]any{"field": map[bool]string{true: "x"}}

	diffs := diffFields(src, dst, []fieldPair{{SourcePaths: []string{"f"}, DestField: "field"}}, reg)
	require.Len(t, diffs, 1)
	_, err := json.Marshal(diffs)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"true": "x"}, diffs[0].Got)
}

func TestDiffVerbatim_ValuesAreJSONSafe(t *testing.T) {
	src := map[string]any{"reactions": map[bool]string{true: "x"}}
	dst := map[string]any{}
	diffs := diffVerbatim(src, dst, nil)
	require.Len(t, diffs, 1)
	_, err := json.Marshal(diffs)
	require.NoError(t, err)
}
