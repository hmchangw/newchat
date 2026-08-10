package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
