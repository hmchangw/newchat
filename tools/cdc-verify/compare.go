package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// getPath walks a dot-path through nested map[string]any. No array indexing.
func getPath(doc map[string]any, path string) (any, bool) {
	cur := any(doc)
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// normalize maps driver-specific scalar types onto canonical comparison forms.
// Numeric magnitudes in this domain (unix ms, counts) are far below 2^53, so
// float64 is a safe common numeric type.
func normalize(v any) any {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case float32:
		return float64(t)
	case time.Time:
		return float64(t.UTC().UnixMilli())
	case []byte:
		return string(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = normalize(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = normalize(vv)
		}
		return out
	default:
		return v
	}
}

func valuesEqual(a, b any) bool {
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// isZeroValue reports whether an already-normalized value is the zero value
// of its comparison form: nil, false, 0, or "". Used to decide whether a dest
// value counts as "absent" when its paired source field is absent (spec §5.3).
func isZeroValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case bool:
		return !t
	case float64:
		return t == 0
	case string:
		return t == ""
	default:
		return false
	}
}

// FieldDiff is one mismatched field in a failed sub-check.
type FieldDiff struct {
	SourcePath string `json:"sourcePath"`
	DestField  string `json:"destField"`
	Want       any    `json:"want"`
	Got        any    `json:"got"`
	Cause      string `json:"cause,omitempty"`
}

// fieldPair is the compiled per-target form of one mapping entry: source
// path(s) -> one dest field, optionally through a transform.
type fieldPair struct {
	SourcePaths []string
	DestField   string
	Transform   string
	Required    bool
}

func diffFields(src, dst map[string]any, pairs []fieldPair, reg transformRegistry) []FieldDiff {
	var diffs []FieldDiff
	for i := range pairs {
		p := &pairs[i]
		args := make([]any, 0, len(p.SourcePaths))
		anyPresent := false
		for _, sp := range p.SourcePaths {
			v, ok := getPath(src, sp)
			if ok {
				anyPresent = true
			}
			args = append(args, v)
		}
		got, gotOK := getPath(dst, p.DestField)

		if !anyPresent {
			// spec §5.3: an absent/null source field matches a dest value that is
			// itself absent/nil or the zero value of its normalized form (false,
			// 0, ""), unless the mapping marks the field required. A required
			// field still needs a genuine dest value — a zero-value dest counts
			// as absent for that check too, so it's diagnosed the same as a
			// fully-absent dest rather than silently passing.
			destZero := !gotOK || got == nil || isZeroValue(normalize(got))
			switch {
			case !destZero:
				diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
					DestField: p.DestField, Want: nil, Got: got, Cause: "absent in source, present in dest"})
			case p.Required:
				diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
					DestField: p.DestField, Cause: "required field absent on both sides"})
			}
			continue
		}

		want, err := reg.apply(p.Transform, args)
		if err != nil {
			diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
				DestField: p.DestField, Cause: fmt.Sprintf("transform-error: %v", err)})
			continue
		}
		if !valuesEqual(want, got) {
			diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
				DestField: p.DestField, Want: want, Got: got})
		}
	}
	return diffs
}

// diffVerbatim deep-compares whole documents both ways, skipping ignored
// top-level keys. Dest-only keys are reported with an empty SourcePath want.
func diffVerbatim(src, dst map[string]any, ignore []string) []FieldDiff {
	skip := make(map[string]bool, len(ignore))
	for _, k := range ignore {
		skip[k] = true
	}
	var diffs []FieldDiff
	for k, want := range src {
		if skip[k] {
			continue
		}
		got, ok := getPath(dst, k)
		if !ok || !valuesEqual(want, got) {
			diffs = append(diffs, FieldDiff{SourcePath: k, DestField: k, Want: want, Got: got})
		}
	}
	for k, got := range dst {
		if skip[k] {
			continue
		}
		if _, ok := src[k]; !ok {
			diffs = append(diffs, FieldDiff{DestField: k, Got: got, Cause: "present only in dest"})
		}
	}
	return diffs
}
