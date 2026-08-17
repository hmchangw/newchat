package main

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
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

// normalize maps driver scalar types onto canonical comparison forms. Domain
// magnitudes (unix ms, counts) sit far below 2^53, so float64 is safe for all numbers.
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

// isZeroValue reports whether a normalized value is its comparison-form zero
// (nil, false, 0, ""): a zero dest counts as absent when the source field is absent.
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

// A diff is a pointer at a mismatch, not a document dump — rows are retained
// and broadcast as JSON, so every axis of a value is bounded: string length,
// container elements, and nesting depth.
const (
	diffValueMaxString = 2048
	diffValueMaxElems  = 64
	diffValueMaxDepth  = 8
)

// sanitizeDiffValue makes a value safe to retain and JSON-encode in a diff:
// non-string map keys are stringified (json.Marshal rejects e.g. map[bool]T
// from a driver), non-finite floats become strings (json.Marshal rejects NaN
// and ±Inf), []byte becomes string, and strings/containers/depth are bounded.
func sanitizeDiffValue(v any) any { return sanitizeDiffDepth(v, 0) }

func sanitizeDiffDepth(v any, depth int) any {
	switch t := v.(type) {
	case nil:
		return nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return fmt.Sprint(t)
		}
		return t
	case float32:
		if f := float64(t); math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Sprint(t)
		}
		return t
	case string:
		return truncDiffString(t)
	case []byte:
		return truncDiffString(string(t))
	case map[string]any:
		if depth >= diffValueMaxDepth {
			return truncDiffString(fmt.Sprint(t))
		}
		out := make(map[string]any, min(len(t), diffValueMaxElems+1))
		for i, k := range sortedMapKeys(t) {
			if i >= diffValueMaxElems {
				out["…"] = fmt.Sprintf("(%d more entries truncated)", len(t)-diffValueMaxElems)
				break
			}
			out[truncDiffString(k)] = sanitizeDiffDepth(t[k], depth+1)
		}
		return out
	case []any:
		return sanitizeDiffSlice(len(t), func(i int) any { return t[i] }, depth)
	}
	if depth >= diffValueMaxDepth {
		return truncDiffString(fmt.Sprint(v))
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		keys := make([]string, 0, rv.Len())
		vals := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			k := truncDiffString(fmt.Sprint(iter.Key().Interface()))
			keys = append(keys, k)
			vals[k] = iter.Value().Interface()
		}
		sort.Strings(keys)
		out := make(map[string]any, min(len(keys), diffValueMaxElems+1))
		for i, k := range keys {
			if i >= diffValueMaxElems {
				out["…"] = fmt.Sprintf("(%d more entries truncated)", len(keys)-diffValueMaxElems)
				break
			}
			out[k] = sanitizeDiffDepth(vals[k], depth+1)
		}
		return out
	case reflect.Slice, reflect.Array:
		return sanitizeDiffSlice(rv.Len(), func(i int) any { return rv.Index(i).Interface() }, depth)
	default:
		return v
	}
}

func sanitizeDiffSlice(n int, at func(int) any, depth int) any {
	if depth >= diffValueMaxDepth {
		return fmt.Sprintf("(list of %d elements truncated at depth cap)", n)
	}
	kept := min(n, diffValueMaxElems)
	out := make([]any, 0, kept+1)
	for i := 0; i < kept; i++ {
		out = append(out, sanitizeDiffDepth(at(i), depth+1))
	}
	if n > kept {
		out = append(out, fmt.Sprintf("…(%d more elements truncated)", n-kept))
	}
	return out
}

func truncDiffString(s string) string {
	if len(s) <= diffValueMaxString {
		return s
	}
	cut := diffValueMaxString
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…(truncated)"
}

// FieldDiff is one mismatched field in a failed sub-check.
type FieldDiff struct {
	SourcePath string `json:"sourcePath"`
	DestField  string `json:"destField"`
	Want       any    `json:"want"`
	Got        any    `json:"got"`
	Cause      string `json:"cause,omitempty"`
}

// fieldPair is one compiled mapping entry: source path(s) -> one dest field.
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
			// An explicit BSON null counts as absent: transformers drop null
			// fields, so the dest legitimately holds nothing for them.
			if ok && v != nil {
				anyPresent = true
			}
			args = append(args, v)
		}
		got, gotOK := getPath(dst, p.DestField)

		if !anyPresent {
			// spec §5.3: an absent source field matches an absent/nil/zero dest,
			// unless required — then a zero dest diagnoses like a fully-absent one.
			destZero := !gotOK || got == nil || isZeroValue(normalize(got))
			switch {
			case !destZero:
				diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
					DestField: p.DestField, Want: nil, Got: sanitizeDiffValue(got), Cause: "absent in source, present in dest"})
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
				DestField: p.DestField, Want: sanitizeDiffValue(want), Got: sanitizeDiffValue(got)})
		}
	}
	return diffs
}

// diffVerbatim deep-compares whole docs both ways minus ignored keys; dest-only keys diff too.
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
			diffs = append(diffs, FieldDiff{SourcePath: k, DestField: k,
				Want: sanitizeDiffValue(want), Got: sanitizeDiffValue(got)})
		}
	}
	for k, got := range dst {
		if skip[k] {
			continue
		}
		if _, ok := src[k]; !ok {
			diffs = append(diffs, FieldDiff{DestField: k, Got: sanitizeDiffValue(got), Cause: "present only in dest"})
		}
	}
	return diffs
}
