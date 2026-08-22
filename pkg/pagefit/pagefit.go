// Package pagefit sizes a paginated reply against a byte budget so a handler
// can return a smaller page with its "more" flag set, rather than a response
// the transport refuses. It owns arithmetic only — callers decide what a row
// is and which flag a trim sets.
package pagefit

import (
	"encoding/json"
	"math"
)

// oversizeRow is the width charged to a row that cannot be marshalled. Large
// enough to lose against any real budget, small enough not to overflow the
// running total.
const oversizeRow = math.MaxInt32

// Budget is the byte ceiling one reply must fit under. The zero value is
// disabled, which keeps every page whole.
type Budget struct{ max int }

// NewBudget derives the ceiling from the broker's advertised max_payload, less
// a reserve for trace headers and the envelope's non-item fields. An unknown
// cap, or one the reserve swallows, disables trimming instead of rejecting
// every page — a misconfigured reserve must not empty every reply.
func NewBudget(brokerMaxPayload int64, reserve int) Budget {
	if brokerMaxPayload <= 0 || reserve < 0 {
		return Budget{}
	}
	if brokerMaxPayload > math.MaxInt32 {
		brokerMaxPayload = math.MaxInt32
	}
	max := int(brokerMaxPayload) - reserve
	if max <= 0 {
		return Budget{}
	}
	return Budget{max: max}
}

// Enabled reports whether this budget trims at all.
func (b Budget) Enabled() bool { return b.max > 0 }

// Bytes is the ceiling in bytes; 0 when disabled.
func (b Budget) Bytes() int { return b.max }

// Prefix returns the largest n where items[:n] fits b, charging envelope for
// the response's non-item bytes. A non-empty slice always yields at least 1:
// a caller handed 0 rows with "more" set would have no position to page from.
func Prefix[T any](items []T, b Budget, envelope int) int {
	if !b.Enabled() || len(items) == 0 {
		return len(items)
	}
	total := envelope
	for i, w := range widths(items) {
		if i > 0 {
			w++ // separator
		}
		if total+w > b.max {
			return max(i, 1)
		}
		total += w
	}
	return len(items)
}

// Window returns the [lo,hi) span around pivot that fits b, grown outward one
// row at a time from each side so the span stays centred. pivot is always
// included — it is the row the caller asked to centre on — and is clamped into
// range.
func Window[T any](items []T, pivot int, b Budget, envelope int) (int, int) {
	if len(items) == 0 {
		return 0, 0
	}
	pivot = min(max(pivot, 0), len(items)-1)
	if !b.Enabled() {
		return 0, len(items)
	}

	w := widths(items)
	total := envelope + w[pivot]
	lo, hi := pivot, pivot+1
	for {
		grew := false
		if hi < len(items) && total+w[hi]+1 <= b.max {
			total += w[hi] + 1
			hi++
			grew = true
		}
		if lo > 0 && total+w[lo-1]+1 <= b.max {
			total += w[lo-1] + 1
			lo--
			grew = true
		}
		if !grew {
			return lo, hi
		}
	}
}

// widths marshals each item once so a caller that scans the slice repeatedly
// pays for one encode per row, not one per comparison.
func widths[T any](items []T) []int {
	out := make([]int, len(items))
	for i := range items {
		data, err := json.Marshal(items[i])
		if err != nil {
			out[i] = oversizeRow
			continue
		}
		out[i] = len(data)
	}
	return out
}
