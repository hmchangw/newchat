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

// DefaultReserve is the headroom services leave under the broker's max_payload
// for reply headers (trace context, request id) that the budget cannot see.
const DefaultReserve = 4 << 10

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

// Resolve picks the reply ceiling at startup: an explicit override (from
// MAX_RESPONSE_BYTES) when positive, otherwise the broker's advertised
// max_payload. Both are reduced by reserve.
func Resolve(override, brokerMaxPayload int64, reserve int) Budget {
	if override > 0 {
		return NewBudget(override, reserve)
	}
	return NewBudget(brokerMaxPayload, reserve)
}

// Enabled reports whether this budget trims at all.
func (b Budget) Enabled() bool { return b.max > 0 }

// Bytes is the ceiling in bytes; 0 when disabled.
func (b Budget) Bytes() int { return b.max }

// Fit trims items to the longest prefix that fits b, charging envelope for the
// response's non-item bytes.
//
// dropped reports that rows were removed, so the caller can set its "more"
// flag. oversize reports that the single kept row still exceeds the budget:
// a non-empty slice always keeps at least one row, because a caller handed
// zero rows with "more" set would have no position to page from.
func Fit[T any](items []T, b Budget, envelope int) (kept []T, dropped, oversize bool) {
	if fitsWhole(items, b, envelope) {
		return items, false, false
	}
	total, n := envelope, 0
	for i, w := range widths(items) {
		if i > 0 {
			w++ // separator
		}
		if total+w > b.max {
			break
		}
		total += w
		n = i + 1
	}
	if n == 0 {
		return items[:1], len(items) > 1, true
	}
	return items[:n], n < len(items), false
}

// FitWindow trims a centred window to the span [lo,hi) that fits b, grown
// outward one row at a time from each side so it stays centred on pivot.
// pivot is always kept — it is the row the caller centred on — and is clamped
// into range. oversize reports that pivot alone exceeds the budget.
func FitWindow[T any](items []T, pivot int, b Budget, envelope int) (lo, hi int, oversize bool) {
	if len(items) == 0 {
		return 0, 0, false
	}
	pivot = min(max(pivot, 0), len(items)-1)
	if fitsWhole(items, b, envelope) {
		return 0, len(items), false
	}

	w := widths(items)
	total := envelope + w[pivot]
	lo, hi = pivot, pivot+1
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
			return lo, hi, hi-lo == 1 && envelope+w[pivot] > b.max
		}
	}
}

// fitsWhole is the fast path both trims share: one streamed encode of the whole
// slice answers the common "nothing needs trimming" case, so a page that fits
// never pays the per-row marshal below.
func fitsWhole[T any](items []T, b Budget, envelope int) bool {
	if !b.Enabled() || len(items) == 0 {
		return true
	}
	return envelope+encodedLen(items) <= b.max
}

// widths marshals each row once so the trim loops can scan encoded sizes
// without re-encoding. Only reached when the whole slice already overflowed.
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

// encodedLen measures JSON output without keeping it — the size is all we need,
// so the bytes go to a counter rather than a buffer.
func encodedLen(v any) int {
	var c lenCounter
	if err := json.NewEncoder(&c).Encode(v); err != nil {
		return oversizeRow
	}
	return c.n - 1 // Encode appends a newline
}

type lenCounter struct{ n int }

func (c *lenCounter) Write(p []byte) (int, error) {
	c.n += len(p)
	return len(p), nil
}
