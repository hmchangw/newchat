// Package pagefit sizes a paginated reply against a byte budget so a handler
// can return a smaller page with its "more" flag set, rather than a response
// the transport refuses. It owns arithmetic only — callers decide what a row
// is and which flag a trim sets.
package pagefit

import (
	"encoding/json"
	"fmt"
	"math"
)

// DefaultReserve is the headroom services leave under the broker's max_payload
// for reply headers (trace context, request id) that the budget cannot see.
const DefaultReserve = 4 << 10

// Budget is the byte ceiling one reply must fit under. The zero value is
// disabled, which keeps every page whole.
type Budget struct{ max int }

// NewBudget derives the ceiling from the broker's max_payload less a reserve.
// An unknown cap, or one the reserve swallows, disables trimming — a
// misconfigured reserve must not empty every reply.
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

// Resolve picks the startup ceiling: a positive override (MAX_RESPONSE_BYTES)
// wins, else the broker's max_payload. The override only ever lowers the
// ceiling — raising it above what the broker accepts would size replies the
// wire then refuses. Both are reduced by reserve.
func Resolve(override, brokerMaxPayload int64, reserve int) Budget {
	if override <= 0 {
		return NewBudget(brokerMaxPayload, reserve)
	}
	if brokerMaxPayload > 0 {
		override = min(override, brokerMaxPayload)
	}
	return NewBudget(override, reserve)
}

// Enabled reports whether this budget trims at all.
func (b Budget) Enabled() bool { return b.max > 0 }

// Bytes is the ceiling in bytes; 0 when disabled.
func (b Budget) Bytes() int { return b.max }

// Fit trims items to the longest prefix that fits b, charging envelope for the
// response's non-item bytes. Callers read "rows were dropped" off the returned
// length; oversize is the part they cannot derive — the one kept row still
// overflows. A non-empty slice always keeps a row, since zero rows with "more"
// set would leave the client no position to page from.
func Fit[T any](items []T, b Budget, envelope int) (kept []T, oversize bool, err error) {
	if !b.Enabled() || len(items) == 0 {
		return items, false, nil
	}
	w, total, err := rowSizes(items)
	if err != nil {
		return nil, false, err
	}
	if total <= b.max-envelope {
		return items, false, nil
	}

	used, n := envelope+brackets, 0
	for i, width := range w {
		sep := 0
		if i > 0 {
			sep = 1
		}
		// Subtraction, never used+width, so a large width cannot overflow int.
		if used > b.max-sep || width > b.max-sep-used {
			break
		}
		used += sep + width
		n = i + 1
	}
	if n == 0 {
		return items[:1], true, nil
	}
	return items[:n], false, nil
}

// FitWindow trims a centred window to the span [lo,hi) that fits b, grown
// outward from pivot so it stays centred. pivot is clamped into range and
// always kept; oversize says pivot alone overflows.
func FitWindow[T any](items []T, pivot int, b Budget, envelope int) (lo, hi int, oversize bool, err error) {
	if len(items) == 0 {
		return 0, 0, false, nil
	}
	pivot = min(max(pivot, 0), len(items)-1)
	if !b.Enabled() {
		return 0, len(items), false, nil
	}
	w, total, err := rowSizes(items)
	if err != nil {
		return 0, 0, false, err
	}
	if total <= b.max-envelope {
		return 0, len(items), false, nil
	}

	// Subtraction throughout, never used+w, so a large width cannot overflow.
	base := envelope + brackets
	pivotOversize := w[pivot] > b.max-base
	used := base
	if !pivotOversize {
		used += w[pivot]
	}
	lo, hi = pivot, pivot+1
	for !pivotOversize {
		grew := false
		if hi < len(items) && used <= b.max-1 && w[hi] <= b.max-1-used {
			used += w[hi] + 1
			hi++
			grew = true
		}
		if lo > 0 && used <= b.max-1 && w[lo-1] <= b.max-1-used {
			used += w[lo-1] + 1
			lo--
			grew = true
		}
		if !grew {
			break
		}
	}
	return lo, hi, pivotOversize, nil
}

// rowSizes measures every row and the whole array in one pass. sum(rows) plus
// separators and brackets IS the array length, so a separate whole-slice encode
// would buy nothing. One encoder over a counter keeps the bytes unallocated —
// only the length is ever needed.
func rowSizes[T any](items []T) (sizes []int, total int, err error) {
	var c lenCounter
	enc := json.NewEncoder(&c)
	sizes = make([]int, len(items))
	prev := 0
	for i := range items {
		if err := enc.Encode(items[i]); err != nil {
			return nil, 0, fmt.Errorf("size row %d: %w", i, err)
		}
		sizes[i] = c.n - prev - 1 // Encode appends a newline
		prev = c.n
	}
	return sizes, c.n - len(items) + max(len(items)-1, 0) + brackets, nil
}

// brackets is the "[" and "]" wrapping any encoded item array.
const brackets = 2

type lenCounter struct{ n int }

func (c *lenCounter) Write(p []byte) (int, error) {
	c.n += len(p)
	return len(p), nil
}
