// Package pagefit sizes a paginated reply against a byte budget so a handler
// can return a smaller page with its "more" flag set, rather than a response
// the transport refuses. It owns arithmetic only — callers decide what a row
// is and which flag a trim sets.
package pagefit

import (
	"encoding/json"
	"math"
)

// unmeasurable marks a row that will not encode. A negative width rather than
// a large one: any sentinel big enough to lose against a small budget still
// ties or overflows against a large one, and a tie reads as "fits".
const unmeasurable = -1

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
// wins, else the broker's max_payload. Both are reduced by reserve.
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
// response's non-item bytes. dropped drives the caller's "more" flag; oversize
// says the one kept row still overflows. A non-empty slice always keeps a row —
// zero rows with "more" set would leave the client no position to page from.
func Fit[T any](items []T, b Budget, envelope int) (kept []T, dropped, oversize bool) {
	if fitsWhole(items, b, envelope) {
		return items, false, false
	}
	total, n := envelope, 0
	for i, w := range widths(items) {
		sep := 0
		if i > 0 {
			sep = 1
		}
		// Subtraction, never total+w, so a large width cannot overflow int.
		if w == unmeasurable || total > b.max-sep || w > b.max-sep-total {
			break
		}
		total += sep + w
		n = i + 1
	}
	if n == 0 {
		return items[:1], len(items) > 1, true
	}
	return items[:n], n < len(items), false
}

// FitWindow trims a centred window to the span [lo,hi) that fits b, grown
// outward from pivot so it stays centred. pivot is clamped into range and
// always kept; oversize says pivot alone overflows.
func FitWindow[T any](items []T, pivot int, b Budget, envelope int) (lo, hi int, oversize bool) {
	if len(items) == 0 {
		return 0, 0, false
	}
	pivot = min(max(pivot, 0), len(items)-1)
	if fitsWhole(items, b, envelope) {
		return 0, len(items), false
	}

	w := widths(items)
	// Subtraction throughout, never total+w, so a large width cannot overflow.
	pivotOversize := w[pivot] == unmeasurable || w[pivot] > b.max-envelope
	total := envelope
	if !pivotOversize {
		total += w[pivot]
	}
	lo, hi = pivot, pivot+1
	for !pivotOversize {
		grew := false
		if hi < len(items) && w[hi] != unmeasurable && total <= b.max-1 && w[hi] <= b.max-1-total {
			total += w[hi] + 1
			hi++
			grew = true
		}
		if lo > 0 && w[lo-1] != unmeasurable && total <= b.max-1 && w[lo-1] <= b.max-1-total {
			total += w[lo-1] + 1
			lo--
			grew = true
		}
		if !grew {
			break
		}
	}
	return lo, hi, pivotOversize
}

// fitsWhole is the shared fast path: one streamed encode answers the common
// "nothing to trim" case, so a fitting page never pays the per-row marshal.
func fitsWhole[T any](items []T, b Budget, envelope int) bool {
	if !b.Enabled() || len(items) == 0 {
		return true
	}
	// A slice that will not encode can never be said to fit — comparing a
	// sentinel width here would call it fitting at a large enough budget.
	n, ok := encodedLen(items)
	return ok && n <= b.max-envelope
}

// widths marshals each row once so the trim loops can scan sizes without
// re-encoding. Only reached once the whole slice has already overflowed.
func widths[T any](items []T) []int {
	out := make([]int, len(items))
	for i := range items {
		data, err := json.Marshal(items[i])
		if err != nil {
			out[i] = unmeasurable
			continue
		}
		out[i] = len(data)
	}
	return out
}

// encodedLen measures JSON output without keeping it — the size is all we need,
// so the bytes go to a counter rather than a buffer. ok is false when v will
// not encode, which callers must treat as "no measurable size", never as zero.
func encodedLen(v any) (n int, ok bool) {
	var c lenCounter
	if err := json.NewEncoder(&c).Encode(v); err != nil {
		return 0, false
	}
	return c.n - 1, true // Encode appends a newline
}

type lenCounter struct{ n int }

func (c *lenCounter) Write(p []byte) (int, error) {
	c.n += len(p)
	return len(p), nil
}
