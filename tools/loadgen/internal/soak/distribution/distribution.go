// Package distribution owns the deterministic probability models used to
// choose soak rooms, payload sizes, and thread reply budgets.
package distribution

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"strings"
	"sync/atomic"

	"github.com/hmchangw/chat/pkg/atrest"
)

// DefaultRoomZipfS and DefaultRoomZipfV reproduce the constants these
// replaced, so a run that sets neither records the same distribution every
// earlier percentile was measured against.
const (
	DefaultRoomZipfS = 1.2
	DefaultRoomZipfV = 1.0
)

const (
	gcmTagBytes           = 16
	maxClientContentBytes = 20 * 1024
	threadReplyP99        = 50
	threadReplyMedian     = 5
	normalP95             = 1.6448536269514722
	normalP99             = 2.3263478740408408
)

// ThreadReplyHardCap bounds both sampled and catalog-recovered thread sizes.
const ThreadReplyHardCap = 500

var encryptedContentOverhead = func() int {
	serialized, err := json.Marshal(atrest.EncryptedFields{Msg: "x"})
	if err != nil {
		panic("marshal modeled encrypted fields: " + err.Error())
	}
	return len(serialized) - 1 + gcmTagBytes
}()

// RoomPicker samples room indexes from a deterministic Zipf distribution.
type RoomPicker struct {
	zipf *rand.Zipf
}

// NewRoomPicker builds the room-popularity distribution: P(rank) is
// proportional to (v+rank)^-s, so s sets how steeply traffic concentrates and
// v flattens the head. Raising v is the only way to model a site whose busiest
// room is a few percent of the whole — math/rand cannot express s <= 1.
func NewRoomPicker(seed int64, roomCount int, zipfS, zipfV float64) (*RoomPicker, error) {
	if roomCount <= 0 {
		return nil, fmt.Errorf("room count must be greater than zero")
	}
	// math/rand guards on `s <= 1`, which NaN and +Inf both slip past, and the
	// generator it then returns is not nil — its Uint64 spins forever. Refusing
	// non-finite values here is what keeps a typo from hanging the run silently.
	if math.IsNaN(zipfS) || math.IsInf(zipfS, 0) || zipfS <= 1 {
		return nil, fmt.Errorf("room Zipf exponent must be greater than 1, got %v", zipfS)
	}
	if math.IsNaN(zipfV) || math.IsInf(zipfV, 0) || zipfV < 1 {
		return nil, fmt.Errorf("room Zipf offset must be at least 1, got %v", zipfV)
	}
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, zipfS, zipfV, uint64(roomCount-1))
	if zipf == nil {
		return nil, fmt.Errorf("room Zipf generator rejected s=%v v=%v", zipfS, zipfV)
	}
	return &RoomPicker{zipf: zipf}, nil
}

// Next returns the next sampled room index.
func (p *RoomPicker) Next() int {
	return int(p.zipf.Uint64())
}

// PayloadSizer samples plaintext content sizes from the configured encrypted
// payload percentiles.
type PayloadSizer struct {
	rng   *rand.Rand
	mu    float64
	sigma float64
	max   int
}

// NewPayloadSizer constructs a deterministic log-normal payload sampler.
func NewPayloadSizer(
	seed int64,
	medianEncryptedBytes int,
	p95EncryptedBytes int,
	maxEncryptedBytes int,
) (*PayloadSizer, error) {
	if medianEncryptedBytes <= encryptedContentOverhead {
		return nil, fmt.Errorf("encrypted payload median must exceed encryption overhead")
	}
	if p95EncryptedBytes < medianEncryptedBytes {
		return nil, fmt.Errorf("encrypted payload p95 must be at least the median")
	}
	if maxEncryptedBytes < p95EncryptedBytes {
		return nil, fmt.Errorf("encrypted payload maximum must be at least p95")
	}
	if maxEncryptedBytes-encryptedContentOverhead > maxClientContentBytes {
		return nil, fmt.Errorf(
			"encrypted payload maximum produces client content above the gatekeeper limit of %d bytes",
			maxClientContentBytes,
		)
	}

	mu := math.Log(float64(medianEncryptedBytes))
	sigma := (math.Log(float64(p95EncryptedBytes)) - mu) / normalP95
	return &PayloadSizer{
		rng:   rand.New(rand.NewSource(seed)),
		mu:    mu,
		sigma: sigma,
		max:   maxEncryptedBytes,
	}, nil
}

// NextContentBytes returns a plaintext size whose modeled encrypted size
// follows the configured distribution.
func (s *PayloadSizer) NextContentBytes() int {
	target := int(math.Round(math.Exp(s.mu + s.sigma*s.rng.NormFloat64())))
	target = max(encryptedContentOverhead+1, min(target, s.max))
	return target - encryptedContentOverhead
}

func modeledEncryptedPayloadBytes(contentBytes int) int {
	if contentBytes <= 0 {
		return gcmTagBytes + 2 // JSON "{}" when Msg is omitted.
	}
	return contentBytes + encryptedContentOverhead
}

// ContentOfSize returns the deterministic body used by soak message sends.
func ContentOfSize(size int) string {
	return strings.Repeat("x", max(0, size))
}

// ThreadBudgetSampler samples the number of replies allowed for a thread.
type ThreadBudgetSampler struct {
	rng   *rand.Rand
	mu    float64
	sigma float64
}

// NewThreadBudgetSampler constructs a deterministic thread reply sampler.
func NewThreadBudgetSampler(seed int64) *ThreadBudgetSampler {
	mu := math.Log(threadReplyMedian)
	return &ThreadBudgetSampler{
		rng:   rand.New(rand.NewSource(seed)),
		mu:    mu,
		sigma: (math.Log(threadReplyP99) - mu) / normalP99,
	}
}

// Next returns the sampled thread reply budget.
func (s *ThreadBudgetSampler) Next() int {
	budget := int(math.Round(math.Exp(s.mu + s.sigma*s.rng.NormFloat64())))
	return max(1, min(budget, ThreadReplyHardCap))
}

type threadBudget struct {
	limit int64
	used  atomic.Int64
}

func newThreadBudget(limit int) *threadBudget {
	return &threadBudget{limit: int64(max(1, min(limit, ThreadReplyHardCap)))}
}

func (b *threadBudget) TryConsume() bool {
	for {
		used := b.used.Load()
		if used >= b.limit {
			return false
		}
		if b.used.CompareAndSwap(used, used+1) {
			return true
		}
	}
}

func (b *threadBudget) Limit() int {
	return int(b.limit)
}

func (b *threadBudget) Used() int {
	return int(b.used.Load())
}
