package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/hmchangw/chat/pkg/atrest"
)

// soakDefaultRoomZipfS and soakDefaultRoomZipfV reproduce the constants these
// replaced, so a run that sets neither records the same distribution every
// earlier percentile was measured against.
const (
	soakDefaultRoomZipfS = 1.2
	soakDefaultRoomZipfV = 1.0
)

const (
	soakGCMTagBytes           = 16
	soakMaxClientContentBytes = 20 * 1024
	soakThreadReplyP99        = 50
	soakThreadReplyHardCap    = 500
	soakThreadReplyMedian     = 5
	normalP95                 = 1.6448536269514722
	normalP99                 = 2.3263478740408408
)

var soakEncryptedContentOverhead = func() int {
	serialized, err := json.Marshal(atrest.EncryptedFields{Msg: "x"})
	if err != nil {
		panic("marshal modeled encrypted fields: " + err.Error())
	}
	return len(serialized) - 1 + soakGCMTagBytes
}()

type soakRoomPicker struct {
	zipf *rand.Zipf
}

// newSoakRoomPicker builds the room-popularity distribution: P(rank) is
// proportional to (v+rank)^-s, so s sets how steeply traffic concentrates and
// v flattens the head. Raising v is the only way to model a site whose busiest
// room is a few percent of the whole — math/rand cannot express s <= 1.
func newSoakRoomPicker(seed int64, roomCount int, zipfS, zipfV float64) (*soakRoomPicker, error) {
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
	return &soakRoomPicker{zipf: zipf}, nil
}

func (p *soakRoomPicker) Next() int {
	return int(p.zipf.Uint64())
}

type soakPayloadSizer struct {
	rng   *rand.Rand
	mu    float64
	sigma float64
	max   int
}

func newSoakPayloadSizer(
	seed int64,
	medianEncryptedBytes int,
	p95EncryptedBytes int,
	maxEncryptedBytes int,
) (*soakPayloadSizer, error) {
	if medianEncryptedBytes <= soakEncryptedContentOverhead {
		return nil, fmt.Errorf("encrypted payload median must exceed encryption overhead")
	}
	if p95EncryptedBytes < medianEncryptedBytes {
		return nil, fmt.Errorf("encrypted payload p95 must be at least the median")
	}
	if maxEncryptedBytes < p95EncryptedBytes {
		return nil, fmt.Errorf("encrypted payload maximum must be at least p95")
	}
	if maxEncryptedBytes-soakEncryptedContentOverhead > soakMaxClientContentBytes {
		return nil, fmt.Errorf(
			"encrypted payload maximum produces client content above the gatekeeper limit of %d bytes",
			soakMaxClientContentBytes,
		)
	}

	mu := math.Log(float64(medianEncryptedBytes))
	sigma := (math.Log(float64(p95EncryptedBytes)) - mu) / normalP95
	return &soakPayloadSizer{
		rng:   rand.New(rand.NewSource(seed)),
		mu:    mu,
		sigma: sigma,
		max:   maxEncryptedBytes,
	}, nil
}

func (s *soakPayloadSizer) NextContentBytes() int {
	target := int(math.Round(math.Exp(s.mu + s.sigma*s.rng.NormFloat64())))
	target = max(soakEncryptedContentOverhead+1, min(target, s.max))
	return target - soakEncryptedContentOverhead
}

func modeledEncryptedPayloadBytes(contentBytes int) int {
	if contentBytes <= 0 {
		return soakGCMTagBytes + 2 // JSON "{}" when Msg is omitted.
	}
	return contentBytes + soakEncryptedContentOverhead
}

func soakContentOfSize(size int) string {
	return strings.Repeat("x", max(0, size))
}

type soakThreadBudgetSampler struct {
	rng   *rand.Rand
	mu    float64
	sigma float64
}

func newSoakThreadBudgetSampler(seed int64) *soakThreadBudgetSampler {
	mu := math.Log(soakThreadReplyMedian)
	return &soakThreadBudgetSampler{
		rng:   rand.New(rand.NewSource(seed)),
		mu:    mu,
		sigma: (math.Log(soakThreadReplyP99) - mu) / normalP99,
	}
}

func (s *soakThreadBudgetSampler) Next() int {
	budget := int(math.Round(math.Exp(s.mu + s.sigma*s.rng.NormFloat64())))
	return max(1, min(budget, soakThreadReplyHardCap))
}

type soakThreadBudget struct {
	limit int64
	used  atomic.Int64
}

func newSoakThreadBudget(limit int) *soakThreadBudget {
	return &soakThreadBudget{limit: int64(max(1, min(limit, soakThreadReplyHardCap)))}
}

func (b *soakThreadBudget) TryConsume() bool {
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

func (b *soakThreadBudget) Limit() int {
	return int(b.limit)
}

func (b *soakThreadBudget) Used() int {
	return int(b.used.Load())
}
