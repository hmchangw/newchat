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

const (
	soakZipfS                 = 1.2
	soakZipfV                 = 1.0
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

func newSoakRoomPicker(seed int64, roomCount int) (*soakRoomPicker, error) {
	if roomCount <= 0 {
		return nil, fmt.Errorf("room count must be greater than zero")
	}
	rng := rand.New(rand.NewSource(seed))
	return &soakRoomPicker{
		zipf: rand.NewZipf(rng, soakZipfS, soakZipfV, uint64(roomCount-1)),
	}, nil
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
