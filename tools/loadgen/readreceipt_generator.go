package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// readReceiptTarget is one (sender, room, message) tuple the workload can
// request a read-receipt for. The requester account is the message's sender
// because the RPC requires msgSender == requesterAccount.
type readReceiptTarget struct {
	Account   string
	RoomID    string
	MessageID string
}

// deriveReadReceiptTargets selects every top-level message (ThreadParentID == "")
// from the plan as a target. Thread replies are excluded.
func deriveReadReceiptTargets(plan *MessagePlan) []readReceiptTarget {
	out := make([]readReceiptTarget, 0, len(plan.Messages))
	for i := range plan.Messages {
		m := &plan.Messages[i]
		if m.ThreadParentID != "" {
			continue
		}
		out = append(out, readReceiptTarget{
			Account:   m.SenderAccount,
			RoomID:    m.RoomID,
			MessageID: m.MessageID,
		})
	}
	return out
}

// ReadReceiptGeneratorConfig bundles every dependency the generator needs.
type ReadReceiptGeneratorConfig struct {
	Targets        []readReceiptTarget
	SiteID         string
	Rate           int
	RequestTimeout time.Duration
	Requester      ReadReceiptRequester
	Collector      *ReadReceiptCollector
	MaxInFlight    int
}

// ReadReceiptGenerator drives the open-loop request/reply loop. Mirrors
// HistoryGenerator.Run's shape: a ticker paces requests, and when MaxInFlight>0
// each tick dispatches to a bounded goroutine pool with saturation tallied.
type ReadReceiptGenerator struct {
	cfg   ReadReceiptGeneratorConfig
	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewReadReceiptGenerator constructs a generator seeded from `seed`.
func NewReadReceiptGenerator(cfg *ReadReceiptGeneratorConfig, seed int64) *ReadReceiptGenerator {
	return &ReadReceiptGenerator{
		cfg: *cfg,
		rng: rand.New(rand.NewSource(seed)),
	}
}

// Run drives the open-loop publisher until ctx cancels. MaxInFlight>0 uses the
// batched pacer (so achieved RPS is not capped by single-ticker resolution);
// MaxInFlight<=0 selects the legacy serial path, retained for bisection — it
// will not ramp past the single-ticker ceiling.
func (g *ReadReceiptGenerator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if g.cfg.MaxInFlight <= 0 {
		serialDispatch(ctx, g.cfg.Rate, g.requestOne)
		return nil
	}
	pacedDispatch(ctx, g.cfg.Rate, g.cfg.MaxInFlight,
		g.cfg.Collector.RecordUnderrun, g.cfg.Collector.RecordSaturation, g.requestOne)
	return nil
}

func (g *ReadReceiptGenerator) requestOne(ctx context.Context) {
	if len(g.cfg.Targets) == 0 {
		return
	}
	t := g.cfg.Targets[g.intn(len(g.cfg.Targets))]

	data, err := json.Marshal(model.ReadReceiptRequest{MessageID: t.MessageID})
	if err != nil {
		g.cfg.Collector.RecordBadRequest()
		return
	}
	subj := subject.MessageReadReceipt(t.Account, t.RoomID, g.cfg.SiteID)

	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, data, g.cfg.RequestTimeout)
	latency := time.Since(start)
	if err != nil {
		// Run-level cancellation isn't a real failure — the run is draining.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(classifyRequesterError(err))
		return
	}
	// A reply carrying an error field is a logical failure, not a latency sample.
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(reply, &payload); err != nil {
		g.cfg.Collector.RecordError(errClassBadReply)
		return
	}
	if payload.Error != "" {
		g.cfg.Collector.RecordError(errClassReply)
		return
	}
	g.cfg.Collector.RecordSample(latency)
}

func (g *ReadReceiptGenerator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}
