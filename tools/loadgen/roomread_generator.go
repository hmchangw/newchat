package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// RoomReadRequester is the narrow request/reply transport seam. The production
// implementation reuses newNATSHistoryRequester (from history_main.go) — both
// interfaces share the same Request method signature; tests inject a recorder.
type RoomReadRequester interface {
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)
}

// roomReadReply is the message.read handler's success envelope.
type roomReadReply struct {
	Status string `json:"status"`
}

// roomReadGeneratorConfig bundles every dependency the generator needs.
type roomReadGeneratorConfig struct {
	Fixtures       *Fixtures
	SiteID         string
	Rate           int
	RequestTimeout time.Duration
	Requester      RoomReadRequester
	Collector      *RoomReadCollector
	MaxInFlight    int
}

// roomReadGenerator drives the open-loop message.read request/reply loop.
// Rooms are picked with a Zipf skew (hot rooms read more often, concentrating
// floor-write contention) and a random member of the chosen room is the reader.
type roomReadGenerator struct {
	cfg roomReadGeneratorConfig

	rngMu sync.Mutex
	rng   *rand.Rand
	zipf  *rand.Zipf

	roomSubs map[string][]model.Subscription
}

// newRoomReadGenerator constructs a generator seeded from `seed`. Zipf params
// match the history workload (s=1.1, v=1.0) for consistent hot-room skew.
func newRoomReadGenerator(cfg *roomReadGeneratorConfig, seed int64) *roomReadGenerator {
	r := rand.New(rand.NewSource(seed))
	rooms := cfg.Fixtures.Rooms
	roomCount := len(rooms)
	if roomCount < 1 {
		roomCount = 1
	}
	zipfN := uint64(roomCount - 1)
	if zipfN < 1 {
		zipfN = 1
	}
	z := rand.NewZipf(r, 1.1, 1.0, zipfN)

	lookup := make(map[string][]model.Subscription, roomCount)
	for i := range cfg.Fixtures.Subscriptions {
		s := &cfg.Fixtures.Subscriptions[i]
		lookup[s.RoomID] = append(lookup[s.RoomID], *s)
	}

	return &roomReadGenerator{cfg: *cfg, rng: r, zipf: z, roomSubs: lookup}
}

// Run drives the open-loop publisher until ctx cancels. Mirrors HistoryGenerator.Run:
// MaxInFlight>0 uses the batched pacer (so achieved RPS is not capped by
// single-ticker resolution); MaxInFlight<=0 selects the legacy serial path,
// retained for bisection — it will not ramp past the single-ticker ceiling.
func (g *roomReadGenerator) Run(ctx context.Context) error {
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

func (g *roomReadGenerator) requestOne(ctx context.Context) {
	roomID := g.pickRoom()
	if roomID == "" {
		return
	}
	subs := g.roomSubs[roomID]
	if len(subs) == 0 {
		return
	}
	reader := subs[g.intn(len(subs))]
	g.doRead(ctx, roomID, reader.User.Account)
}

func (g *roomReadGenerator) doRead(ctx context.Context, roomID, account string) {
	subj := subject.MessageRead(account, roomID, g.cfg.SiteID)
	// Mint a fresh X-Request-ID per request, like a real client. The requester
	// carries it on the NATS header, so server-side logs/traces for benchmark
	// traffic are correlatable.
	ctx = natsutil.WithRequestID(ctx, idgen.GenerateRequestID())
	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, nil, g.cfg.RequestTimeout)
	latency := time.Since(start)

	if err != nil {
		// Run-level cancellation isn't a real failure — the run is draining.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(classifyRequesterError(err), latency)
		return
	}

	var parsed roomReadReply
	if jerr := json.Unmarshal(reply, &parsed); jerr != nil || parsed.Status != "accepted" {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	g.cfg.Collector.RecordSample(RoomReadSample{Latency: latency, At: time.Now()})
}

func (g *roomReadGenerator) pickRoom() string {
	rooms := g.cfg.Fixtures.Rooms
	if len(rooms) == 0 {
		return ""
	}
	g.rngMu.Lock()
	idx := g.zipf.Uint64()
	g.rngMu.Unlock()
	if int(idx) >= len(rooms) {
		idx = uint64(len(rooms) - 1)
	}
	return rooms[idx].ID
}

func (g *roomReadGenerator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}
