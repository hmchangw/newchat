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

// threadReadReply is the minimal projection of the GetThreadMessages reply the
// generator needs to validate it: a non-empty top-level "error" is the errcode
// envelope (failure); a present "parentMessage" marks a healthy success. This is
// stricter than the history workload, which records any non-transport reply as a
// success sample.
type threadReadReply struct {
	Error         string          `json:"error"`
	ParentMessage json.RawMessage `json:"parentMessage"`
}

// threadReadCallerSet bundles a room's subscribers and its seeded thread parents.
type threadReadCallerSet struct {
	subscribers []model.Subscription
	parents     []ThreadParentRef
}

// threadReadGeneratorConfig bundles every dependency the generator needs.
type threadReadGeneratorConfig struct {
	Fixtures       *HistoryFixtures
	SiteID         string
	Rate           int
	PageLimit      int
	RequestTimeout time.Duration
	Requester      HistoryRequester
	Collector      *threadReadCollector
	MaxInFlight    int
}

// threadReadGenerator drives the open-loop GetThreadMessages request/reply loop.
// Rooms are picked with a Zipf skew (hot rooms read more often); a random
// subscriber of the chosen room is the caller, reading a random seeded thread.
// Rooms with no seeded thread parents are skipped and counted via RecordNoParents, not excluded from the pick set.
type threadReadGenerator struct {
	cfg threadReadGeneratorConfig

	rngMu sync.Mutex
	rng   *rand.Rand
	zipf  *rand.Zipf

	roomLookup map[string]*threadReadCallerSet
}

// newThreadReadGenerator constructs a generator seeded from `seed`. Zipf params
// (s=1.1, v=1.0) match the history/room-read workloads for consistent skew.
func newThreadReadGenerator(cfg *threadReadGeneratorConfig, seed int64) *threadReadGenerator {
	r := rand.New(rand.NewSource(seed))
	rooms := cfg.Fixtures.Fixtures.Rooms
	roomCount := len(rooms)
	if roomCount < 1 {
		roomCount = 1
	}
	zipfN := uint64(roomCount - 1)
	if zipfN < 1 {
		zipfN = 1
	}
	z := rand.NewZipf(r, 1.1, 1.0, zipfN)

	lookup := make(map[string]*threadReadCallerSet, roomCount)
	for i := range rooms {
		lookup[rooms[i].ID] = &threadReadCallerSet{}
	}
	for i := range cfg.Fixtures.Fixtures.Subscriptions {
		s := &cfg.Fixtures.Fixtures.Subscriptions[i]
		if set, ok := lookup[s.RoomID]; ok {
			set.subscribers = append(set.subscribers, *s)
		}
	}
	for roomID, refs := range cfg.Fixtures.ThreadParents {
		if set, ok := lookup[roomID]; ok {
			set.parents = refs
		}
	}

	return &threadReadGenerator{cfg: *cfg, rng: r, zipf: z, roomLookup: lookup}
}

// Run drives the open-loop requester until ctx cancels. MaxInFlight>0 uses the
// batched pacer; MaxInFlight<=0 selects the legacy serial path (bisection only).
func (g *threadReadGenerator) Run(ctx context.Context) error {
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

func (g *threadReadGenerator) requestOne(ctx context.Context) {
	roomID := g.pickRoom()
	if roomID == "" {
		return
	}
	set := g.roomLookup[roomID]
	if set == nil || len(set.subscribers) == 0 {
		return
	}
	if len(set.parents) == 0 {
		g.cfg.Collector.RecordNoParents()
		return
	}
	caller := set.subscribers[g.intn(len(set.subscribers))]
	parent := set.parents[g.intn(len(set.parents))]
	g.doThreadRead(ctx, roomID, caller.User.Account, parent.MessageID)
}

func (g *threadReadGenerator) doThreadRead(ctx context.Context, roomID, account, parentID string) {
	body := getThreadMessagesRequest{ThreadMessageID: parentID, Limit: g.cfg.PageLimit}
	data, err := json.Marshal(body)
	if err != nil {
		g.cfg.Collector.RecordBadReply(0)
		return
	}
	subj := subject.MsgThread(account, roomID, g.cfg.SiteID)
	// Mint a fresh X-Request-ID per request so server-side logs/traces for
	// benchmark traffic are correlatable.
	ctx = natsutil.WithRequestID(ctx, idgen.GenerateRequestID())
	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, data, g.cfg.RequestTimeout)
	latency := time.Since(start)

	if err != nil {
		// Run-level cancellation isn't a real failure — the run is draining.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(classifyRequesterError(err), latency)
		return
	}

	var parsed threadReadReply
	if jerr := json.Unmarshal(reply, &parsed); jerr != nil {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	if parsed.Error != "" {
		g.cfg.Collector.RecordError(errClassReply, latency)
		return
	}
	if len(parsed.ParentMessage) == 0 || string(parsed.ParentMessage) == "null" {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	g.cfg.Collector.RecordSample(threadReadSample{Latency: latency, At: time.Now()})
}

func (g *threadReadGenerator) pickRoom() string {
	rooms := g.cfg.Fixtures.Fixtures.Rooms
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

func (g *threadReadGenerator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}
