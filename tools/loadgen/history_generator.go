package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// EndpointMix weights how often each history endpoint is chosen per request.
// The two integers form a ratio; they need not sum to 100.
type EndpointMix struct {
	History int
	Thread  int
}

func (m EndpointMix) total() int { return m.History + m.Thread }

// BeforeMode weights how the `before` cursor is chosen on LoadHistory.
// Open mode sets Before=nil (server uses now). Scrollback chains the cursor
// from the prior reply's oldest createdAt for ScrollbackPages requests, then
// resets to a fresh open page.
type BeforeMode struct {
	Open       int
	Scrollback int
}

func (m BeforeMode) total() int { return m.Open + m.Scrollback }

// ParseEndpointMix parses "history:N,thread:M".
func ParseEndpointMix(s string) (EndpointMix, error) {
	pairs, err := parsePairs(s, []string{"history", "thread"})
	if err != nil {
		return EndpointMix{}, fmt.Errorf("parse endpoint mix: %w", err)
	}
	mix := EndpointMix{History: pairs["history"], Thread: pairs["thread"]}
	if mix.total() <= 0 {
		return EndpointMix{}, fmt.Errorf("endpoint mix totals must be > 0")
	}
	return mix, nil
}

// ParseBeforeMode parses "open:N,scrollback:M".
func ParseBeforeMode(s string) (BeforeMode, error) {
	pairs, err := parsePairs(s, []string{"open", "scrollback"})
	if err != nil {
		return BeforeMode{}, fmt.Errorf("parse before mode: %w", err)
	}
	mode := BeforeMode{Open: pairs["open"], Scrollback: pairs["scrollback"]}
	if mode.total() <= 0 {
		return BeforeMode{}, fmt.Errorf("before mode totals must be > 0")
	}
	return mode, nil
}

// parsePairs splits a comma-delimited "key:int,key:int" string and verifies
// that exactly the expected keys are present.
func parsePairs(s string, expected []string) (map[string]int, error) {
	if s == "" {
		return nil, fmt.Errorf("empty input")
	}
	out := map[string]int{}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("malformed pair %q", part)
		}
		k := strings.TrimSpace(kv[0])
		if _, exists := out[k]; exists {
			return nil, fmt.Errorf("duplicate key %q", k)
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("bad int in %q: %w", part, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("value in %q must be >= 0", part)
		}
		out[k] = n
	}
	if len(out) != len(expected) {
		return nil, fmt.Errorf("expected exactly keys %v, got %d", expected, len(out))
	}
	for _, k := range expected {
		if _, ok := out[k]; !ok {
			return nil, fmt.Errorf("missing key %q", k)
		}
	}
	return out, nil
}

// loadHistoryRequest mirrors history-service/internal/models.LoadHistoryRequest.
// Replicated here because loadgen can't import the internal models package.
type loadHistoryRequest struct {
	Before *int64 `json:"before,omitempty"`
	Limit  int    `json:"limit"`
}

// loadHistoryReply captures the minimum payload fields the load generator
// needs to advance its scrollback cursor: oldest CreatedAt in the returned page.
type loadHistoryReply struct {
	Messages []struct {
		CreatedAt time.Time `json:"createdAt"`
	} `json:"messages"`
}

// getThreadMessagesRequest mirrors models.GetThreadMessagesRequest.
type getThreadMessagesRequest struct {
	ThreadMessageID string `json:"threadMessageId"`
	Limit           int    `json:"limit"`
}

// HistoryRequester is the narrow consumer interface for the request/reply
// transport. The production implementation wraps nats.Conn.RequestWithContext;
// tests inject a recorder.
type HistoryRequester interface {
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)
}

// HistoryGeneratorConfig bundles every dependency the generator needs.
type HistoryGeneratorConfig struct {
	Preset          *HistoryPreset
	Fixtures        *HistoryFixtures
	SiteID          string
	Rate            int
	Mix             EndpointMix
	BeforeMode      BeforeMode
	ScrollbackPages int
	PageLimit       int
	RequestTimeout  time.Duration
	Requester       HistoryRequester
	Collector       *HistoryCollector
	MaxInFlight     int
}

// HistoryGenerator drives the request/reply loop. Open-loop ticker dispatches
// each request to a bounded goroutine pool; saturation is tallied so the
// run-time can report it but the ticker stays punctual.
type HistoryGenerator struct {
	cfg HistoryGeneratorConfig

	rngMu sync.Mutex
	rng   *rand.Rand
	zipf  *rand.Zipf

	// roomLookup maps roomID -> (subscribers, thread-parents). Built once at
	// generator construction so the publish path stays allocation-light.
	roomLookup map[string]*roomCallerSet

	// scrollback state is keyed on (account, room) and accessed only from the
	// publish path. Guarded by its own mutex.
	chainMu sync.Mutex
	chains  map[chainKey]*chainState
}

type chainKey struct {
	account string
	room    string
}

type chainState struct {
	pageIdx    int       // 0..ScrollbackPages-1; 0 = open mode
	lastCursor time.Time // oldest createdAt from prior reply
}

type roomCallerSet struct {
	subscribers []model.Subscription
	parents     []ThreadParentRef
}

// NewHistoryGenerator constructs a generator seeded from `seed`. The Zipf
// distribution uses a moderate s=1.1, v=1.0 over the room count — enough skew
// to surface hot-room partition effects without starving the long tail
// entirely.
func NewHistoryGenerator(cfg *HistoryGeneratorConfig, seed int64) *HistoryGenerator {
	r := rand.New(rand.NewSource(seed))
	roomCount := len(cfg.Fixtures.Fixtures.Rooms)
	if roomCount < 1 {
		roomCount = 1
	}
	// rand.NewZipf is undefined for n<1; cap at 1 so Uint64() returns 0.
	zipfN := uint64(roomCount - 1)
	if zipfN < 1 {
		zipfN = 1
	}
	z := rand.NewZipf(r, 1.1, 1.0, zipfN)

	lookup := make(map[string]*roomCallerSet, roomCount)
	for i := range cfg.Fixtures.Fixtures.Rooms {
		lookup[cfg.Fixtures.Fixtures.Rooms[i].ID] = &roomCallerSet{}
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

	return &HistoryGenerator{
		cfg:        *cfg,
		rng:        r,
		zipf:       z,
		roomLookup: lookup,
		chains:     map[chainKey]*chainState{},
	}
}

// Run drives the open-loop requester until ctx cancels. Mirrors Generator.Run's
// shape so operators experience consistent semantics across workloads:
// MaxInFlight > 0 drives the batched pacer (runPaced); MaxInFlight == 0 keeps
// the legacy serial-on-ticker path for bisection (runSerial).
func (g *HistoryGenerator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if g.cfg.MaxInFlight <= 0 {
		return g.runSerial(ctx)
	}
	return g.runPaced(ctx)
}

// runSerial is the legacy one-request-per-tick path (MaxInFlight == 0), retained
// for bisection; it will not ramp past the single-ticker ceiling.
func (g *HistoryGenerator) runSerial(ctx context.Context) error {
	serialDispatch(ctx, g.cfg.Rate, g.requestOne)
	return nil
}

// runPaced drives the batched pacer into a bounded worker pool so achieved RPS
// is not capped by single-ticker resolution. A full pool is recorded as
// saturation (raise MaxInFlight); events the pacer could not release on schedule
// as underrun (the load box could not keep up).
func (g *HistoryGenerator) runPaced(ctx context.Context) error {
	pacedDispatch(ctx, g.cfg.Rate, g.cfg.MaxInFlight,
		g.cfg.Collector.RecordUnderrun, g.cfg.Collector.RecordSaturation, g.requestOne)
	return nil
}

func (g *HistoryGenerator) requestOne(ctx context.Context) {
	roomID := g.pickRoom()
	if roomID == "" {
		return
	}
	set := g.roomLookup[roomID]
	if set == nil || len(set.subscribers) == 0 {
		return
	}
	caller := set.subscribers[g.intn(len(set.subscribers))]

	endpoint := g.pickEndpoint()
	if endpoint == HistoryEndpointThread && len(set.parents) == 0 {
		g.cfg.Collector.RecordNoThreadParents()
		endpoint = HistoryEndpointHistory
	}

	switch endpoint {
	case HistoryEndpointThread:
		g.doThreadRequest(ctx, roomID, caller.User.Account, set.parents)
	default:
		g.doHistoryRequest(ctx, roomID, caller.User.Account)
	}
}

func (g *HistoryGenerator) doHistoryRequest(ctx context.Context, roomID, account string) {
	mode := g.pickBeforeMode()
	body := loadHistoryRequest{Limit: g.cfg.PageLimit}

	key := chainKey{account: account, room: roomID}
	if mode == beforeModeScrollback {
		g.chainMu.Lock()
		st := g.chains[key]
		if st == nil {
			// Page 1 of a new scrollback chain — still open mode. The reply
			// seeds the cursor for subsequent pages via advanceChain.
			g.chains[key] = &chainState{}
		} else if !st.lastCursor.IsZero() {
			ms := st.lastCursor.UTC().UnixMilli()
			body.Before = &ms
		}
		g.chainMu.Unlock()
	} else {
		// Open mode resets any in-flight chain.
		g.chainMu.Lock()
		delete(g.chains, key)
		g.chainMu.Unlock()
	}

	data, err := json.Marshal(body)
	if err != nil {
		g.cfg.Collector.RecordBadRequest(HistoryEndpointHistory)
		return
	}
	subj := subject.MsgHistory(account, roomID, g.cfg.SiteID)
	g.issue(ctx, HistoryEndpointHistory, subj, data, key, mode)
}

func (g *HistoryGenerator) doThreadRequest(ctx context.Context, roomID, account string, parents []ThreadParentRef) {
	parent := parents[g.intn(len(parents))]
	body := getThreadMessagesRequest{ThreadMessageID: parent.MessageID, Limit: g.cfg.PageLimit}
	data, err := json.Marshal(body)
	if err != nil {
		g.cfg.Collector.RecordBadRequest(HistoryEndpointThread)
		return
	}
	subj := subject.MsgThread(account, roomID, g.cfg.SiteID)
	g.issue(ctx, HistoryEndpointThread, subj, data, chainKey{}, beforeModeOpen)
}

// issue performs the request/reply round-trip and records the sample.
// For history requests in scrollback mode, advances the chain on success.
func (g *HistoryGenerator) issue(ctx context.Context, endpoint HistoryEndpoint, subj string, data []byte, key chainKey, mode beforeMode) {
	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, data, g.cfg.RequestTimeout)
	latency := time.Since(start)

	if err != nil {
		// When the run-level ctx is the cause, the request didn't really
		// fail — the test/run is shutting down. Counting it as a timeout
		// would inflate the error tally on short runs (PR #230's CI flake)
		// and conflate transport timeouts with shutdown drain. A per-request
		// DeadlineExceeded with outer ctx still alive is a real timeout and
		// is recorded below.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(endpoint, classifyRequesterError(err), latency)
		return
	}

	sample := HistorySample{
		Endpoint:     endpoint,
		Latency:      latency,
		PayloadBytes: len(reply),
		At:           time.Now(),
	}

	// Parse the reply if we need to advance the scrollback chain.
	if endpoint == HistoryEndpointHistory && mode == beforeModeScrollback {
		var parsed loadHistoryReply
		if err := json.Unmarshal(reply, &parsed); err != nil {
			g.cfg.Collector.RecordError(endpoint, errClassBadReply, latency)
			return
		}
		g.advanceChain(key, &parsed)
		sample.PageDepthMs = pageDepthMs(&parsed)
	}

	g.cfg.Collector.RecordSample(sample)
}

func (g *HistoryGenerator) advanceChain(key chainKey, reply *loadHistoryReply) {
	g.chainMu.Lock()
	defer g.chainMu.Unlock()
	st := g.chains[key]
	if st == nil {
		st = &chainState{}
		g.chains[key] = st
	}
	// Determine the oldest createdAt in this reply; that becomes the cursor
	// for the next page.
	var oldest time.Time
	for i := range reply.Messages {
		t := reply.Messages[i].CreatedAt
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	if !oldest.IsZero() {
		st.lastCursor = oldest
	}
	st.pageIdx++
	if st.pageIdx >= g.cfg.ScrollbackPages {
		// Reset: the next request for this chain key starts fresh.
		delete(g.chains, key)
	}
}

// pageDepthMs returns the createdAt range of a reply page in milliseconds,
// used as a coarse proxy for how many Cassandra buckets the walker traversed.
func pageDepthMs(reply *loadHistoryReply) int64 {
	if len(reply.Messages) == 0 {
		return 0
	}
	min, max := reply.Messages[0].CreatedAt, reply.Messages[0].CreatedAt
	for i := 1; i < len(reply.Messages); i++ {
		t := reply.Messages[i].CreatedAt
		if t.Before(min) {
			min = t
		}
		if t.After(max) {
			max = t
		}
	}
	return max.Sub(min).Milliseconds()
}

func (g *HistoryGenerator) pickRoom() string {
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

func (g *HistoryGenerator) pickEndpoint() HistoryEndpoint {
	total := g.cfg.Mix.total()
	if total <= 0 {
		return HistoryEndpointHistory
	}
	g.rngMu.Lock()
	roll := g.rng.Intn(total)
	g.rngMu.Unlock()
	if roll < g.cfg.Mix.History {
		return HistoryEndpointHistory
	}
	return HistoryEndpointThread
}

type beforeMode int

const (
	beforeModeOpen beforeMode = iota
	beforeModeScrollback
)

func (g *HistoryGenerator) pickBeforeMode() beforeMode {
	total := g.cfg.BeforeMode.total()
	if total <= 0 {
		return beforeModeOpen
	}
	g.rngMu.Lock()
	roll := g.rng.Intn(total)
	g.rngMu.Unlock()
	if roll < g.cfg.BeforeMode.Open {
		return beforeModeOpen
	}
	return beforeModeScrollback
}

func (g *HistoryGenerator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}

// classifyRequesterError maps the requester's error to a stable error class.
// Run-level cancellation (context.Canceled with outer ctx done) is filtered
// at the call site in issue() before we get here, so this only sees errors
// that occurred while the run was still active. A per-request
// DeadlineExceeded means the RequestTimeout fired and is a real timeout.
func classifyRequesterError(err error) errClass {
	if errors.Is(err, context.DeadlineExceeded) {
		return errClassTimeout
	}
	return errClassReply
}
