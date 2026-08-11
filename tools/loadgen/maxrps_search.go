package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// The search workload drives search-service's request/reply endpoints so SLO-7
// ("search returns ok / eligible search requests", docs/load-testing/system/sli-slo.md
// §5) can be measured under load.
//
// SLO-7 is already computable server-side from
// search_service_requests_total{kind,status}. SLO-8 (latency) is not: the
// duration histogram carries only a `kind` attribute, with no status, so
// successful requests cannot be isolated from failed ones — that needs the
// label from §8 P4. This workload drives the traffic for both; only SLO-7 can
// be scored from the service side until P4 lands.
//
// §5 also names a blind spot this workload does not close: the SLO-7
// denominator is search-service-local, so a total outage reads as no traffic
// rather than as failures. Client-side, a dead service shows up here as
// request timeouts classified as failures, which is a useful cross-check but
// not a substitute for the health-check/prober backstop §5 calls for.

type searchEndpoint int

const (
	searchMessages searchEndpoint = iota
	searchRooms
	searchUsers
)

func (e searchEndpoint) String() string {
	switch e {
	case searchMessages:
		return "messages"
	case searchRooms:
		return "rooms"
	default:
		return "users"
	}
}

// searchEndpoints is the iteration order used for per-endpoint reporting.
var searchEndpoints = []searchEndpoint{searchMessages, searchRooms, searchUsers}

// searchSubjectFor builds the request subject for one endpoint.
func searchSubjectFor(e searchEndpoint, account, siteID string) string {
	switch e {
	case searchMessages:
		return subject.SearchMessages(account, siteID)
	case searchRooms:
		return subject.SearchRooms(account, siteID)
	default:
		return subject.SearchUsers(account, siteID)
	}
}

// SearchMix is the request share per endpoint, as parsed from --search-mix.
type SearchMix struct {
	Messages, Rooms, Users int
}

func (m SearchMix) total() int { return m.Messages + m.Rooms + m.Users }

// pick maps iteration i onto an endpoint, deterministically and in proportion.
func (m SearchMix) pick(i int) searchEndpoint {
	slot := i % m.total()
	if slot < m.Messages {
		return searchMessages
	}
	if slot < m.Messages+m.Rooms {
		return searchRooms
	}
	return searchUsers
}

// ParseSearchMix parses "messages:N,rooms:M,users:K".
func ParseSearchMix(s string) (SearchMix, error) {
	pairs, err := parsePairs(s, []string{"messages", "rooms", "users"})
	if err != nil {
		return SearchMix{}, fmt.Errorf("parse search mix: %w", err)
	}
	mix := SearchMix{Messages: pairs["messages"], Rooms: pairs["rooms"], Users: pairs["users"]}
	if mix.total() <= 0 {
		return SearchMix{}, fmt.Errorf("search mix totals must be > 0")
	}
	return mix, nil
}

// classifySearchReply maps one request/reply round onto an eligibility class.
// A transport error (which for a request means the reply never arrived within
// the timeout) is a failure, not an absent sample — the same rule the messages
// workload applies to dropped broadcasts.
//
// A reply that is neither an errcode envelope nor a decodable success payload
// is a failure too. Treating "not an error envelope" as success let an empty,
// truncated or contract-incompatible body improve both SLO-7 and SLO-8, which
// is the same bias as the rest of this file: a response the client could not
// consume must not read as a served request.
func classifySearchReply(e searchEndpoint, body []byte, reqErr error) sloOutcome {
	if reqErr != nil {
		return outcomeFailed
	}
	if ec, ok := errcode.Parse(body); ok {
		return classifyErrcode(ec.Code)
	}
	if !decodesAsSearchResponse(e, body) {
		return outcomeFailed
	}
	return outcomeGood
}

// decodesAsSearchResponse reports whether body is the success payload this
// endpoint is contracted to return. The check is the presence of the top-level
// collection, not its length: a zero-hit search legitimately replies with an
// empty array, while `{}` or `{"unexpected":1}` leaves it nil and is a contract
// violation rather than an empty result.
func decodesAsSearchResponse(e searchEndpoint, body []byte) bool {
	switch e {
	case searchMessages:
		var resp model.SearchMessagesResponse
		return json.Unmarshal(body, &resp) == nil && resp.Messages != nil
	case searchRooms:
		var resp model.SearchRoomsResponse
		return json.Unmarshal(body, &resp) == nil && resp.Rooms != nil
	default:
		// search.users replies with a bare JSON array, not an object.
		var users []model.SearchUser
		return json.Unmarshal(body, &users) == nil && users != nil
	}
}

// searchQueryGen builds request payloads. Query terms vary per request: a
// constant term would be served from Elasticsearch's query cache after the
// first hit and measure nothing.
type searchQueryGen struct {
	mu   sync.Mutex
	rng  *rand.Rand
	size int
}

func newSearchQueryGen(seed int64) *searchQueryGen {
	return &searchQueryGen{rng: rand.New(rand.NewSource(seed)), size: 20} // #nosec G404 -- deterministic fixture generation, not security
}

// searchTerms are ordinary vocabulary tokens, used verbatim. They used to be
// suffixed with the iteration number for variety, which turned "hello" into
// "hello0" — a distinct term under standard or keyword analysis, so no indexed
// document could match and the workload only ever exercised the zero-hit path,
// skipping retrieval and enrichment entirely. Variety now comes from the term
// and filter combinations below, which keeps every query a real term.
//
// These do NOT match loadgen's own seeded corpus: the message generator fills
// bodies with a run of 'x'. As the README states, the search workload reads
// whatever the index already holds, so a meaningful run needs an index carrying
// representative data — a prerequisite of the workload, not of these terms.
var searchTerms = []string{
	"hello", "meeting", "report", "update", "review", "deploy",
	"status", "issue", "release", "ticket", "design", "budget",
}

var searchRoomTypes = []string{"", "all", "channel", "dm"}

// build returns the JSON body for one request. i selects the second term so a
// run is reproducible for a given --seed.
func (g *searchQueryGen) build(e searchEndpoint, i int) []byte {
	g.mu.Lock()
	term := searchTerms[g.rng.Intn(len(searchTerms))]
	roomType := searchRoomTypes[g.rng.Intn(len(searchRoomTypes))]
	size := g.size
	g.mu.Unlock()

	// Consecutive queries differ by pairing a second real term with the first,
	// rather than by mutating either into a token no document can contain.
	query := term
	if second := searchTerms[i%len(searchTerms)]; second != term {
		query = term + " " + second
	}

	// Typed request structs from pkg/model, not map[string]any: CLAUDE.md
	// requires NATS payloads to use them, and it makes a field rename in the
	// contract a compile error here instead of a silently-rejected request.
	var (
		body []byte
		err  error
	)
	switch e {
	case searchMessages:
		body, err = json.Marshal(model.SearchMessagesRequest{Query: query, Size: size})
	case searchRooms:
		body, err = json.Marshal(model.SearchRoomsRequest{Query: query, RoomType: roomType, Size: size})
	default:
		// search.users pages a third-party endpoint and uses Limit, not Size.
		body, err = json.Marshal(model.SearchUsersRequest{Query: query, Limit: size})
	}
	if err != nil {
		// Every branch marshals a struct of strings and ints, so this is
		// unreachable; an empty body would be rejected as bad_request and
		// excluded rather than silently counted as a success.
		return nil
	}
	return body
}

// searchCollector accumulates one step's outcomes per endpoint. Only successes
// are timed because SLO-8's valid denominator is successful searches. The
// verdict counts successful searches within SLO-8's own bound directly;
// endpoint percentiles remain diagnostic.
type searchCollector struct {
	mu         sync.Mutex
	samples    map[searchEndpoint][]time.Duration
	good       map[searchEndpoint]int
	failed     map[searchEndpoint]int
	excluded   map[searchEndpoint]int
	saturation int
	underrun   int
}

func newSearchCollector() *searchCollector {
	return &searchCollector{
		samples:  map[searchEndpoint][]time.Duration{},
		good:     map[searchEndpoint]int{},
		failed:   map[searchEndpoint]int{},
		excluded: map[searchEndpoint]int{},
	}
}

func (c *searchCollector) Record(e searchEndpoint, o sloOutcome, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch o {
	case outcomeGood:
		c.good[e]++
		c.samples[e] = append(c.samples[e], d)
	case outcomeFailed:
		c.failed[e]++
	case outcomeExcluded:
		c.excluded[e]++
	}
}

// RecordSaturation / RecordUnderrun tally load-box limits, never service
// failures: saturation means the in-flight pool was full, underrun means the
// pacer could not release on schedule. Both feed the INCONCLUSIVE guard.
func (c *searchCollector) RecordSaturation() {
	c.mu.Lock()
	c.saturation++
	c.mu.Unlock()
}

func (c *searchCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	c.underrun += n
	c.mu.Unlock()
}

func (c *searchCollector) Saturation() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

func (c *searchCollector) Underrun() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}

func (c *searchCollector) Samples(e searchEndpoint) []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.samples[e]...)
}

func (c *searchCollector) totals() (good, failed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range searchEndpoints {
		good += c.good[e]
		failed += c.failed[e]
	}
	return good, failed
}

// buildSearchInputs assembles step inputs. Endpoint percentiles stay diagnostic
// because the cost models differ; the spec's two aggregate ratios carry the
// verdict — SLO-7 (successes / eligible requests) and SLO-8 (successful
// searches within SLO-8's bound / successful searches).
//
// The failed/attempted gate is diagnostic for the same reason it is on the
// login path: SLO-7 already scores availability, at its own 99.5% objective.
// Leaving the generic gate live would let a step inside its SLO trip on the
// tighter --slo-error-rate default.
func buildSearchInputs(targetRPS int, hold time.Duration, c *searchCollector, slo7, slo8 sloRatio) rpsStepInputs {
	good, failed := c.totals()
	allSuccessful := make([]time.Duration, 0, good)
	in := rpsStepInputs{
		TargetRPS:               targetRPS,
		Hold:                    hold,
		AttemptedOps:            good + failed,
		FailedOps:               failed,
		Saturation:              c.Saturation(),
		EmitUnderrun:            c.Underrun(),
		ErrorRateDiagnosticOnly: true,
	}
	for _, e := range searchEndpoints {
		samples := c.Samples(e)
		allSuccessful = append(allSuccessful, samples...)
		in.Latencies = append(in.Latencies, seriesSamples{
			Name: e.String(), Samples: samples, DiagnosticOnly: true,
		})
	}
	in.EventRatios = []eventRatioInput{{
		// SLO-7 is availability, not latency: every success is good, against
		// every eligible request. Excluded outcomes have already left both
		// sides in the collector.
		Name: "SLO-7", Valid: good + failed, SuccessfulLatencies: allSuccessful,
		Target: slo7.Target, Kind: ratioSuccess,
	}}
	// SLO-8 is only declared when the step actually produced successes. Its
	// denominator is successful searches, so a total outage would build it with
	// Valid=0 — an invalid SLI, and invalid SLIs outrank a TRIP. That let the
	// one verdict a dead search service must produce (SLO-7 at 0%) be masked by
	// an undefined latency ratio, exactly inverting the intended precedence.
	// With no successes there is no latency statement to make; the availability
	// ratio carries the step on its own.
	if good > 0 {
		in.EventRatios = append(in.EventRatios, eventRatioInput{
			Name: "SLO-8", Valid: good, SuccessfulLatencies: allSuccessful,
			Target: slo8.Target, Kind: ratioLatency, Bound: slo8.Bound,
		})
	}
	return in
}

// searchWorkload drives search-service request/reply at a given RPS.
type searchWorkload struct {
	nc             *nats.Conn
	fixtures       Fixtures
	mix            SearchMix
	queries        *searchQueryGen
	siteID         string
	requestTimeout time.Duration
	maxInFlt       int
	slo7, slo8     sloRatio
}

func (w *searchWorkload) Label() string { return "search" }

// newSearchWorkload dials NATS and builds the workload from the message
// preset's fixtures, so searches run as the same accounts the rest of the load
// exercises. No seeding step: search reads whatever the index already holds.
func newSearchWorkload(cfg *config, preset *Preset, seed int64, mix SearchMix, timeout time.Duration, slo7, slo8 sloRatio) (*searchWorkload, func(), error) {
	nc, err := dialNATS(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	f := BuildFixtures(preset, seed, cfg.SiteID)
	if len(f.Subscriptions) == 0 {
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("preset %s produced no accounts to search as", preset.Name)
	}
	w := &searchWorkload{
		nc: nc.NatsConn(), fixtures: f, mix: mix,
		queries: newSearchQueryGen(seed), siteID: cfg.SiteID,
		requestTimeout: timeout, maxInFlt: max(1, cfg.MaxInFlight),
		slo7: slo7, slo8: slo8,
	}
	return w, func() { _ = nc.Drain() }, nil
}

// RunStep paces searches at targetRPS for warmup then hold, measuring only the
// hold window.
func (w *searchWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		if err := w.drive(ctx, targetRPS, warmup, newSearchCollector()); err != nil {
			return rpsStepInputs{}, err
		}
	}
	c := newSearchCollector()
	if err := w.drive(ctx, targetRPS, hold, c); err != nil {
		return rpsStepInputs{}, err
	}
	return buildSearchInputs(targetRPS, hold, c, w.slo7, w.slo8), nil
}

// drive emits at targetRPS for d, recording every outcome into c.
//
// Uses the shared batched pacer rather than a ticker per request: at the top
// of this workload's default ramp the per-request interval is 500µs, below
// what the Go runtime can schedule, and it silently coalesces ticks — the load
// box would cap the achieved rate long before search-service did.
func (w *searchWorkload) drive(ctx context.Context, targetRPS int, d time.Duration, c *searchCollector) error {
	runCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var seq atomic.Int64
	pacedDispatch(runCtx, targetRPS, w.maxInFlt,
		c.RecordUnderrun,
		c.RecordSaturation,
		func(reqCtx context.Context) {
			i := int(seq.Add(1) - 1)
			endpoint := w.mix.pick(i)
			sub := w.fixtures.Subscriptions[i%len(w.fixtures.Subscriptions)]
			subj := searchSubjectFor(endpoint, sub.User.Account, w.siteID)

			// RequestWithContext, not Request: the plain form observes only
			// requestTimeout and ignores reqCtx entirely, so at the step
			// boundary up to MaxInFlight requests kept running for the full
			// timeout — delaying shutdown, holding inboxes, and letting a late
			// reply be recorded into a hold window that had already closed.
			// The child deadline keeps a genuine per-request timeout a failure
			// while step cancellation propagates immediately.
			callCtx, callCancel := context.WithTimeout(reqCtx, w.requestTimeout)
			start := time.Now()
			msg, err := w.nc.RequestWithContext(callCtx, subj, w.queries.build(endpoint, i))
			elapsed := time.Since(start)
			callCancel()
			// A request cut short because the step window closed is the harness
			// ending the measurement, not the service failing. Excluded for the
			// same reason as the login path; a genuine reply timeout leaves
			// reqCtx live and stays a failure.
			if err != nil && reqCtx.Err() != nil {
				c.Record(endpoint, outcomeExcluded, elapsed)
				return
			}
			var data []byte
			if msg != nil {
				data = msg.Data
			}
			c.Record(endpoint, classifySearchReply(endpoint, data, err), elapsed)
		})

	// The parent being cancelled is a real abort; this step's own deadline
	// expiring is the normal end of the window.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
