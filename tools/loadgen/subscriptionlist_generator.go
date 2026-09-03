package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sort"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// validSubscriptionListTypes mirrors user-service's own accepted set. An
// unknown type is rejected there as a 400, so every request in the ramp would
// fail identically — worth catching at flag-parse time instead.
var validSubscriptionListTypes = map[string]bool{"current": true, "rooms": true, "apps": true}

// workloadSupportedListTypes is the subset these fixtures can actually serve.
// `apps` is missing deliberately: BuildFixtures creates only channel and dm
// rooms, while `apps` matches subscribed botDM rows alone, so every reply would
// be an empty page — recorded as a failure and contributing no latency, which
// reports a 100% failure rate against a healthy service. Seeding botDM rows
// needs bot users and app records, a different fixture family; until then the
// flag rejects the value instead of measuring nothing.
var workloadSupportedListTypes = map[string]bool{"current": true, "rooms": true}

// SubscriptionListRequester is the narrow request/reply transport seam. The
// production implementation reuses newNATSHistoryRequester (from
// history_main.go) — both interfaces share the same Request method signature;
// tests inject a recorder.
type SubscriptionListRequester interface {
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)
}

// subscriptionListGeneratorConfig bundles every dependency the generator needs.
// ListType/Limit/IncludeLastMessage shape the request body; a nil
// IncludeLastMessage is omitted from the wire, which user-service reads as true.
type subscriptionListGeneratorConfig struct {
	Fixtures           *Fixtures
	SiteID             string
	Rate               int
	RequestTimeout     time.Duration
	Requester          SubscriptionListRequester
	Collector          *SubscriptionListCollector
	MaxInFlight        int
	ListType           string
	Limit              int
	IncludeLastMessage *bool
}

// subscriptionListGenerator drives the open-loop subscription.list
// request/reply loop. Accounts are picked uniformly rather than with the Zipf
// skew room-read uses: a client cold-opens its sidebar once per connect, so
// there is no hot-account concentration to model — the skew that matters here
// is across rooms within one account's page, and the fixtures already carry it.
type subscriptionListGenerator struct {
	cfg subscriptionListGeneratorConfig

	rngMu sync.Mutex
	rng   *rand.Rand

	accounts []string
	// body is identical for every request, so it is marshalled once. Re-encoding
	// it per call would put the load box's CPU on the critical path and surface
	// as emit underrun — an INCONCLUSIVE ramp rather than a measurement.
	body    []byte
	bodyErr error
}

// newSubscriptionListGenerator constructs a generator seeded from `seed`. The
// account set is sorted before use so a given seed replays the same sequence
// regardless of fixture iteration order.
func newSubscriptionListGenerator(cfg *subscriptionListGeneratorConfig, seed int64) *subscriptionListGenerator {
	seen := make(map[string]struct{}, len(cfg.Fixtures.Subscriptions))
	var accounts []string
	for i := range cfg.Fixtures.Subscriptions {
		account := cfg.Fixtures.Subscriptions[i].User.Account
		if _, ok := seen[account]; ok || account == "" {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)

	body, err := json.Marshal(soakSubscriptionListRequest{
		Type:               cfg.ListType,
		Limit:              cfg.Limit,
		IncludeLastMessage: cfg.IncludeLastMessage,
	})

	return &subscriptionListGenerator{
		cfg:      *cfg,
		rng:      rand.New(rand.NewSource(seed)),
		accounts: accounts,
		body:     body,
		bodyErr:  err,
	}
}

// Run drives the open-loop requester until ctx cancels. Mirrors
// roomReadGenerator.Run: MaxInFlight>0 uses the batched pacer (so achieved RPS
// is not capped by single-ticker resolution); MaxInFlight<=0 selects the legacy
// serial path, retained for bisection.
func (g *subscriptionListGenerator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	// Reported once here rather than as one bad reply per request: a body that
	// cannot be built is a configuration fault, not a service failure.
	if g.bodyErr != nil {
		return fmt.Errorf("marshal subscription.list body: %w", g.bodyErr)
	}
	if g.cfg.MaxInFlight <= 0 {
		serialDispatch(ctx, g.cfg.Rate, g.requestOne)
		return nil
	}
	pacedDispatch(ctx, g.cfg.Rate, g.cfg.MaxInFlight,
		g.cfg.Collector.RecordUnderrun, g.cfg.Collector.RecordSaturation, g.requestOne)
	return nil
}

func (g *subscriptionListGenerator) requestOne(ctx context.Context) {
	account := g.pickAccount()
	if account == "" {
		return
	}
	g.doList(ctx, account)
}

func (g *subscriptionListGenerator) doList(ctx context.Context, account string) {
	subj := subject.UserSubscriptionList(account, g.cfg.SiteID)
	// Mint a fresh X-Request-ID per request, like a real client, so server-side
	// logs and traces for benchmark traffic are correlatable.
	ctx = natsutil.WithRequestID(ctx, idgen.GenerateRequestID())

	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, g.body, g.cfg.RequestTimeout)
	latency := time.Since(start)

	if err != nil {
		// Run-level cancellation isn't a real failure — the run is draining.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(classifyRequesterError(err), latency)
		return
	}
	g.classifyReply(reply, latency)
}

// classifyReply splits a delivered reply four ways. The distinction that earns
// its keep is the last one: an errcode envelope and a `{}` both decode to zero
// rows, and folding either into "empty page" would report a service outage or a
// contract break as a fast, healthy, empty sidebar.
func (g *subscriptionListGenerator) classifyReply(reply []byte, latency time.Duration) {
	if _, ok := errcode.Parse(reply); ok {
		g.cfg.Collector.RecordError(errClassReply, latency)
		return
	}
	var parsed soakSubscriptionListResponse
	// Presence of the collection, not its length: a genuinely empty sidebar
	// replies with an empty array, while `{}` leaves it nil and is a contract
	// violation.
	if err := json.Unmarshal(reply, &parsed); err != nil || parsed.Subscriptions == nil {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	if len(parsed.Subscriptions) == 0 {
		g.cfg.Collector.RecordEmptyPage(latency)
		return
	}
	g.cfg.Collector.RecordSample(SubscriptionListSample{
		Latency: latency,
		Rows:    len(parsed.Subscriptions),
		HasMore: parsed.HasMore,
	})
}

func (g *subscriptionListGenerator) pickAccount() string {
	if len(g.accounts) == 0 {
		return ""
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.accounts[g.rng.Intn(len(g.accounts))]
}
