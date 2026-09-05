package search

import (
	"context"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

// IndexResult is what the index probe could establish about one message. It is
// deliberately four-valued: "not due yet" and "could not determine" are both
// distinct from "not there", and only the latter is evidence of loss.
type IndexResult string

const (
	IndexFound   IndexResult = "found"
	IndexMissing IndexResult = "missing"
	IndexUnknown IndexResult = "unknown"
	// IndexTooEarly is distinct from unknown on purpose. It says the
	// answer is not due yet, which lets the reconciler reschedule to the settle
	// boundary instead of re-asking every retry interval — at the default
	// rates, polling through a 30s settle window would spend several times the
	// entire reconciliation budget on queries that cannot succeed.
	IndexTooEarly IndexResult = "too_early"
)

type Config struct {
	SiteID         string
	PageSize       int
	RequestTimeout time.Duration
	// Settle is how long after publish the index probe waits before asking.
	// Elasticsearch is refreshed asynchronously, so a hit that is absent
	// inside this window is legal rather than lost.
	Settle time.Duration
}

// Reader drives search-service and answers whether a message reached
// the search index.
//
// The read lane it serves is ordinary read traffic: latency and outcome, no
// ledger. The index probe is the part that matters for evidence —
// search-sync-worker Acks and drops a message whose payload fails to decode or
// build an action, so that loss leaves its consumer at zero pending and is
// invisible from JetStream. Asking the query side is the only way to see it.
type Reader struct {
	cfg      Config
	rpc      *rpc.Client
	recorder read.SampleRecorder
	now      func() time.Time

	mu       sync.Mutex
	rng      *rand.Rand
	accounts []string
	terms    []string
}

func New(
	cfg Config,
	topology *topology.Topology,
	rpc *rpc.Client,
	recorder read.SampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*Reader, error) {
	if topology == nil {
		return nil, fmt.Errorf("soak search reader requires a topology")
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if now == nil {
		now = time.Now
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 20
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 30 * time.Second
	}

	reader := &Reader{
		cfg: cfg, rpc: rpc, recorder: recorder, now: now, rng: rng,
		// Tokens the seeded room names carry (soak-{runId}-channel-NNNNNN), so
		// room search returns real hits. Message bodies are generated filler —
		// one repeated character, no word boundaries — so message search matches
		// nothing by construction and measures the request path and an empty
		// result set. That is the lane's purpose here: it exists to show whether
		// search-service still answers during a fault, and the recorded result
		// count makes the empty set visible rather than implied.
		terms: []string{"soak", "channel"},
	}
	for i := range topology.ActiveUsers {
		if account := topology.ActiveUsers[i].Account; account != "" {
			reader.accounts = append(reader.accounts, account)
		}
	}
	if len(reader.accounts) == 0 {
		return nil, fmt.Errorf("soak search reader requires at least one active account")
	}
	return reader, nil
}

func (r *Reader) ReadMixed(ctx context.Context) error {
	r.mu.Lock()
	messages := r.rng.Float64() < 0.7
	r.mu.Unlock()
	if messages {
		return r.SearchMessages(ctx)
	}
	return r.SearchRooms(ctx)
}

func (r *Reader) SearchMessages(ctx context.Context) error {
	account, term := r.pickQuery()
	var response model.SearchMessagesResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionSearchMessages,
		Subject: subject.SearchMessages(account, r.cfg.SiteID),
		Account: account,
		Body:    model.SearchMessagesRequest{Query: term, Size: r.cfg.PageSize},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *read.Sample) {
		sample.CountRows(len(response.Messages))
	})
}

func (r *Reader) SearchRooms(ctx context.Context) error {
	account, term := r.pickQuery()
	var response model.SearchRoomsResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionSearchRooms,
		Subject: subject.SearchRooms(account, r.cfg.SiteID),
		Account: account,
		Body:    model.SearchRoomsRequest{Query: term, Size: r.cfg.PageSize},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *read.Sample) {
		sample.CountRows(len(response.Rooms))
	})
}

// IndexedAt reports whether one message reached the search index, scoping the
// query to the message's own room so the answer does not depend on relevance
// ranking across the whole corpus.
//
// term is the query the catalogue kept for this message; the catalogue reduces
// each body to it so no verifier has to hold one.
//
// publishedAt gates the settle window. Before it elapses the probe returns
// IndexTooEarly without issuing a request, because an absent hit there is legal
// and the query would only spend a read slot to learn nothing.
func (r *Reader) IndexedAt(
	ctx context.Context,
	account, roomID, messageID, term string,
	publishedAt time.Time,
) (IndexResult, bool, error) {
	if account == "" || roomID == "" || messageID == "" {
		return IndexUnknown, false,
			fmt.Errorf("search index probe requires an account, room and message")
	}
	if r.now().UTC().Sub(publishedAt.UTC()) < r.cfg.Settle {
		return IndexTooEarly, false, nil
	}

	var response model.SearchMessagesResponse
	err := r.call(ctx, rpc.Request{
		Action:  rpc.ActionSearchIndexProbe,
		Subject: subject.SearchMessages(account, r.cfg.SiteID),
		Account: account, RoomID: roomID,
		Body: model.SearchMessagesRequest{
			Query:   term,
			RoomIDs: []string{roomID},
			Size:    r.cfg.PageSize,
		},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *read.Sample) {
		sample.CountRows(len(response.Messages))
	})
	if err != nil {
		// A search-service or Elasticsearch outage proves nothing about the
		// message. Reporting missing here would turn every dependency outage
		// longer than the deadline into a data-loss claim.
		return IndexUnknown, true, err
	}
	for i := range response.Messages {
		if response.Messages[i].MessageID == messageID {
			return IndexFound, true, nil
		}
	}
	return IndexMissing, true, nil
}

func (r *Reader) pickQuery() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accounts[r.rng.Intn(len(r.accounts))], r.terms[r.rng.Intn(len(r.terms))]
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (r *Reader) call(
	ctx context.Context,
	request rpc.Request,
	response any,
	apply func(*read.Sample),
) error {
	if r.rpc == nil {
		return fmt.Errorf("soak search reader requires an RPC client")
	}
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	sample := read.Sample{
		Action: request.Action, Latency: r.now().Sub(startedAt),
		ReplyBytes: result.ReplyBytes, Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		sample.ErrorReason = result.ErrorReason
		r.record(&sample)
		return fmt.Errorf("search read lane: %w", err)
	}
	if apply != nil {
		apply(&sample)
	}
	r.record(&sample)
	return nil
}

func (r *Reader) record(sample *read.Sample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}

// SettleBoundary is the earliest time an index probe for a message published at
// publishedAt can produce a usable answer. The reconciler reschedules a
// too-early operation to exactly this point.
func (r *Reader) SettleBoundary(publishedAt time.Time) time.Time {
	return publishedAt.UTC().Add(r.cfg.Settle)
}
