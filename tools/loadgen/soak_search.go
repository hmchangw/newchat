package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// soakSearchIndexResult is what the index probe could establish about one
// message. It is deliberately three-valued: "not there yet" is not the same
// claim as "not there", and only the latter is evidence of loss.
type soakSearchIndexResult string

const (
	soakSearchIndexFound   soakSearchIndexResult = "found"
	soakSearchIndexMissing soakSearchIndexResult = "missing"
	soakSearchIndexUnknown soakSearchIndexResult = "unknown"
	// soakSearchIndexTooEarly is distinct from unknown on purpose. It says the
	// answer is not due yet, which lets the reconciler reschedule to the settle
	// boundary instead of re-asking every retry interval — at the default
	// rates, polling through a 30s settle window would spend several times the
	// entire reconciliation budget on queries that cannot succeed.
	soakSearchIndexTooEarly soakSearchIndexResult = "too_early"
)

type soakSearchConfig struct {
	SiteID         string
	PageSize       int
	RequestTimeout time.Duration
	// Settle is how long after publish the index probe waits before asking.
	// Elasticsearch is refreshed asynchronously, so a hit that is absent
	// inside this window is legal rather than lost.
	Settle time.Duration
}

// soakSearchReader drives search-service and answers whether a message reached
// the search index.
//
// The read lane it serves is ordinary read traffic: latency and outcome, no
// ledger. The index probe is the part that matters for evidence —
// search-sync-worker Acks and drops a message whose payload fails to decode or
// build an action, so that loss leaves its consumer at zero pending and is
// invisible from JetStream. Asking the query side is the only way to see it.
type soakSearchReader struct {
	cfg      soakSearchConfig
	rpc      *soakRPCClient
	recorder soakReadSampleRecorder
	now      func() time.Time

	mu       sync.Mutex
	rng      *rand.Rand
	accounts []string
	terms    []string
}

func newSoakSearchReader(
	cfg soakSearchConfig,
	topology *soakTopology,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakSearchReader, error) {
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
		cfg.RequestTimeout = soakRequestTimeout
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 30 * time.Second
	}

	reader := &soakSearchReader{
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

func (r *soakSearchReader) ReadMixed(ctx context.Context) error {
	r.mu.Lock()
	messages := r.rng.Float64() < 0.7
	r.mu.Unlock()
	if messages {
		return r.SearchMessages(ctx)
	}
	return r.SearchRooms(ctx)
}

func (r *soakSearchReader) SearchMessages(ctx context.Context) error {
	account, term := r.pickQuery()
	var response model.SearchMessagesResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCSearchMessages,
		Subject: subject.SearchMessages(account, r.cfg.SiteID),
		Body:    model.SearchMessagesRequest{Query: term, Size: r.cfg.PageSize},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Messages)
	})
}

func (r *soakSearchReader) SearchRooms(ctx context.Context) error {
	account, term := r.pickQuery()
	var response model.SearchRoomsResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCSearchRooms,
		Subject: subject.SearchRooms(account, r.cfg.SiteID),
		Body:    model.SearchRoomsRequest{Query: term, Size: r.cfg.PageSize},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Rooms)
	})
}

// IndexedAt reports whether one message reached the search index, scoping the
// query to the message's own room so the answer does not depend on relevance
// ranking across the whole corpus.
//
// publishedAt gates the settle window. Before it elapses the probe returns
// unknown without issuing a request, because an absent hit there is legal and
// the query would only spend a read slot to learn nothing.
func (r *soakSearchReader) IndexedAt(
	ctx context.Context,
	account, roomID, messageID, content string,
	publishedAt time.Time,
) (soakSearchIndexResult, error) {
	if account == "" || roomID == "" || messageID == "" {
		return soakSearchIndexUnknown,
			fmt.Errorf("search index probe requires an account, room and message")
	}
	if r.now().UTC().Sub(publishedAt.UTC()) < r.cfg.Settle {
		return soakSearchIndexTooEarly, nil
	}

	var response model.SearchMessagesResponse
	err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCSearchIndexProbe,
		Subject: subject.SearchMessages(account, r.cfg.SiteID),
		Body: model.SearchMessagesRequest{
			Query:   searchProbeTerm(content),
			RoomIDs: []string{roomID},
			Size:    r.cfg.PageSize,
		},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Messages)
	})
	if err != nil {
		// A search-service or Elasticsearch outage proves nothing about the
		// message. Reporting missing here would turn every dependency outage
		// longer than the deadline into a data-loss claim.
		return soakSearchIndexUnknown, err
	}
	for i := range response.Messages {
		if response.Messages[i].MessageID == messageID {
			return soakSearchIndexFound, nil
		}
	}
	return soakSearchIndexMissing, nil
}

// searchProbeTerm reduces a payload to a term the analyzer will match. The
// bodies are generated filler, so the first whitespace-delimited token is both
// stable and present in the indexed document.
func searchProbeTerm(content string) string {
	for field := range strings.FieldsSeq(content) {
		if len(field) >= 3 {
			return field
		}
	}
	return "soak"
}

func (r *soakSearchReader) pickQuery() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accounts[r.rng.Intn(len(r.accounts))], r.terms[r.rng.Intn(len(r.terms))]
}

func (r *soakSearchReader) call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
	apply func(*soakReadSample),
) error {
	if r.rpc == nil {
		return fmt.Errorf("soak search reader requires an RPC client")
	}
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	sample := soakReadSample{
		Action: request.Action, Latency: r.now().Sub(startedAt), Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		sample.ErrorReason = result.ErrorReason
		r.record(&sample)
		return fmt.Errorf("issue %s request: %w", request.Action, err)
	}
	if apply != nil {
		apply(&sample)
	}
	r.record(&sample)
	return nil
}

func (r *soakSearchReader) record(sample *soakReadSample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}

// soakSearchIndexProbe answers the ledger's index question for one operation.
//
// The query term comes from the in-process catalog rather than the WAL: the
// ledger deliberately never stores a message body, and search-service offers no
// lookup by message ID. After a pod replacement the catalog is empty, so a
// recovered operation reports unknown and resolves `unverified` — the same
// stance recovered recipient operations already take, and the honest one, since
// the pre-restart evidence cannot be reconstructed.
type soakSearchIndexProbe struct {
	reader  *soakSearchReader
	catalog *soakCatalog
}

func newSoakSearchIndexProbe(
	reader *soakSearchReader,
	catalog *soakCatalog,
) *soakSearchIndexProbe {
	return &soakSearchIndexProbe{reader: reader, catalog: catalog}
}

func (p *soakSearchIndexProbe) Indexed(
	ctx context.Context,
	operation *failureOperation,
) (soakSearchIndexResult, error) {
	if p == nil || p.reader == nil || p.catalog == nil || operation == nil {
		return soakSearchIndexUnknown, nil
	}
	roomID := operation.Targets["roomId"]
	messageID := operation.Targets["messageId"]
	account := operation.Attributes[soakFailureAttributeAccount]
	if roomID == "" || messageID == "" || account == "" {
		return soakSearchIndexUnknown, nil
	}
	message, known := p.catalog.Get(roomID, messageID)
	if !known || message.Content == "" {
		return soakSearchIndexUnknown, nil
	}
	return p.reader.IndexedAt(
		ctx, account, roomID, messageID, message.Content, operation.StartedAt,
	)
}

// SettleBoundary is the earliest time an index probe for a message published at
// publishedAt can produce a usable answer. The reconciler reschedules a
// too-early operation to exactly this point.
func (r *soakSearchReader) SettleBoundary(publishedAt time.Time) time.Time {
	return publishedAt.UTC().Add(r.cfg.Settle)
}

func (p *soakSearchIndexProbe) SettleBoundary(publishedAt time.Time) time.Time {
	if p == nil || p.reader == nil {
		return publishedAt
	}
	return p.reader.SettleBoundary(publishedAt)
}
