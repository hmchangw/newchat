package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/searchindex"
)

// parentResolveTimeout bounds one thread-parent createdAt lookup — including any
// fallback search — so a slow search backend can't stall the indexing path.
const parentResolveTimeout = 2 * time.Second

// esGetFn reads a message's createdAt from one concrete index by document id.
// ok=false means the index or the document is absent.
type esGetFn func(ctx context.Context, index, messageID string) (time.Time, bool)

// esSearchFn reads a message's createdAt by searching every monthly index.
// ok=false means not found — the caller leaves the field unset.
type esSearchFn func(ctx context.Context, messageID string) (time.Time, bool)

// esParentResolver resolves a thread parent's createdAt from the search index. It
// never errors: a miss or backend failure degrades the field rather than stalling.
type esParentResolver struct {
	esGet       esGetFn
	esSearch    esSearchFn
	indexPrefix string
	timeout     time.Duration
	metrics     *collectionMetrics // optional, set by main; nil-safe recording
}

// prevMonth returns any instant inside the calendar month before at.
//
// Not at.AddDate(0, -1, 0): that normalises overflow, so 31 March becomes
// "31 February" and lands back on 3 March — repeating the month the caller just
// probed. Going via time.Date on the month number normalises correctly, including
// January rolling back into the previous December.
func prevMonth(at time.Time) time.Time {
	at = at.UTC()
	return time.Date(at.Year(), at.Month()-1, 1, 0, 0, 0, 0, time.UTC)
}

// ResolveParentCreatedAt returns the parent's createdAt and ok=true when resolved.
// ok=false means the parent isn't indexed yet — the caller leaves the field unset.
//
// A parent always predates its reply and threads are overwhelmingly same-month, so
// this point-reads the reply's month and then the one before it before falling back
// to the cross-month search. Two reasons that ordering matters:
//
//   - A get is realtime — served from the translog, not bound by the index's
//     refresh_interval — so it sees a parent indexed seconds ago. The search cannot,
//     which made it miss exactly the recent-parent case that dominates live chat.
//   - A get routes to a single shard by id; the search fans out across every monthly
//     index times its shard count.
//
// The search stays as the fallback for parents older than one month, where the month
// probes can't guess the index.
func (r *esParentResolver) ResolveParentCreatedAt(ctx context.Context, messageID string, replyCreatedAt time.Time) (time.Time, bool) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Times the whole resolution — both month probes and any fallback search — because
	// that is the latency the indexing path actually pays for one parent.
	start := time.Now()
	createdAt, ok := r.resolve(ctx, messageID, replyCreatedAt)
	r.metrics.recordParentResolve(ctx, time.Since(start).Seconds(), ok)
	return createdAt, ok
}

// resolve point-reads the reply's month and then the one before it, falling back to the
// cross-month search only for a parent older than either.
func (r *esParentResolver) resolve(ctx context.Context, messageID string, replyCreatedAt time.Time) (time.Time, bool) {
	for _, at := range [...]time.Time{replyCreatedAt, prevMonth(replyCreatedAt)} {
		if createdAt, ok := r.esGet(ctx, searchindex.MessageIndexName(r.indexPrefix, at), messageID); ok {
			return createdAt, true
		}
	}
	return r.esSearch(ctx, messageID)
}

// esReader is the narrow surface the parent lookup needs.
// searchengine.SearchEngine satisfies it.
type esReader interface {
	GetDoc(ctx context.Context, index, docID string) (json.RawMessage, bool, error)
	Search(ctx context.Context, indices []string, body json.RawMessage) (json.RawMessage, error)
}

// newESGet returns an esGetFn that point-reads one index. A missing index is a
// miss, not an error: GetDoc reports both as 404, which is what lets the caller
// probe a month that may never have been created.
func newESGet(reader esReader) esGetFn {
	return func(ctx context.Context, index, messageID string) (time.Time, bool) {
		raw, found, err := reader.GetDoc(ctx, index, messageID)
		if err != nil {
			slog.WarnContext(ctx, "ES parent createdAt get failed", "error", err, "index", index, "messageId", messageID)
			return time.Time{}, false
		}
		if !found {
			return time.Time{}, false
		}
		var resp struct {
			Source struct {
				CreatedAt time.Time `json:"createdAt"`
			} `json:"_source"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			slog.WarnContext(ctx, "decode ES parent get response failed", "error", err, "index", index, "messageId", messageID)
			return time.Time{}, false
		}
		if resp.Source.CreatedAt.IsZero() {
			return time.Time{}, false
		}
		return resp.Source.CreatedAt, true
	}
}

// newESSearch returns an esSearchFn spanning every monthly index for indexPrefix's
// version. Only reached for parents older than the two months probed by id.
func newESSearch(reader esReader, indexPrefix string) esSearchFn {
	pattern := indexPrefix + "-*"
	return func(ctx context.Context, messageID string) (time.Time, bool) {
		body, err := json.Marshal(map[string]any{
			"size":    1,
			"_source": []string{"createdAt"},
			"query":   map[string]any{"term": map[string]any{"messageId": messageID}},
		})
		if err != nil {
			slog.WarnContext(ctx, "build ES parent lookup query failed", "error", err, "messageId", messageID)
			return time.Time{}, false
		}
		raw, err := reader.Search(ctx, []string{pattern}, body)
		if err != nil {
			slog.WarnContext(ctx, "ES parent createdAt lookup failed", "error", err, "messageId", messageID)
			return time.Time{}, false
		}
		var resp struct {
			Hits struct {
				Hits []struct {
					Source struct {
						CreatedAt time.Time `json:"createdAt"`
					} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			slog.WarnContext(ctx, "decode ES parent lookup response failed", "error", err, "messageId", messageID)
			return time.Time{}, false
		}
		if len(resp.Hits.Hits) == 0 || resp.Hits.Hits[0].Source.CreatedAt.IsZero() {
			return time.Time{}, false
		}
		return resp.Hits.Hits[0].Source.CreatedAt, true
	}
}

// newESParentResolver builds the ES-self-lookup resolver. reader must be non-nil.
func newESParentResolver(reader esReader, indexPrefix string) *esParentResolver {
	return &esParentResolver{
		esGet:       newESGet(reader),
		esSearch:    newESSearch(reader, indexPrefix),
		indexPrefix: indexPrefix,
		timeout:     parentResolveTimeout,
	}
}
