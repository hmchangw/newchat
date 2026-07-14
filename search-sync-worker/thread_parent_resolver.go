package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// parentResolveTimeout bounds a single thread-parent createdAt lookup so a slow
// search backend can't stall the indexing path.
const parentResolveTimeout = 2 * time.Second

// esReadFn reads a message's (server-stamped, authoritative) createdAt from the
// search index. ok=false means not found — the caller leaves the field unset.
type esReadFn func(ctx context.Context, messageID string) (time.Time, bool)

// esParentResolver resolves a thread parent's createdAt via an ES self-lookup. It
// never errors: a miss or backend failure degrades the field rather than stalling.
type esParentResolver struct {
	esRead  esReadFn
	timeout time.Duration
}

// ResolveParentCreatedAt returns the parent's createdAt and ok=true when resolved.
// ok=false means the parent isn't indexed yet — the caller leaves the field unset.
func (r *esParentResolver) ResolveParentCreatedAt(ctx context.Context, messageID string) (time.Time, bool) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.esRead(ctx, messageID)
}

// esSearcher is the narrow search surface the ES self-lookup needs.
// searchengine.SearchEngine satisfies it.
type esSearcher interface {
	Search(ctx context.Context, indices []string, body json.RawMessage) (json.RawMessage, error)
}

// newESRead returns an esReadFn that looks the parent up by ID and returns its
// createdAt. The lookup spans every monthly index for indexPrefix's version.
func newESRead(searcher esSearcher, indexPrefix string) esReadFn {
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
		raw, err := searcher.Search(ctx, []string{pattern}, body)
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

// newESParentResolver builds the ES-self-lookup resolver. searcher must be non-nil.
func newESParentResolver(searcher esSearcher, indexPrefix string) *esParentResolver {
	return &esParentResolver{
		esRead:  newESRead(searcher, indexPrefix),
		timeout: parentResolveTimeout,
	}
}
