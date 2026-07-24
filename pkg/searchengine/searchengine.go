package searchengine

import (
	"context"
	"encoding/json"
	"net/http"
)

// Transporter performs raw HTTP requests. Both elastic/go-elasticsearch and
// opensearch-go clients implement this interface natively.
type Transporter interface {
	Perform(req *http.Request) (*http.Response, error)
}

// ActionType represents the type of bulk action.
type ActionType string

const (
	ActionIndex  ActionType = "index"
	ActionDelete ActionType = "delete"
	ActionUpdate ActionType = "update"
)

// BulkAction represents a single action in a bulk request.
//
// For ActionUpdate, Doc contains the full ES update body (doc / script /
// upsert) and Version is ignored. The _update operation is read-modify-write
// on the ES side and does not accept `version`/`version_type=external`; that
// parameter pair is only valid for `index` (full-document replacement).
type BulkAction struct {
	Action  ActionType
	Index   string
	DocID   string
	Version int64           // used as ES external version (ignored for ActionUpdate)
	Doc     json.RawMessage // index: full doc; update: update body; delete: nil
}

// BulkResult represents the result of a single bulk action item.
//
// ErrorType is the ES error type string (e.g., `document_missing_exception`,
// `index_not_found_exception`, `version_conflict_engine_exception`) when the
// item failed with an error block. Empty on 2xx success and on delete-404
// responses (delete of a missing doc sets `result:"not_found"` without an
// error block).
//
// Callers that need to classify 4xx outcomes (e.g., deciding whether a 404
// is a benign "doc already absent" or a fatal "index missing") should match
// on ErrorType rather than parsing the human-readable Error string.
type BulkResult struct {
	Status    int
	ErrorType string
	Error     string
}

// SearchEngine defines domain operations for search indexing.
type SearchEngine interface {
	Ping(ctx context.Context) error
	Bulk(ctx context.Context, actions []BulkAction) ([]BulkResult, error)
	UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error

	// PutScript registers (creates or updates) an Elasticsearch stored
	// script under the given id via `PUT /_scripts/{id}`. Stored scripts let
	// callers reference a script by id instead of inlining its full source in
	// every request — critical for fan-out bulk updates that would otherwise
	// repeat the same script body once per action.
	PutScript(ctx context.Context, id string, body json.RawMessage) error

	// UpdateMapping PUTs an additive `{"properties":...}` onto every index
	// matching pattern; templates apply only at creation, existing indices need this.
	UpdateMapping(ctx context.Context, indexPattern string, body json.RawMessage) error

	GetIndexMapping(ctx context.Context, index string) (json.RawMessage, error)

	// Search executes a `_search` against the comma-joined list of indices
	// and returns the raw response body. `ignore_unavailable=true` and
	// `allow_no_indices=true` are applied automatically so unknown remote
	// clusters and missing local indices degrade to an empty result set
	// rather than an error. Callers that need CCS pass patterns like
	// []string{"messages-*", "*:messages-*"}.
	Search(ctx context.Context, indices []string, body json.RawMessage) (json.RawMessage, error)

	// GetDoc fetches a single document by index and id. Returns found=false
	// and a nil body on 404 (doc or index missing); the two cases are
	// indistinguishable to the caller and are treated uniformly as
	// "no document" — same convention as GetIndexMapping.
	GetDoc(ctx context.Context, index, docID string) (json.RawMessage, bool, error)
}
