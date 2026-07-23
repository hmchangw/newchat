package model

import "encoding/json"

// TeamsBatchRequest is one batch of Teams-history messages to migrate. Each entry
// is a source-shaped payload the injected MessageTransformer decodes — kept as raw
// JSON so the seam stays source-agnostic.
type TeamsBatchRequest struct {
	Messages []json.RawMessage `json:"messages"`
}

// Per-message migration outcomes reported back to the caller so it retries only failures.
const (
	TeamsBatchPersisted = "persisted" // canonical publish PubAck succeeded
	TeamsBatchSkipped   = "skipped"   // deliberately not migrated (e.g. missing id)
	TeamsBatchError     = "error"     // transform / resolve / publish failed
)

// TeamsBatchResult is the per-message outcome, logged as the batch is consumed.
// TeamsMsgID echoes the source id; Error is set only when Status == error.
type TeamsBatchResult struct {
	TeamsMsgID string `json:"teamsMsgId"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}
