package searchengine

// ErrDocumentMissing is the ES error type returned when an ActionUpdate
// targets a document that does not exist yet (the scripted remove-path
// update against a not-yet-created user-room doc, or any other update
// racing a not-yet-indexed target).
const ErrDocumentMissing = "document_missing_exception"

// IsBulkItemSuccess classifies one _bulk response item's outcome for the
// given action type. Both search-sync-worker (the live path) and
// data-migration/es-index-migrator (the one-time/rebuild path) share this
// classifier so a benign, idempotent-redelivery 409/404 can never be
// misclassified as a hard failure by one caller but not the other.
//
//   - 2xx is always success.
//   - 409 is success for ActionIndex/ActionDelete (external-versioning
//     rejected a stale write — the desired state is already there) but a
//     real failure for ActionUpdate (the LWW guard runs inside the script,
//     not via ES versioning, so a 409 on update means the script itself
//     never ran).
//   - 404 is success for ActionDelete with no error block (delete of an
//     already-missing doc) and for ActionUpdate with ErrDocumentMissing
//     (scripted remove against a doc that was never created); any other
//     404 (in particular index_not_found_exception, and any 404 on
//     ActionIndex) is a real failure.
func IsBulkItemSuccess(action ActionType, result BulkResult) bool {
	if result.Status >= 200 && result.Status < 300 {
		return true
	}
	if result.Status == 409 {
		switch action {
		case ActionIndex, ActionDelete:
			return true
		case ActionUpdate:
			return false
		}
	}
	if result.Status == 404 {
		switch action {
		case ActionDelete:
			return result.ErrorType == ""
		case ActionUpdate:
			return result.ErrorType == ErrDocumentMissing
		case ActionIndex:
			return false
		}
	}
	return false
}

// ErrRejectedExecution / ErrCircuitBreaking are the ES error types that mean
// "the cluster is saturated", not "this write is invalid". They arrive as
// per-item failures inside an otherwise-200 _bulk response.
const (
	ErrRejectedExecution = "es_rejected_execution_exception"
	ErrCircuitBreaking   = "circuit_breaking_exception"
)

// IsBulkItemBackpressure reports whether a failed bulk item was rejected
// because the search backend is overloaded rather than because the write
// itself was bad. Callers use it to retry on a longer delay: retrying
// promptly against a cluster that just shed load is what turns a transient
// overload into a sustained one.
//
// Matched on status (429) or on the error type, since a circuit breaker can
// surface under a 503 as well.
func IsBulkItemBackpressure(result BulkResult) bool {
	if result.Status == 429 {
		return true
	}
	switch result.ErrorType {
	case ErrRejectedExecution, ErrCircuitBreaking:
		return true
	}
	return false
}

// IsBulkItemPermanent reports whether a failed bulk item can never succeed on
// retry. A 400 means the backend rejected the document itself — a mapping
// conflict, an unparseable field, an illegal argument — so redelivering
// identical bytes can only fail identically. Callers drop it rather than
// burning the consumer's redelivery budget on a doomed retry.
//
// Deliberately narrow: 429/503 are backpressure (IsBulkItemBackpressure), a 409
// on an update is a live conflict a retry can win, and a 404
// index_not_found_exception clears once the index or template exists. None of
// those are permanent.
func IsBulkItemPermanent(result BulkResult) bool {
	return result.Status == 400
}
