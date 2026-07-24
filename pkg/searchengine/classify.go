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
