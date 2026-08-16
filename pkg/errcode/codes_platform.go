package errcode

// Reasons owned by cross-cutting platform code (pkg/natsutil, pkg/natsrouter)
// rather than by a single domain service.
//
// How each one reaches the wire differs, and the difference matters when
// reasoning about coverage. RequestIDRequired is emitted by middleware, so
// every request on a dedup-critical path is checked whether the handler knows
// it or not. NatsNoResponders and NatsRequestTimeout are not: they come from
// natsutil.RequestFailure, which each request/reply call site opts into
// explicitly. A call site that does not call it still collapses these failures
// to internal, and nothing enforces adoption.
const (
	// RequestIDRequired marks a rejected request that arrived without a valid
	// X-Request-ID on a dedup-critical path (see natsutil.RequireRequestID).
	// Clients should special-case it by retrying with a freshly minted
	// hyphenated UUID rather than surfacing a generic "bad request".
	RequestIDRequired Reason = "request_id_required"

	// NatsNoResponders marks a request/reply whose subject had no subscriber —
	// the upstream service is down, not yet started, or not routed to this
	// site. Retryable once the upstream returns.
	NatsNoResponders Reason = "no_responders"

	// NatsRequestTimeout marks a request that was delivered but not answered
	// within the caller's timeout. Retryable.
	NatsRequestTimeout Reason = "upstream_timeout"
)
