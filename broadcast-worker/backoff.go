package main

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/retrylane"
)

// settleBackoff picks the retry schedule a failed message calls for.
//
// broadcast-worker normally settles on LowLatencyBackoff, whose 200ms first
// rung is the point: a sub-second blip must not be visible to a client waiting
// on fan-out. That is the wrong curve for one case — a downstream that is
// *shedding*.
//
// The thread-parent path issues a synchronous request to history-service, which
// replies Unavailable ("service busy") once its admission cap is saturated.
// Retrying that in 200ms aims more load at the service that is already failing,
// so offered load rises as capacity falls. BackpressureBackoff exists for
// exactly this — its doc notes "a one-second retry only feeds the overload that
// caused the rejection" — and search-sync-worker already routes ES 429s to it.
//
// The delivery budget stays correct across both: MaxDeliver is derived once
// from LowLatencyBackoff (the faster schedule, so the larger count), and both
// schedules share a 10m repeating tail, so a message settling on the slower
// curve still has enough deliveries to cover the outage window. A future change
// that breaks that shared tail must re-derive the budget from the slower
// schedule — TestSettleBackoff_TailMatchesDeliveryBudget guards it.
func settleBackoff(err error) []time.Duration {
	if isDownstreamShedding(err) {
		return jsretry.BackpressureBackoff
	}
	return jsretry.LowLatencyBackoff
}

// isDownstreamShedding reports whether err is a dependency telling us it is over
// capacity, as opposed to failing. historyParentFetcher propagates the typed
// remote error precisely so it can be classified here, and natsutil.RequestFailure
// maps a request timeout or a missing responder onto Unavailable too.
func isDownstreamShedding(err error) bool {
	var ee *errcode.Error
	if !errors.As(err, &ee) {
		return false
	}
	return ee.Code == errcode.CodeUnavailable || ee.Code == errcode.CodeTooManyRequests
}

// slowBackoffFor picks the retry lane's tail from the reason the hot lane
// escalated with, so escalation preserves settleBackoff's choice instead of
// discarding it.
//
// Without this, a message shed by a downstream rides BackpressureBackoff for its
// fast rungs and then drains on LowLatencyBackoff's tail — a 30s first retry
// where the curve called for 5m. Turning the retry lane ON would therefore
// retry a shedding dependency HARDER than leaving it off, inverting the point of
// both features at exactly the moment the downstream is asking for relief.
//
// The delivery budget is unaffected: ConsumerConfig derives the retry consumer's
// MaxDeliver from the faster schedule (the larger count) and both tails end in
// the same repeating 10m rung, so a message on the slower curve still covers the
// outage window — the coupling TestSettleBackoff_TailMatchesDeliveryBudget pins.
func slowBackoffFor(h nats.Header, normal, backpressure []time.Duration) []time.Duration {
	if isSheddingReason(h.Get(retrylane.HeaderReason)) {
		return backpressure
	}
	return normal
}

// isSheddingReason is isDownstreamShedding over the wire: the hot lane stamps
// X-Retry-Reason via retrylane.ReasonFor, which is the errcode code string, and
// the typed error itself does not survive escalation. Anything unrecognized —
// including a missing header — takes the normal curve, so an unknown reason
// cannot accidentally slow delivery down.
func isSheddingReason(reason string) bool {
	return reason == string(errcode.CodeUnavailable) || reason == string(errcode.CodeTooManyRequests)
}
