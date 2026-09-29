package main

import (
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
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
