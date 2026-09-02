package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// permanentErrorTotal counts messages Ack-dropped as poison after a permanent processing error.
var permanentErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bot_msg_worker_permanent_error_total",
	Help: "Bot messages Ack-dropped as poison after a permanent processing error (schema violation, invariant break). One increment per poison-drop.",
})

// Failure outcomes for botFailureTotal, one per settle path that is not an Ack
// of successful work.
const (
	outcomeNak       = "nak"
	outcomePermanent = "permanent"
	outcomeMalformed = "malformed"
)

// unknownBot labels a failure that carries no sender identity in either the
// header or the payload. Counting it under a reserved label keeps the series
// complete: a spike with no attributable sender is itself a signal.
const unknownBot = "unknown"

// botFailureTotal counts failed settles per sending bot. Bots are a bounded,
// registered set, so a per-sender label is safe here in a way it never is for
// human accounts — it answers "which bot is filling the lane's ack-pending
// budget" without a log search. Successes are deliberately not counted: this
// series exists to name a culprit, not to measure throughput.
var botFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bot_msg_worker_failure_total",
	Help: "Failed bot-message settles by sending bot account and outcome (nak = transient retry, permanent = poison ack-drop, malformed = undecodable payload ack-drop).",
}, []string{"bot_account", "outcome"})
