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
