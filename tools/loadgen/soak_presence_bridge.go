package main

import (
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	soakpresence "github.com/hmchangw/chat/tools/loadgen/internal/soak/presence"
)

type soakPresencePublisher = soakpresence.Publisher
type soakPresenceConfig = soakpresence.Config
type soakPresenceLane = soakpresence.Lane

type soakPresenceMetricsAdapter struct {
	metrics *Metrics
}

func (a *soakPresenceMetricsAdapter) CountSignal(signal string) {
	a.metrics.SoakPresenceSignals.WithLabelValues(signal).Inc()
}

func (a *soakPresenceMetricsAdapter) CountCheck(result string, count int) {
	a.metrics.SoakPresenceChecks.WithLabelValues(result).Add(float64(count))
}

func (a *soakPresenceMetricsAdapter) SetConnections(
	status model.PresenceStatus,
	count int,
) {
	a.metrics.SoakPresenceConnections.WithLabelValues(string(status)).Set(float64(count))
}

func newSoakPresenceLane(
	cfg soakPresenceConfig,
	topology *soakTopology,
	pool soakPresencePublisher,
	rpc *soakRPCClient,
	metrics *Metrics,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakPresenceLane, error) {
	var observer soakpresence.Observer
	if metrics != nil {
		observer = &soakPresenceMetricsAdapter{metrics: metrics}
	}
	return soakpresence.New(
		cfg,
		topology,
		pool,
		rpc,
		observer,
		recorder,
		rng,
		now,
	)
}

type natsSoakPresencePublisher = soakpresence.NATSPublisher

func newNATSSoakPresencePublisher(conn *nats.Conn) *natsSoakPresencePublisher {
	return soakpresence.NewNATSPublisher(conn)
}
