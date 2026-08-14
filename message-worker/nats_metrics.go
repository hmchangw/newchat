package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// messageKindLabel and persistResult are closed enums so a typo is a compile
// error rather than a silent `unknown` series at campaign time.
type messageKindLabel string

const (
	kindUser           messageKindLabel = "user"
	kindSystem         messageKindLabel = "system"
	kindThreadReply    messageKindLabel = "thread_reply"
	kindTeamsMigration messageKindLabel = "teams_migration"
	kindUnknown        messageKindLabel = "unknown"
)

var allMessageKinds = []messageKindLabel{kindUser, kindSystem, kindThreadReply, kindTeamsMigration, kindUnknown}

type persistResult string

const (
	persistSuccess persistResult = "success"
	persistError   persistResult = "error"
)

var allPersistResults = []persistResult{persistSuccess, persistError}

type persistKey struct {
	kind   messageKindLabel
	result persistResult
}

type persistenceMetrics struct {
	outcomes metric.Int64Counter
	opts     map[persistKey]metric.MeasurementOption
}

func newPersistenceMetrics(meter metric.Meter) *persistenceMetrics {
	counter, _ := meter.Int64Counter("message_worker_persistence_total",
		metric.WithDescription("Cassandra message persistence attempts by bounded message kind and result."))
	m := &persistenceMetrics{
		outcomes: counter,
		opts:     make(map[persistKey]metric.MeasurementOption, len(allMessageKinds)*len(allPersistResults)),
	}
	for _, kind := range allMessageKinds {
		for _, result := range allPersistResults {
			m.opts[persistKey{kind, result}] = metric.WithAttributes(
				attribute.String("message_kind", string(kind)),
				attribute.String("result", string(result)),
			)
		}
	}
	return m
}

func (m *persistenceMetrics) Record(ctx context.Context, kind messageKindLabel, result persistResult) {
	if m == nil || m.outcomes == nil {
		return
	}
	opt, ok := m.opts[persistKey{kind, result}]
	if !ok {
		opt = m.opts[persistKey{kindUnknown, persistError}]
	}
	m.outcomes.Add(ctx, 1, opt)
}
