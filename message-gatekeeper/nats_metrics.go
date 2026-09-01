package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/hmchangw/chat/pkg/broadcastpath"
)

// gatekeeperResult and gatekeeperReasonCode are closed enums so a typo is a
// compile error rather than a silent `unknown` series at campaign time.
type gatekeeperResult string

const (
	resultAccepted gatekeeperResult = "accepted"
	resultRejected gatekeeperResult = "rejected"
	resultRetry    gatekeeperResult = "retry"
	resultFailed   gatekeeperResult = "failed"
)

var allGatekeeperResults = []gatekeeperResult{resultAccepted, resultRejected, resultRetry, resultFailed}

type gatekeeperReasonCode string

const (
	reasonNone             gatekeeperReasonCode = "none"
	reasonInvalidSubject   gatekeeperReasonCode = "invalid_subject"
	reasonInvalidPayload   gatekeeperReasonCode = "invalid_payload"
	reasonNotSubscribed    gatekeeperReasonCode = "not_subscribed"
	reasonRoomRestricted   gatekeeperReasonCode = "room_restricted"
	reasonCanonicalPublish gatekeeperReasonCode = "canonical_publish"
	reasonDependency       gatekeeperReasonCode = "dependency"
	// reasonInternal is a permanent server-side fault (a value that cannot be
	// marshaled): undeliverable work, never a client rejection.
	reasonInternal gatekeeperReasonCode = "internal"
	reasonUnknown  gatekeeperReasonCode = "unknown"
)

var allGatekeeperReasons = []gatekeeperReasonCode{
	reasonNone, reasonInvalidSubject, reasonInvalidPayload, reasonNotSubscribed,
	reasonRoomRestricted, reasonCanonicalPublish, reasonDependency, reasonInternal, reasonUnknown,
}

type gatekeeperMetrics struct {
	messages metric.Int64Counter
	opts     map[gatekeeperKey]metric.MeasurementOption

	// canonicalPublished is SLO-1a's and SLO-1b's denominator. It sits beside
	// messages rather than as a label on it: messages is a handler-outcome
	// counter recorded after the reply, so widening it with broadcast_path would
	// multiply its label space across all four results for the benefit of one
	// and change the meaning of a series other dashboards already read.
	canonicalPublished metric.Int64Counter
	// canonicalDuplicate makes the publish counter's one approximation visible
	// rather than inferred. See RecordCanonicalPublished.
	canonicalDuplicate metric.Int64Counter
	pathOpts           map[broadcastpath.Path]metric.MeasurementOption
}

type gatekeeperKey struct {
	result gatekeeperResult
	reason gatekeeperReasonCode
}

func newGatekeeperMetrics(meter metric.Meter) *gatekeeperMetrics {
	noopMeter := noop.NewMeterProvider().Meter("message-gatekeeper")
	counter, err := meter.Int64Counter("message_gatekeeper_messages_total",
		metric.WithDescription("Gatekeeper business outcomes by bounded result and reason."))
	if err != nil {
		// Telemetry must never block startup: fall back to a no-op instrument so
		// the service runs blind on this metric rather than not at all.
		counter, _ = noopMeter.Int64Counter("message_gatekeeper_messages_total")
	}
	published, err := meter.Int64Counter("messages_canonical_published_total",
		metric.WithDescription("Canonical messages published to MESSAGES-CANONICAL, by the fan-out route they will take."))
	if err != nil {
		published, _ = noopMeter.Int64Counter("messages_canonical_published_total")
	}
	duplicate, err := meter.Int64Counter("messages_canonical_publish_duplicate_total",
		metric.WithDescription("Canonical publishes the stream deduplicated, excluded from messages_canonical_published_total."))
	if err != nil {
		duplicate, _ = noopMeter.Int64Counter("messages_canonical_publish_duplicate_total")
	}
	m := &gatekeeperMetrics{
		messages:           counter,
		opts:               make(map[gatekeeperKey]metric.MeasurementOption, len(allGatekeeperResults)*len(allGatekeeperReasons)),
		canonicalPublished: published,
		canonicalDuplicate: duplicate,
		pathOpts:           make(map[broadcastpath.Path]metric.MeasurementOption, len(broadcastpath.All)),
	}
	for _, result := range allGatekeeperResults {
		for _, reason := range allGatekeeperReasons {
			m.opts[gatekeeperKey{result, reason}] = metric.WithAttributes(
				attribute.String("result", string(result)),
				attribute.String("reason", string(reason)),
			)
		}
	}
	for _, path := range broadcastpath.All {
		m.pathOpts[path] = metric.WithAttributes(attribute.String("broadcast_path", string(path)))
	}
	return m
}

func (m *gatekeeperMetrics) Record(ctx context.Context, result gatekeeperResult, reason gatekeeperReasonCode) {
	if m == nil || m.messages == nil {
		return
	}
	opt, ok := m.opts[gatekeeperKey{result, reason}]
	if !ok {
		opt = m.opts[gatekeeperKey{resultFailed, reasonUnknown}]
	}
	m.messages.Add(ctx, 1, opt)
}

// RecordCanonicalPublished counts one canonical message on the fan-out route it
// will take. It is the denominator of SLO-1a and SLO-1b, and it is emitted here
// — upstream of both workers — so that a stalled worker drops the ratio instead
// of removing the message from both halves of it.
//
// It is an approximate PubAck-based publish count, and the approximation runs in
// one direction each way. A JetStream redelivery of an already-published message
// gets a duplicate-flagged ack, which is excluded here and counted by
// RecordCanonicalPublishDuplicate instead; but a first publish whose ack is lost
// is retried, flagged duplicate, and so counted by neither. No application
// counter can be an exactly-once logical-publish count — that needs a
// server-side stream delta or a persisted ledger.
func (m *gatekeeperMetrics) RecordCanonicalPublished(ctx context.Context, path broadcastpath.Path) {
	if m == nil || m.canonicalPublished == nil {
		return
	}
	opt, ok := m.pathOpts[path]
	if !ok {
		// A value outside the enum can only arrive via a conversion; collapse it
		// rather than mint a series nothing closed.
		opt = m.pathOpts[broadcastpath.Unknown]
	}
	m.canonicalPublished.Add(ctx, 1, opt)
}

// RecordCanonicalPublishDuplicate counts a publish the stream deduplicated. It
// carries no labels: it exists to make the size of the exclusion above visible,
// and the message it describes has already been classified once.
func (m *gatekeeperMetrics) RecordCanonicalPublishDuplicate(ctx context.Context) {
	if m == nil || m.canonicalDuplicate == nil {
		return
	}
	m.canonicalDuplicate.Add(ctx, 1)
}
