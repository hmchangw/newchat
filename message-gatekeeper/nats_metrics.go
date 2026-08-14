package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
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
	reasonUnknown          gatekeeperReasonCode = "unknown"
)

var allGatekeeperReasons = []gatekeeperReasonCode{
	reasonNone, reasonInvalidSubject, reasonInvalidPayload, reasonNotSubscribed,
	reasonRoomRestricted, reasonCanonicalPublish, reasonDependency, reasonUnknown,
}

type gatekeeperMetrics struct {
	messages metric.Int64Counter
	opts     map[gatekeeperKey]metric.MeasurementOption
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
	m := &gatekeeperMetrics{
		messages: counter,
		opts:     make(map[gatekeeperKey]metric.MeasurementOption, len(allGatekeeperResults)*len(allGatekeeperReasons)),
	}
	for _, result := range allGatekeeperResults {
		for _, reason := range allGatekeeperReasons {
			m.opts[gatekeeperKey{result, reason}] = metric.WithAttributes(
				attribute.String("result", string(result)),
				attribute.String("reason", string(reason)),
			)
		}
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
