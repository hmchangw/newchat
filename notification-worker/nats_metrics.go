package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// notifyKind and notifyResult are closed enums so a typo is a compile error
// rather than a silent `unknown` series at campaign time.
type notifyKind string

const (
	notifyKindPush    notifyKind = "push"
	notifyKindUnknown notifyKind = "unknown"
)

var allNotifyKinds = []notifyKind{notifyKindPush, notifyKindUnknown}

type notifyResult string

const (
	notifySent          notifyResult = "sent"
	notifySuppressed    notifyResult = "suppressed"
	notifyPublishFailed notifyResult = "publish_failed"
	notifyFailed        notifyResult = "failed"
)

var allNotifyResults = []notifyResult{notifySent, notifySuppressed, notifyPublishFailed, notifyFailed}

type notifyKey struct {
	kind   notifyKind
	result notifyResult
}

type notificationMetrics struct {
	outcomes        metric.Int64Counter
	opts            map[notifyKey]metric.MeasurementOption
	mentionFailures metric.Int64Counter
}

func newNotificationMetrics(meter metric.Meter) *notificationMetrics {
	noopMeter := noop.NewMeterProvider().Meter("notification-worker")
	counter, err := meter.Int64Counter("notification_worker_outcomes_total",
		metric.WithDescription("Notification processing outcomes by bounded kind and result."))
	if err != nil {
		// Telemetry must never block startup: fall back to a no-op instrument so
		// the service runs blind on this metric rather than not at all.
		counter, _ = noopMeter.Int64Counter("notification_worker_outcomes_total")
	}
	// Failures only, and attribute-free: a mention lookup that errors or times out
	// ships raw @tokens in every push body, which nothing else in this service
	// reports. Per-token resolved/unresolved accounting was deliberately dropped —
	// it costs a counter add per message on the hot path to report something an
	// operator cannot act on.
	mentionFailures, err := meter.Int64Counter("notification_worker_mention_lookup_failures_total",
		metric.WithDescription("Push-body @mention lookups that errored or timed out."))
	if err != nil {
		mentionFailures, _ = noopMeter.Int64Counter("notification_worker_mention_lookup_failures_total")
	}
	m := &notificationMetrics{
		outcomes:        counter,
		opts:            make(map[notifyKey]metric.MeasurementOption, len(allNotifyKinds)*len(allNotifyResults)),
		mentionFailures: mentionFailures,
	}
	for _, kind := range allNotifyKinds {
		for _, result := range allNotifyResults {
			m.opts[notifyKey{kind, result}] = metric.WithAttributes(
				attribute.String("kind", string(kind)),
				attribute.String("result", string(result)),
			)
		}
	}
	return m
}

// RecordMentionFailure counts one failed mention lookup — the push still goes
// out, with raw @tokens in the body.
func (m *notificationMetrics) RecordMentionFailure(ctx context.Context) {
	if m == nil || m.mentionFailures == nil {
		return
	}
	m.mentionFailures.Add(ctx, 1)
}

func (m *notificationMetrics) Record(ctx context.Context, kind notifyKind, result notifyResult) {
	if m == nil || m.outcomes == nil {
		return
	}
	opt, ok := m.opts[notifyKey{kind, result}]
	if !ok {
		opt = m.opts[notifyKey{notifyKindUnknown, notifyFailed}]
	}
	m.outcomes.Add(ctx, 1, opt)
}
