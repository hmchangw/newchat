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

// mentionResult is the per-token outcome of push-body mention resolution. The
// three values partition every looked-up token, so resolved+unresolved+failed
// equals the tokens requested: a failed lookup's tokens count as failed only,
// never also as unresolved. Without this the fail-open path is invisible — a
// users-collection outage would leave notification_worker_outcomes_total fully
// green while every body shipped raw @tokens.
type mentionResult string

const (
	mentionResolved   mentionResult = "resolved"
	mentionUnresolved mentionResult = "unresolved"
	mentionFailed     mentionResult = "failed"
)

var allMentionResults = []mentionResult{mentionResolved, mentionUnresolved, mentionFailed}

type notifyKey struct {
	kind   notifyKind
	result notifyResult
}

type notificationMetrics struct {
	outcomes    metric.Int64Counter
	opts        map[notifyKey]metric.MeasurementOption
	mentions    metric.Int64Counter
	mentionOpts map[mentionResult]metric.MeasurementOption
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
	mentions, err := meter.Int64Counter("notification_worker_mention_resolution_total",
		metric.WithDescription("Push-body @mention tokens by resolution result."))
	if err != nil {
		mentions, _ = noopMeter.Int64Counter("notification_worker_mention_resolution_total")
	}
	m := &notificationMetrics{
		outcomes:    counter,
		opts:        make(map[notifyKey]metric.MeasurementOption, len(allNotifyKinds)*len(allNotifyResults)),
		mentions:    mentions,
		mentionOpts: make(map[mentionResult]metric.MeasurementOption, len(allMentionResults)),
	}
	for _, kind := range allNotifyKinds {
		for _, result := range allNotifyResults {
			m.opts[notifyKey{kind, result}] = metric.WithAttributes(
				attribute.String("kind", string(kind)),
				attribute.String("result", string(result)),
			)
		}
	}
	for _, result := range allMentionResults {
		m.mentionOpts[result] = metric.WithAttributes(attribute.String("result", string(result)))
	}
	return m
}

// RecordMentions adds n tokens under result. A non-positive n is a no-op so
// call sites don't have to guard the common zero case.
func (m *notificationMetrics) RecordMentions(ctx context.Context, result mentionResult, n int) {
	if m == nil || m.mentions == nil || n <= 0 {
		return
	}
	opt, ok := m.mentionOpts[result]
	if !ok {
		return
	}
	m.mentions.Add(ctx, int64(n), opt)
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
