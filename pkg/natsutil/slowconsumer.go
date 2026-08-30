package natsutil

import (
	"context"
	"log/slog"
	"strings"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// slowConsumerEvents counts slow-consumer episodes, tagged by subject and queue
// group.
//
// One increment per episode — deliberately NOT the subscription's drop count.
// Subscription.Dropped() is a cumulative total that is never reset except on
// unsubscribe (nats.go:3770, 5574-5584), and the async error callback fires only
// on the Active->SlowConsumer transition (nats.go:3771-3787), with sub.sc cleared
// again on the next successful delivery (nats.go:3742). Adding Dropped() would
// therefore re-add every earlier episode's drops on each new episode: 3 dropped
// then 5 dropped reports 3 + 8 = 11. The callback is also dispatched through an
// async queue (nats.go:3785), so Dropped() read inside it is not even the count
// at episode time. Exact per-episode numbers live in the log fields.
var slowConsumerEvents metric.Int64Counter

func init() {
	var err error
	slowConsumerEvents, err = otel.Meter("nats").Int64Counter(
		"nats_slow_consumer_events_total",
		metric.WithDescription("Slow-consumer episodes on a NATS subscription, by subject and queue group"),
	)
	if err != nil {
		// Fall back to a no-op counter so recording stays safe even if the
		// global meter provider is not initialised at package init time.
		slowConsumerEvents, _ = noop.NewMeterProvider().Meter("nats").
			Int64Counter("nats_slow_consumer_events_total")
	}
}

// subDropped returns the subscription's cumulative drop count and whether it
// could be read at all. sub is nil for connection-level async errors, and
// Dropped() errors on an already-closed subscription, which happens routinely
// during shutdown.
func subDropped(sub *nats.Subscription) (int, bool) {
	if sub == nil {
		return 0, false
	}
	dropped, err := sub.Dropped()
	if err != nil {
		return 0, false
	}
	return dropped, true
}

// slowConsumerFields builds the slog fields describing a slow consumer.
//
// dropped/ok come from a single Dropped() read made by the caller rather than a
// second read here, so the level decision and the logged value cannot diverge:
// if the subscription closes between two reads, the level could say ERROR
// ("messages dropped") while the field set silently omits the dropped count.
//
// The remaining accessors stay best-effort — Pending/PendingLimits also error on
// a closed subscription, and a diagnostic path must never panic or mask the
// event it is reporting.
func slowConsumerFields(sub *nats.Subscription, dropped int, ok bool) []any {
	if sub == nil {
		return []any{"subject", "unknown"}
	}
	fields := []any{"subject", sub.Subject, "queue", sub.Queue}
	if ok {
		fields = append(fields, "dropped", dropped)
	}
	// nbytes, not bytes — a local named bytes shadows the stdlib package and
	// trips the linter's shadow check.
	if msgs, nbytes, err := sub.Pending(); err == nil {
		fields = append(fields, "pending_msgs", msgs, "pending_bytes", nbytes)
	}
	if msgs, nbytes, err := sub.PendingLimits(); err == nil {
		fields = append(fields, "limit_msgs", msgs, "limit_bytes", nbytes)
	}
	return fields
}

// logSlowConsumer reports a slow-consumer episode and counts it.
func logSlowConsumer(log *slog.Logger, sub *nats.Subscription) {
	dropped, ok := subDropped(sub)
	logSlowConsumerAt(log, dropped, slowConsumerFields(sub, dropped, ok))
	if sub == nil {
		return
	}
	slowConsumerEvents.Add(context.Background(), 1,
		// Both suppressions are about the option below, and both rules report on
		// it, so they share one directive.
		//
		// metrics-no-per-call-attribute-set: unlike a per-item failure loop this
		// fires once per slow-consumer episode with no loop around it, so there
		// is nothing to precompute against — the label set is per-subscription
		// and the subscription set is not known at init.
		//
		// metrics-no-unbounded-label: the rule matches the label key and cannot
		// see that the value is bounded. subjectLabel collapses response inboxes
		// to a constant, and TestSubjectLabel_CollapsesResponseInboxes is what
		// keeps that true — the earlier suppression here claimed boundedness
		// without anything enforcing it, which is how the leak survived.
		// nosemgrep: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
		metric.WithAttributes(
			attribute.String("subject", subjectLabel(sub.Subject)),
			attribute.String("queue", sub.Queue),
		))
}

// inboxLabel replaces any response-inbox subject, whose token is unique per
// connection.
const inboxLabel = "inbox"

// subjectLabel bounds the subject label.
//
// A registered subject is a per-subscription constant — the router registers
// wildcarded patterns — so it is safe as a label. A response inbox is not:
// nats.go opens one shared response subscription per connection on
// `_INBOX.<random>.*`, and while that is a single series at any instant, the
// token is regenerated on every connection. Every restart and every reconnect
// would mint a value that never recurs, which is unbounded growth spread over
// time rather than all at once — and the metrics contract already forbids
// "concrete request/reply inboxes" as labels.
//
// This was suppressed with a nosemgrep and a justification that only considered
// the instant, not the lifetime. The rule was right.
//
// What keeps the *other* half of that justification true — "a registered subject
// is a per-subscription constant" — is TestEverySubscriptionSubjectIsBounded in
// pkg/obs, which fails on any subscribe whose subject is not a pkg/subject
// builder or a stored route pattern. Without it the claim would be exactly the
// kind this file's own history warns about: asserted, and enforced by nothing.
//
// The default prefix is the only one recognised. nats.CustomInboxPrefix would
// produce inboxes this leaves uncollapsed, but Connect is never called with it
// anywhere in the repo, and reading the effective prefix off the connection
// would mean threading it through every caller for a setting nothing uses.
func subjectLabel(subject string) string {
	if strings.HasPrefix(subject, "_INBOX.") {
		return inboxLabel
	}
	return subject
}

// logSlowConsumerAt picks the level: dropped messages on a core subscription are
// unrecoverable loss and should page, so they log at ERROR. A slow consumer that
// has not dropped anything yet is a warning.
func logSlowConsumerAt(log *slog.Logger, dropped int, fields []any) {
	if dropped > 0 {
		log.Error("nats slow consumer, messages dropped", fields...)
		return
	}
	log.Warn("nats slow consumer", fields...)
}
