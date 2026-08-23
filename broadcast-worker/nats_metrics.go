package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// roomKindLabel and deliveryResult are closed enums so a typo is a compile
// error rather than a silent `unknown` series at campaign time.
type roomKindLabel string

const (
	roomChannel roomKindLabel = "channel"
	roomDM      roomKindLabel = "dm"
	roomBotDM   roomKindLabel = "bot_dm"
	roomThread  roomKindLabel = "thread"
	roomUnknown roomKindLabel = "unknown"
)

var allRoomKinds = []roomKindLabel{roomChannel, roomDM, roomBotDM, roomThread, roomUnknown}

type deliveryResult string

const (
	deliverySuccess deliveryResult = "success"
	deliveryFailed  deliveryResult = "failed"
)

var allDeliveryResults = []deliveryResult{deliverySuccess, deliveryFailed}

type fanoutKey struct {
	room  roomKindLabel
	event natsmetrics.EventType
}

type deliveryKey struct {
	room   roomKindLabel
	event  natsmetrics.EventType
	result deliveryResult
}

// broadcastMetrics explains user impact for a fan-out.
//
// The two families answer different questions and are NOT a ratio for channel
// rooms: a channel event is delivered by a single publish to the room stream,
// so fanout records the intended audience (meta.UserCount) while deliveries
// records one attempt. Per-recipient delivery evidence for channels comes from
// the loadgen recipient observer, not from these counters. For DM, bot-DM, and
// thread fan-out the two are directly comparable — one publish per recipient.
type broadcastMetrics struct {
	fanout       metric.Int64Histogram
	deliveries   metric.Int64Counter
	fanoutOpts   map[fanoutKey]metric.MeasurementOption
	deliveryOpts map[deliveryKey]metric.MeasurementOption
	// threadViewOpts is keyed by event type alone — that is the only label this
	// counter carries.
	threadViewOpts map[natsmetrics.EventType]metric.MeasurementOption

	// Failures only: the delivery counter already carries this lane's volume.
	threadViewFailures metric.Int64Counter
}

func newBroadcastMetrics(meter metric.Meter) *broadcastMetrics {
	noopMeter := noop.NewMeterProvider().Meter("broadcast-worker")
	fanout, err := meter.Int64Histogram("broadcast_worker_fanout_recipients",
		metric.WithDescription("Intended fan-out recipients by bounded room and event kind."))
	if err != nil {
		// Telemetry must never block startup: fall back to a no-op instrument so
		// the service runs blind on this metric rather than not at all. The
		// fallback's own error is discarded here and at each instrument below —
		// a no-op meter has no failure mode, and there is nothing left to fall
		// back to if it somehow had one.
		fanout, _ = noopMeter.Int64Histogram("broadcast_worker_fanout_recipients")
	}
	deliveries, err := meter.Int64Counter("broadcast_worker_recipient_deliveries_total",
		metric.WithDescription("Actual Core NATS recipient publish attempts by result."))
	if err != nil {
		deliveries, _ = noopMeter.Int64Counter("broadcast_worker_recipient_deliveries_total")
	}
	threadViewFailures, err := meter.Int64Counter("broadcast_worker_thread_view_publish_failures_total",
		metric.WithDescription("Publishes to the thread-scoped view subject that failed."))
	if err != nil {
		threadViewFailures, _ = noopMeter.Int64Counter("broadcast_worker_thread_view_publish_failures_total")
	}
	m := &broadcastMetrics{
		fanout:             fanout,
		deliveries:         deliveries,
		threadViewFailures: threadViewFailures,
		fanoutOpts:         make(map[fanoutKey]metric.MeasurementOption),
		deliveryOpts:       make(map[deliveryKey]metric.MeasurementOption),
		threadViewOpts:     make(map[natsmetrics.EventType]metric.MeasurementOption),
	}
	// threadViewOpts is keyed by event alone, so it gets its own loop. Filling it
	// inside the room loop below wrote the same eight entries once per room kind
	// and threw away thirty-two of the forty.
	for _, event := range allBroadcastEvents {
		m.threadViewOpts[event] = metric.WithAttributes(attribute.String("event_type", string(event)))
	}
	for _, room := range allRoomKinds {
		roomAttr := attribute.String("room_kind", string(room))
		for _, event := range allBroadcastEvents {
			eventAttr := attribute.String("event_type", string(event))
			m.fanoutOpts[fanoutKey{room, event}] = metric.WithAttributes(roomAttr, eventAttr)
			for _, result := range allDeliveryResults {
				m.deliveryOpts[deliveryKey{room, event, result}] = metric.WithAttributes(
					roomAttr, eventAttr, attribute.String("result", string(result)))
			}
		}
	}
	return m
}

// allBroadcastEvents is the subset of the shared event vocabulary this worker
// can observe, plus unknown for anything it cannot classify.
var allBroadcastEvents = []natsmetrics.EventType{
	natsmetrics.EventCreated, natsmetrics.EventUpdated, natsmetrics.EventDeleted,
	natsmetrics.EventPinned, natsmetrics.EventUnpinned, natsmetrics.EventReacted,
	natsmetrics.EventThreadReplyAdded, natsmetrics.EventUnknown,
}

func normalizeRoomKind(value roomKindLabel) roomKindLabel {
	switch value {
	case roomChannel, roomDM, roomBotDM, roomThread:
		return value
	default:
		return roomUnknown
	}
}

func normalizeBroadcastEvent(value natsmetrics.EventType) natsmetrics.EventType {
	switch value {
	case natsmetrics.EventCreated, natsmetrics.EventUpdated, natsmetrics.EventDeleted,
		natsmetrics.EventPinned, natsmetrics.EventUnpinned, natsmetrics.EventReacted,
		natsmetrics.EventThreadReplyAdded:
		return value
	default:
		return natsmetrics.EventUnknown
	}
}

func (m *broadcastMetrics) Fanout(ctx context.Context, room roomKindLabel, event natsmetrics.EventType, recipients int) {
	if m == nil || m.fanout == nil {
		return
	}
	m.fanout.Record(ctx, int64(recipients), m.fanoutOpts[fanoutKey{normalizeRoomKind(room), normalizeBroadcastEvent(event)}])
}

func (m *broadcastMetrics) Delivery(ctx context.Context, room roomKindLabel, event natsmetrics.EventType, err error) {
	if m == nil || m.deliveries == nil {
		return
	}
	result := deliverySuccess
	if err != nil {
		result = deliveryFailed
	}
	m.deliveries.Add(ctx, 1, m.deliveryOpts[deliveryKey{normalizeRoomKind(room), normalizeBroadcastEvent(event), result}])
}

// ThreadViewPublishFailed counts a failed thread-view publish. Failures only:
// viewers refetch on panel open, so only the rate is worth alerting on.
//
// The option is looked up, not built here. "It only runs after a failure" does
// not make this a cold path: publishThreadViewEvent calls it once per target
// subject inside its fan-out loop, so a broker outage runs it for every target
// of every thread event — the moment the label set stops being cheap is exactly
// the moment this counter matters.
func (m *broadcastMetrics) ThreadViewPublishFailed(ctx context.Context, event natsmetrics.EventType) {
	if m == nil || m.threadViewFailures == nil {
		return
	}
	m.threadViewFailures.Add(ctx, 1, m.threadViewOpts[normalizeBroadcastEvent(event)])
}

type broadcastMetricLabels struct {
	roomKind  roomKindLabel
	eventType natsmetrics.EventType
}

type broadcastMetricKey struct{}

func withBroadcastMetricLabels(ctx context.Context, roomKind roomKindLabel, eventType natsmetrics.EventType) context.Context {
	return context.WithValue(ctx, broadcastMetricKey{}, broadcastMetricLabels{normalizeRoomKind(roomKind), normalizeBroadcastEvent(eventType)})
}

func broadcastLabels(ctx context.Context) broadcastMetricLabels {
	labels, _ := ctx.Value(broadcastMetricKey{}).(broadcastMetricLabels)
	labels.roomKind = normalizeRoomKind(labels.roomKind)
	labels.eventType = normalizeBroadcastEvent(labels.eventType)
	return labels
}

type broadcastMetricPublisher struct {
	next    Publisher
	metrics *broadcastMetrics
}

func (p *broadcastMetricPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	err := p.next.Publish(ctx, subject, data)
	labels := broadcastLabels(ctx)
	p.metrics.Delivery(ctx, labels.roomKind, labels.eventType, err)
	return err
}
