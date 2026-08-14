package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

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
}

func newBroadcastMetrics(meter metric.Meter) *broadcastMetrics {
	fanout, _ := meter.Int64Histogram("broadcast_worker_fanout_recipients",
		metric.WithDescription("Intended fan-out recipients by bounded room and event kind."))
	deliveries, _ := meter.Int64Counter("broadcast_worker_recipient_deliveries_total",
		metric.WithDescription("Actual Core NATS recipient publish attempts by result."))
	m := &broadcastMetrics{
		fanout:       fanout,
		deliveries:   deliveries,
		fanoutOpts:   make(map[fanoutKey]metric.MeasurementOption),
		deliveryOpts: make(map[deliveryKey]metric.MeasurementOption),
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
