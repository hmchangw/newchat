package main

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
)

type broadcastMetrics struct {
	fanout     metric.Int64Histogram
	deliveries metric.Int64Counter
}

func classifyBroadcastDelivery(msg jetstream.Msg) natsmetrics.EventType {
	var evt struct {
		Event model.EventType `json:"event"`
	}
	if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
		return natsmetrics.EventUnknown
	}
	return natsmetrics.NormalizeEventType(string(evt.Event))
}

func newBroadcastMetrics(meter metric.Meter) *broadcastMetrics {
	fanout, _ := meter.Int64Histogram("broadcast_worker_fanout_recipients",
		metric.WithDescription("Intended fan-out recipients by bounded room and event kind."))
	deliveries, _ := meter.Int64Counter("broadcast_worker_recipient_deliveries_total",
		metric.WithDescription("Actual Core NATS recipient publish attempts by result."))
	return &broadcastMetrics{fanout: fanout, deliveries: deliveries}
}

func normalizeRoomKind(value string) string {
	switch value {
	case "channel", "dm", "bot_dm", "thread":
		return value
	default:
		return "unknown"
	}
}

func normalizeBroadcastEvent(value string) string {
	switch value {
	case "created", "updated", "deleted", "pinned", "unpinned", "reacted", "thread_reply_added":
		return value
	default:
		return "unknown"
	}
}

func (m *broadcastMetrics) Fanout(ctx context.Context, roomKind, eventType string, recipients int) {
	if m == nil || m.fanout == nil {
		return
	}
	m.fanout.Record(ctx, int64(recipients), metric.WithAttributes(
		attribute.String("room_kind", normalizeRoomKind(roomKind)),
		attribute.String("event_type", normalizeBroadcastEvent(eventType)),
	))
}

func (m *broadcastMetrics) Delivery(ctx context.Context, roomKind, eventType string, err error) {
	if m == nil || m.deliveries == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "failed"
	}
	m.deliveries.Add(ctx, 1, metric.WithAttributes(
		attribute.String("room_kind", normalizeRoomKind(roomKind)),
		attribute.String("event_type", normalizeBroadcastEvent(eventType)),
		attribute.String("result", result),
	))
}

type broadcastMetricLabels struct{ roomKind, eventType string }
type broadcastMetricKey struct{}

func withBroadcastMetricLabels(ctx context.Context, roomKind, eventType string) context.Context {
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
