package natsmetrics

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	benchmarkTracked *Message
	benchmarkContext context.Context
)

func benchmarkConsumer() *Consumer {
	metrics := New(noop.NewMeterProvider().Meter("benchmark"))
	return metrics.Consumer(ConsumerConfig{
		ServiceName: "benchmark-worker",
		Site:        "site-a",
		Stream:      "MESSAGES_CANONICAL_site-a",
		Consumer:    "benchmark-worker",
	})
}

// BenchmarkConsumer_Track isolates the repository-owned wrapper cost. fakeMsg
// returns pre-decoded metadata, so nats.go reply-subject parsing is not included.
func BenchmarkConsumer_Track(b *testing.B) {
	consumer := benchmarkConsumer()
	ctx := context.Background()

	for _, delivered := range []uint64{1, 2} {
		name := "first_delivery"
		if delivered > 1 {
			name = "redelivery"
		}
		b.Run(name, func(b *testing.B) {
			msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: delivered}}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkTracked = consumer.Track(ctx, msg, EventCreated, 5)
			}
		})
	}
}

func BenchmarkMessage_Context(b *testing.B) {
	consumer := benchmarkConsumer()
	tracked := consumer.Track(context.Background(), &fakeMsg{
		meta: &jetstream.MsgMetadata{NumDelivered: 1},
	}, EventCreated, 5)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkContext = tracked.Context(ctx)
	}
}
