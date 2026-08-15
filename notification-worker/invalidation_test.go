package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

type invalidationTestMsg struct {
	jetstream.Msg
	data   []byte
	acks   int
	ackErr error
}

func (m *invalidationTestMsg) Data() []byte { return m.data }

// Metadata is needed once a test tracks the message through natsmetrics; the
// embedded nil jetstream.Msg would otherwise panic.
func (m *invalidationTestMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (m *invalidationTestMsg) Ack() error {
	m.acks++
	return m.ackErr
}

func TestProcessInvalidationMessage(t *testing.T) {
	t.Run("enqueues room and acknowledges", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte(`{"type":"muted","roomId":"room-a","account":"alice","muted":true,"timestamp":1}`)}
		queue := make(chan string, 1)

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "room-a", <-queue)
	})

	t.Run("malformed payload acknowledges poison", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte("not-json")}
		queue := make(chan string, 1)

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Empty(t, queue)
	})

	t.Run("full queue acknowledges because ttl reconciles", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte(`{"type":"muted","roomId":"room-b","account":"alice","muted":false,"timestamp":2}`)}
		queue := make(chan string, 1)
		queue <- "already-full"

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "already-full", <-queue)
	})

	t.Run("acknowledgement failure is contained", func(t *testing.T) {
		msg := &invalidationTestMsg{
			data:   []byte(`{"type":"muted","roomId":"room-c","account":"alice","muted":true,"timestamp":3}`),
			ackErr: errors.New("ack unavailable"),
		}
		queue := make(chan string, 1)

		assert.NotPanics(t, func() {
			processInvalidationMessage(context.Background(), msg, queue)
		})

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "room-c", <-queue)
	})
}

// TestProcessInvalidationMessage_MalformedRecordsTerminal pins the evidence
// rule: a payload that will never decode is Acked and permanently dropped, and
// Message.Ack records OutcomeAck — a success. Without an explicit terminal mark
// the drop is indistinguishable from normal processing in the metrics.
func TestProcessInvalidationMessage_MalformedRecordsTerminal(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	consumer := natsmetrics.NewFromProvider(mp).Consumer(natsmetrics.ConsumerConfig{
		ServiceName: "notification-worker", Site: "site-a",
		Stream: "ROOMS-site-a", Consumer: "notification-worker-room-event-invalidate",
	})
	raw := &invalidationTestMsg{data: []byte("not-json")}
	tracked := consumer.Track(context.Background(), raw, natsmetrics.EventUnknown, 5)

	processInvalidationMessage(tracked.Context(context.Background()), tracked, make(chan string, 1))
	tracked.Finish(context.Background())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var terminal int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "chat.nats.terminal.failures" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if string(kv.Key) == "reason" && kv.Value.AsString() == "invalid_payload" {
						terminal += dp.Value
					}
				}
			}
		}
	}
	assert.Equal(t, int64(1), terminal, "a permanently dropped malformed event must leave terminal evidence")
}
