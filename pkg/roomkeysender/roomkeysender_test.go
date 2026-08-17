package roomkeysender_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/roomkeysender"
)

// mockPublisher captures the subject and data from the last Publish call.
type mockPublisher struct {
	subject string
	data    []byte
	err     error // error to return from Publish
}

func (m *mockPublisher) Publish(subject string, data []byte) error {
	m.subject = subject
	m.data = data
	return m.err
}

func TestSender_Send(t *testing.T) {
	priv32 := make([]byte, 32)
	for i := range priv32 {
		priv32[i] = byte(i + 100)
	}

	tests := []struct {
		name       string
		account    string
		evt        model.RoomKeyEvent
		publishErr error
		wantSubj   string
		wantErr    string
	}{
		{
			name:    "valid send",
			account: "alice",
			evt: model.RoomKeyEvent{
				RoomID:     "room-1",
				Version:    0,
				PrivateKey: priv32,
			},
			wantSubj: "chat.user.alice.event.room.key",
		},
		{
			name:    "different user produces different subject",
			account: "bob",
			evt: model.RoomKeyEvent{
				RoomID:     "room-2",
				Version:    1,
				PrivateKey: []byte{0x0a},
			},
			wantSubj: "chat.user.bob.event.room.key",
		},
		{
			name:    "publish error is wrapped and returned",
			account: "carol",
			evt: model.RoomKeyEvent{
				RoomID:     "room-3",
				Version:    2,
				PrivateKey: []byte{0x01},
			},
			publishErr: errors.New("connection lost"),
			wantErr:    "publish room key event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Deep-copy the caller's event for the post-call non-mutation check:
			// the shallow struct copy alone would share PrivateKey backing array
			// with tt.evt, so an in-place slice mutation by Send would be
			// invisible to a plain assert.Equal(before, tt.evt).
			before := tt.evt
			before.PrivateKey = append([]byte(nil), tt.evt.PrivateKey...)

			pub := &mockPublisher{err: tt.publishErr}
			sender := roomkeysender.NewSender(pub)

			err := sender.Send(tt.account, tt.evt)

			// Non-mutation contract: Send takes the event by value and stamps Timestamp
			// on its local copy — the caller's struct must be unchanged on success or error.
			assert.Equal(t, before, tt.evt, "Send must not mutate the caller's RoomKeyEvent")

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.ErrorIs(t, err, tt.publishErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSubj, pub.subject)

			// Verify payload round-trips correctly.
			var got model.RoomKeyEvent
			require.NoError(t, json.Unmarshal(pub.data, &got))
			assert.Equal(t, tt.evt.RoomID, got.RoomID)
			assert.Equal(t, tt.evt.Version, got.Version)
			assert.Equal(t, tt.evt.PrivateKey, got.PrivateKey)
			assert.Greater(t, got.Timestamp, int64(0))
		})
	}
}

// TestSender_Marshal verifies Marshal stamps a timestamp and serializes the
// event once into reusable bytes, without mutating the caller's struct.
func TestSender_Marshal(t *testing.T) {
	evt := model.RoomKeyEvent{RoomID: "room-1", Version: 7, PrivateKey: []byte{0x01, 0x02}}
	before := evt
	before.PrivateKey = append([]byte(nil), evt.PrivateKey...)

	sender := roomkeysender.NewSender(&mockPublisher{})
	data, err := sender.Marshal(evt)
	require.NoError(t, err)

	// Non-mutation contract: Marshal takes the event by value.
	assert.Equal(t, before, evt, "Marshal must not mutate the caller's RoomKeyEvent")

	var got model.RoomKeyEvent
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt.RoomID, got.RoomID)
	assert.Equal(t, evt.Version, got.Version)
	assert.Equal(t, evt.PrivateKey, got.PrivateKey)
	assert.Greater(t, got.Timestamp, int64(0), "Marshal must stamp a timestamp")
}

// TestSender_SendData publishes pre-marshaled bytes verbatim to the account's
// room-key subject — the marshal-once fan-out building block.
func TestSender_SendData(t *testing.T) {
	t.Run("publishes bytes to the account subject", func(t *testing.T) {
		pub := &mockPublisher{}
		sender := roomkeysender.NewSender(pub)
		payload := []byte(`{"roomId":"r","version":3}`)

		require.NoError(t, sender.SendData("alice", payload))
		assert.Equal(t, "chat.user.alice.event.room.key", pub.subject)
		assert.Equal(t, payload, pub.data, "SendData must publish the bytes verbatim")
	})

	t.Run("wraps publish errors", func(t *testing.T) {
		sentinel := errors.New("connection lost")
		sender := roomkeysender.NewSender(&mockPublisher{err: sentinel})
		err := sender.SendData("bob", []byte("{}"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "publish room key event")
		assert.ErrorIs(t, err, sentinel)
	})
}

// TestSender_WithoutMetrics pins the un-instrumented path. bot-room-service and
// every handler test construct a Sender with no options, so the zero-value
// Publisher must stay inert rather than panic on the fan-out hot path.
func TestSender_WithoutMetrics(t *testing.T) {
	pub := &mockPublisher{}
	sender := roomkeysender.NewSender(pub)

	require.NoError(t, sender.SendDataContext(context.Background(), "alice", []byte("{}")))
	assert.Equal(t, "chat.user.alice.event.room.key", pub.subject)
}

// TestSender_WithMetrics records one bounded publish attempt per send. The key
// subject carries an account token, so the recorded labels must come from the
// classifier rather than the subject itself.
func TestSender_WithMetrics(t *testing.T) {
	tests := []struct {
		name        string
		publishErr  error
		wantOutcome string
	}{
		{name: "success", wantOutcome: "success"},
		{name: "publish failure", publishErr: errors.New("connection lost"), wantOutcome: "other_error"},
		{name: "disconnected", publishErr: nats.ErrConnectionClosed, wantOutcome: "disconnected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := natsmetrics.NewFromProvider(mp).Publisher("room-worker", "site-a")
			sender := roomkeysender.NewSender(&mockPublisher{err: tt.publishErr}, roomkeysender.WithMetrics(metrics))

			err := sender.SendDataContext(context.Background(), "alice", []byte("{}"))
			if tt.publishErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))

			var attempts int64
			var gotAttrs map[string]string
			for _, scope := range rm.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name != "chat.nats.publish.attempts" {
						continue
					}
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						attrs := map[string]string{}
						for _, kv := range point.Attributes.ToSlice() {
							attrs[string(kv.Key)] = kv.Value.AsString()
						}
						attempts += point.Value
						gotAttrs = attrs
					}
				}
			}

			require.Equal(t, int64(1), attempts, "exactly one publish attempt per send")
			assert.Equal(t, "recipient_event", gotAttrs["destination_kind"])
			assert.Equal(t, "room_publish", gotAttrs["operation"])
			assert.Equal(t, tt.wantOutcome, gotAttrs["outcome"])
			assert.Equal(t, "room-worker", gotAttrs["service_name"])
			assert.NotContains(t, gotAttrs, "account", "the account token must never become a label")
		})
	}
}
