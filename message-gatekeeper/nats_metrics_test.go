package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
)

func TestCanonicalPublishError_PreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("stream unavailable")
	err := fmt.Errorf("publish to MESSAGES-CANONICAL: %w", errors.Join(errCanonicalPublish, cause))

	assert.ErrorIs(t, err, errCanonicalPublish)
	assert.ErrorIs(t, err, cause)
}

func gatekeeperCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "message_gatekeeper_messages_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["result"]+"/"+values["reason"]] = point.Value
			}
		}
	}
	return got
}

func TestGatekeeperMetrics_BoundedResults(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newGatekeeperMetrics(mp.Meter("test"))

	m.Record(context.Background(), resultAccepted, reasonNone)
	// A value outside the enum can only arrive via a conversion; it must still
	// collapse instead of minting an unbounded series.
	m.Record(context.Background(), gatekeeperResult("dynamic"), gatekeeperReasonCode("secret error text"))

	assert.Equal(t, map[string]int64{"accepted/none": 1, "failed/unknown": 1}, gatekeeperCounts(t, reader))
}

func TestHandler_HandleJetStreamMsg_RecordsRejectedOutcome(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	h := NewHandler(nil, nil, nil, func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
	msg := &fakeJSMsg{subject: "chat.invalid", data: []byte(`{}`)}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked)
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"rejected/invalid_subject": 1}, gatekeeperCounts(t, reader))
}

func TestHandler_HandleJetStreamMsg_RecordsAcceptedAndRetryOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name       string
		publishErr error
		wantMetric map[string]int64
		wantAck    bool
		wantNak    bool
		final      bool
	}{
		{name: "accepted", wantMetric: map[string]int64{"accepted/none": 1}, wantAck: true},
		{name: "canonical publish retry", publishErr: errors.New("stream unavailable"), wantMetric: map[string]int64{"retry/canonical_publish": 1}, wantNak: true},
		{name: "canonical publish exhausted", publishErr: errors.New("stream unavailable"), wantMetric: map[string]int64{"failed/canonical_publish": 1}, wantNak: true, final: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newGatekeeperMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), "weather.bot", "room-1").Return(&model.Subscription{
				User:  model.SubscriptionUser{ID: "u-bot", Account: "weather.bot"},
				Roles: []model.Role{model.RoleMember},
			}, nil)
			h := NewHandler(store, nil, makePublishFunc(nil, tt.publishErr), func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
			request := model.SendMessageRequest{
				ID: idgen.GenerateMessageID(), Content: "hello",
				RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
			}
			data, err := json.Marshal(request)
			require.NoError(t, err)
			msg := &fakeJSMsg{subject: "chat.user.weather_bot.room.room-1.site-a.msg.send", data: data}
			ctx := context.Background()
			var delivery jetstream.Msg = msg
			if tt.final {
				msg.numDelivered = 1
				consumer := natsmetrics.New(mp.Meter("shared")).Consumer(natsmetrics.ConsumerConfig{
					Site: "site-a", Stream: "MESSAGES_site-a", Consumer: "message-gatekeeper",
				})
				consumer.LoopStarted(ctx)
				tracked := consumer.Track(ctx, msg, natsmetrics.EventSend, 1)
				delivery = tracked
				ctx = tracked.Context(ctx)
			}

			h.HandleJetStreamMsg(ctx, delivery)

			assert.Equal(t, tt.wantAck, msg.acked)
			assert.Equal(t, tt.wantNak, msg.naked)
			assert.Equal(t, tt.wantMetric, gatekeeperCounts(t, reader))
		})
	}
}

func TestGatekeeperMetrics_Record_NilReceiverIsSafe(t *testing.T) {
	var metrics *gatekeeperMetrics
	metrics.Record(context.Background(), resultAccepted, reasonNone)
}

// A permanent server-side fault (e.g. a value that cannot be marshaled) is
// undeliverable work, not a client rejection: it must Ack — retrying can never
// succeed — but count as failed/internal so the rejection series stays a pure
// signal of client errors.
func TestHandler_HandleJetStreamMsg_PermanentFault_RecordsFailedNotRejected(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
		Return(nil, errcode.MarshalFailed("message event", errors.New("json: unsupported value: NaN")))

	h := NewHandler(store, nil, nil, func(context.Context, *nats.Msg) error { return nil },
		"site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
	request := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	data, err := json.Marshal(request)
	require.NoError(t, err)
	msg := &fakeJSMsg{subject: "chat.user.alice.room.room-1.site-a.msg.send", data: data}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked, "a permanent fault can never succeed on redelivery — Ack-drop it")
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"failed/internal": 1}, gatekeeperCounts(t, reader))
}

// A canonical publish rejected for exceeding max_payload fails identically on
// every redelivery: Ack-drop it (with a reply) instead of Nakking to MaxDeliver.
func TestHandler_HandleJetStreamMsg_OversizedCanonicalPublish_IsPermanent(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "weather.bot", "room-1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u-bot", Account: "weather.bot"},
		Roles: []model.Role{model.RoleMember},
	}, nil)
	h := NewHandler(store, nil, makePublishFunc(nil, nats.ErrMaxPayload),
		func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
		withGatekeeperMetrics(metrics))
	request := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	data, err := json.Marshal(request)
	require.NoError(t, err)
	msg := &fakeJSMsg{subject: "chat.user.weather_bot.room.room-1.site-a.msg.send", data: data}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked, "an oversized message can never be published — Ack-drop it")
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"failed/internal": 1}, gatekeeperCounts(t, reader))
}
