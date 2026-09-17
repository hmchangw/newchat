package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

// infraFailureDelivery drives one send through HandleJetStreamMsg with a
// canonical publish that always fails — the bare-error path that NAKs — and
// returns whatever the handler replied to the sender.
//
// maxDeliver == numDelivered makes this the configured last delivery, which is
// what IsFinalDeliveryFromContext keys on.
func infraFailureDelivery(t *testing.T, numDelivered uint64, maxDeliver int) []*nats.Msg {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u-1", Account: "alice"},
		Roles: []model.Role{model.RoleMember},
	}, nil).AnyTimes()
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
		Return(roommetacache.Meta{ID: "room-1", UserCount: 1}, nil).AnyTimes()

	var captured []*nats.Msg
	reply := func(_ context.Context, m *nats.Msg) error {
		captured = append(captured, m)
		return nil
	}

	h := NewHandler(store, nil,
		makePublishFunc(nil, errors.New("jetstream unavailable")),
		reply, "site-a", nil, 500, 1, 8192, "",
		withGatekeeperMetrics(newGatekeeperMetrics(mp.Meter("test"))))

	data, err := json.Marshal(model.SendMessageRequest{
		ID:        idgen.GenerateMessageID(),
		Content:   "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	})
	require.NoError(t, err)

	msg := &fakeJSMsg{
		subject:      "chat.user.alice.room.room-1.site-a.msg.send",
		data:         data,
		numDelivered: numDelivered,
	}

	ctx := context.Background()
	consumer := natsmetrics.New(mp.Meter("shared")).Consumer(natsmetrics.ConsumerConfig{
		Site: "site-a", Stream: "MESSAGES-site-a", Consumer: "message-gatekeeper",
	})
	consumer.LoopStarted(ctx)
	tracked := consumer.Track(ctx, msg, natsmetrics.EventSend, maxDeliver)
	var delivery jetstream.Msg = tracked

	h.HandleJetStreamMsg(tracked.Context(ctx), delivery)
	require.True(t, msg.naked, "an infra failure must NAK")
	return captured
}

// TestHandleJetStreamMsg_FinalDeliveryRepliesToSender is the regression guard
// for the silent drop: MESSAGES has exactly one consumer, so once the delivery
// budget is spent the send is gone for good. Before this the sender was never
// told — it published successfully and then waited forever on a reply subject
// nothing would ever publish to.
func TestHandleJetStreamMsg_FinalDeliveryRepliesToSender(t *testing.T) {
	captured := infraFailureDelivery(t, 3, 3)

	require.Len(t, captured, 1, "the last delivery must tell the sender the send failed")

	ee, ok := errcode.Parse(captured[0].Data)
	require.True(t, ok, "the reply must be an errcode envelope, got %q", captured[0].Data)
	assert.Equal(t, errcode.CodeUnavailable, ee.Code,
		"a send exhausted by a dependency outage is unavailable, not a client error")
}

// TestHandleJetStreamMsg_NonFinalDeliveryStaysSilent pins the other half: while
// deliveries remain the message will be retried, so replying would tell the
// sender the send failed when it may still succeed.
func TestHandleJetStreamMsg_NonFinalDeliveryStaysSilent(t *testing.T) {
	captured := infraFailureDelivery(t, 1, 3)
	assert.Empty(t, captured, "a retryable delivery must not report failure to the sender")
}

// TestGuardedProcessor_RecoversPanic is the regression guard for panic
// containment. natsmetrics.Consume spawns a goroutine per message with no
// recover(), and MESSAGES has exactly one consumer — so an unrecovered panic
// takes the process down with the message un-acked, and JetStream redelivers it
// into a crash loop during which the site accepts no messages at all.
func TestGuardedProcessor_RecoversPanic(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	msg := &fakeJSMsg{
		subject:      "chat.user.alice.room.room-1.site-a.msg.send",
		data:         []byte(`{}`),
		numDelivered: 1,
	}
	ctx := context.Background()
	consumer := natsmetrics.New(mp.Meter("shared")).Consumer(natsmetrics.ConsumerConfig{
		Site: "site-a", Stream: "MESSAGES-site-a", Consumer: "message-gatekeeper",
	})
	consumer.LoopStarted(ctx)
	tracked := consumer.Track(ctx, msg, natsmetrics.EventSend, 6)

	guarded := guardedProcessor(func(context.Context, *natsmetrics.Message) {
		panic("handler exploded")
	})

	assert.NotPanics(t, func() { guarded(ctx, tracked) },
		"a handler panic must be contained, not crash the consumer goroutine")
	assert.True(t, msg.acked, "a poison message must be Acked, not left to redeliver into a crash loop")
}
