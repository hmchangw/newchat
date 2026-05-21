//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestLoadgenSmallPreset_EndToEnd verifies the generator publishes messages,
// a fake gatekeeper forwards them to MESSAGES_CANONICAL, two JetStream
// consumers drain the stream, a fake broadcast-worker emits room events,
// and MongoDB shows the seeded room data.
func TestLoadgenSmallPreset_EndToEnd(t *testing.T) {
	ctx := context.Background()
	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	defer nc.Drain()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	siteID := "site-test"
	canonical := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     canonical.Name,
		Subjects: canonical.Subjects,
	})
	require.NoError(t, err)

	// Two durable consumers that simply ack — stand in for message-worker
	// and broadcast-worker so the canonical stream drains to zero.
	for _, durable := range []string{"message-worker", "broadcast-worker"} {
		cons, err := js.CreateOrUpdateConsumer(ctx, canonical.Name, jetstream.ConsumerConfig{
			Durable:   durable,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		require.NoError(t, err)
		cc, err := cons.Consume(func(msg jetstream.Msg) { _ = msg.Ack() })
		require.NoError(t, err)
		defer cc.Stop()
	}

	db := testutil.MongoDB(t, "loadgen")

	preset, _ := BuiltinPreset("small")
	fixtures := BuildFixtures(&preset, 42, siteID)
	require.NoError(t, Seed(ctx, db, &fixtures))

	metrics := NewMetrics()
	collector := NewCollector(metrics, preset.Name)

	// Fake gatekeeper: frontdoor subject → publish MessageEvent to canonical.
	gkSub, err := nc.Subscribe(
		subject.MsgSendWildcard(siteID),
		func(m *nats.Msg) {
			var req model.SendMessageRequest
			if err := json.Unmarshal(m.Data, &req); err != nil {
				return
			}
			evt := model.MessageEvent{
				Message: model.Message{
					ID:        req.ID,
					Content:   req.Content,
					CreatedAt: time.Now().UTC(),
				},
				SiteID:    siteID,
				Timestamp: time.Now().UnixMilli(),
			}
			data, _ := json.Marshal(evt)
			_, _ = js.Publish(ctx, subject.MsgCanonicalCreated(siteID), data)
		},
	)
	require.NoError(t, err)
	defer gkSub.Unsubscribe()

	// Fake broadcast-worker: canonical event → room event.
	bwSub, err := nc.Subscribe(
		subject.MsgCanonicalCreated(siteID),
		func(m *nats.Msg) {
			var evt model.MessageEvent
			if err := json.Unmarshal(m.Data, &evt); err != nil {
				return
			}
			roomEvt := model.RoomEvent{
				Type:    model.RoomEventNewMessage,
				RoomID:  "r",
				Message: &model.ClientMessage{Message: evt.Message},
			}
			data, _ := json.Marshal(roomEvt)
			_ = nc.Publish("chat.room.r.event", data)
		},
	)
	require.NoError(t, err)
	defer bwSub.Unsubscribe()

	publisher := &natsCorePublisher{nc: nc}
	gen := NewGenerator(&GeneratorConfig{
		Preset:    &preset,
		Fixtures:  fixtures,
		SiteID:    siteID,
		Rate:      50,
		Inject:    InjectFrontdoor,
		Publisher: publisher,
		Metrics:   metrics,
		Collector: collector,
	}, 42)

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, gen.Run(runCtx))

	time.Sleep(2 * time.Second)

	for _, durable := range []string{"message-worker", "broadcast-worker"} {
		cons, err := js.Consumer(ctx, canonical.Name, durable)
		require.NoError(t, err)
		info, err := cons.Info(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(0), info.NumPending, "durable %s still has pending", durable)
	}

	var room model.Room
	err = db.Collection("rooms").FindOne(ctx, bson.M{"_id": fixtures.Rooms[0].ID}).Decode(&room)
	require.NoError(t, err)
	require.Equal(t, fixtures.Rooms[0].ID, room.ID)
}

func TestMain(m *testing.M) { testutil.RunTests(m) }
