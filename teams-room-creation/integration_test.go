//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// dial returns a connected *o11ynats.Conn backed by the shared test NATS
// server, plus its traced JetStream handle — the same o11ynats.JetStream type
// newJetStreamPublisher takes in main.go (raw jetstream.New(nc) does not
// satisfy that parameter). The connection is drained on test cleanup.
func dial(t *testing.T) (*o11ynats.Conn, o11ynats.JetStream) {
	t.Helper()
	nc, err := natsutil.Connect(context.Background(), testutil.NATS(t), "", noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	js, err := nc.JetStream()
	require.NoError(t, err)
	return nc, js
}

// fetchOne fetches the next message from cons within timeout, acks it, and
// returns its data. o11ynats.Consumer intentionally has no single-message
// Next (see the o11y nats package's jetstream.go doc comment); Fetch with a
// batch of 1 is the documented substitute.
func fetchOne(t *testing.T, ctx context.Context, cons o11ynats.Consumer, timeout time.Duration) []byte {
	t.Helper()
	batch, err := cons.Fetch(ctx, 1, jetstream.FetchMaxWait(timeout))
	require.NoError(t, err)
	select {
	case fm, ok := <-batch.Messages():
		require.True(t, ok, "expected one message on the fetch batch")
		require.NoError(t, fm.Msg.Ack())
		return fm.Msg.Data()
	case <-time.After(timeout):
		t.Fatal("timed out waiting for JetStream message")
		return nil
	}
}

// TestEndToEnd_PublishesAndClearsFlag seeds two needCreateRoom=true teams_chat
// docs for the same site, runs one runner pass against real Mongo + JetStream
// (store built from testutil.MongoDB, publisher built from the o11ynats
// JetStream handle exactly as main.go wires it), and asserts the batch landed
// as a single TeamsRoomCreateEvent on the site's room-canonical subject and
// that needCreateRoom was cleared for both chats.
func TestEndToEnd_PublishesAndClearsFlag(t *testing.T) {
	ctx := context.Background()
	const siteID = "site-a"

	db := testutil.MongoDB(t, "teamsroom-e2e")
	_, err := db.Collection("teams_chat").InsertMany(ctx, []any{
		bson.M{"_id": "c1", "name": "A", "siteId": siteID, "needCreateRoom": true,
			"members": []bson.M{{"account": "alice"}}},
		bson.M{"_id": "c2", "name": "B", "siteId": siteID, "needCreateRoom": true, "members": []bson.M{}},
	})
	require.NoError(t, err)

	_, js := dial(t)

	// Create the ROOMS stream so the publish lands (dev-only; ops owns it in prod).
	rc := stream.Rooms(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: rc.Name, Subjects: rc.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), rc.Name) })

	store := newMongoStore(db, db)
	r := newRunner(store, newJetStreamPublisher(js), runConfig{BatchSize: 10, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(ctx))

	// Both seeded chats share one site, so exactly one event should carry both.
	cons, err := js.CreateOrUpdateConsumer(ctx, rc.Name, jetstream.ConsumerConfig{
		Durable:       "test-teams-room-create-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject.RoomCanonicalTeamsCreate(siteID),
	})
	require.NoError(t, err)

	data := fetchOne(t, ctx, cons, 3*time.Second)
	var evt model.TeamsRoomCreateEvent
	require.NoError(t, json.Unmarshal(data, &evt))
	// The site is identified by the subject we consumed from, not the payload.
	require.Len(t, evt.Chats, 2)
	ids := map[string]bool{}
	for _, c := range evt.Chats {
		ids[c.ID] = true
	}
	assert.True(t, ids["c1"] && ids["c2"], "expected both seeded chats in the batch")

	// Members round-tripped Mongo -> store projection -> buildEvent -> wire event.
	byID := map[string]model.TeamsRoomCreateChat{}
	for _, c := range evt.Chats {
		byID[c.ID] = c
	}
	require.Contains(t, byID, "c1")
	require.Contains(t, byID, "c2")
	require.Len(t, byID["c1"].Members, 1)
	assert.Equal(t, "alice", byID["c1"].Members[0].Account)
	assert.Empty(t, byID["c2"].Members)

	// Flags cleared: no chats remain flagged for room creation.
	remaining, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}
