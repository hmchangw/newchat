//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// startSourceMongo stands up a single-node Mongo replica set and returns a client + URI. Inline per
// the CLAUDE.md exception: testutil only offers standalone Mongo, but the transformer needs a writable source collection.
func startSourceMongo(t *testing.T) *mongo.Client {
	t.Helper()
	ctx := context.Background()
	container, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, container.Terminate(context.Background())) })

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	client, err := mongoutil.Connect(ctx, uri, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { mongoutil.Disconnect(context.Background(), client) })
	return client
}

// stubHistory satisfies historyClient but is never invoked on the insert path.
type stubHistory struct{}

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (stubHistory) Edit(context.Context, model.MigrationEditRequest) error { return nil }

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (stubHistory) Delete(context.Context, model.MigrationDeleteRequest) error { return nil }

// TestTransformer_InsertToCanonical drives an insert oplog event through a handler wired to a real
// JetStream publisher and reads the canonical .created envelope back off MESSAGES_CANONICAL.
func TestTransformer_InsertToCanonical(t *testing.T) {
	const (
		site = "site1"
		coll = "rocketchat_message"
	)
	client := startSourceMongo(t)
	source := client.Database("rocketchat").Collection(coll)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Seed the source doc, then read it back as relaxed extJSON via the real lookup so the
	// FullDocument we feed handle() is byte-shaped exactly as the connector emits.
	const msgID = "abc123def456ghi78"
	_, err := source.InsertOne(ctx, bson.M{
		"_id": msgID,
		"rid": "room1",
		"msg": "hello world",
		"ts":  time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC),
		"u":   bson.M{"_id": "u1", "username": "alice", "name": "Alice A"},
	})
	require.NoError(t, err)

	lookup := migration.NewMongoSourceLookup(source)
	fullDoc, err := lookup.FindByID(ctx, msgID)
	require.NoError(t, err)
	require.NotEmpty(t, fullDoc)

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	canonical := stream.MessagesCanonical(site)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: canonical.Name, Subjects: canonical.Subjects})
	require.NoError(t, err)

	h := &handler{
		collection:     coll,
		softDeleteType: "rm",
		publisher:      &canonicalPublisher{siteID: site, publish: js.PublishMsg, now: nowMs},
		history:        stubHistory{},
		lookup:         lookup,
	}

	require.NoError(t, h.handle(ctx, oplogEvent{
		Collection:   coll,
		Op:           "insert",
		FullDocument: fullDoc,
	}))

	cons, err := js.CreateOrUpdateConsumer(ctx, canonical.Name, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subject.MsgCanonicalCreated(site)},
	})
	require.NoError(t, err)

	var got jetstream.Msg
	require.Eventually(t, func() bool {
		batch, berr := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for msg := range batch.Messages() {
			assert.NoError(t, msg.Ack())
			got = msg
			return true
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "canonical .created envelope must land on MESSAGES_CANONICAL")

	require.NotNil(t, got)
	assert.Equal(t, subject.MsgCanonicalCreated(site), got.Subject())

	// IsMigrationLive reads a raw *nats.Msg header — reconstruct one from the consumed headers.
	raw := &nats.Msg{Subject: got.Subject(), Header: got.Headers()}
	assert.True(t, natsutil.IsMigrationLive(raw), "migrated inserts carry the X-Migration: live header")

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(got.Data(), &evt))
	assert.Equal(t, model.EventCreated, evt.Event)
	assert.Equal(t, site, evt.SiteID)
	assert.Equal(t, msgID, evt.Message.ID)
	assert.Equal(t, "room1", evt.Message.RoomID)
}

// fakeHistoryResponder records the MigrationDeleteRequest it receives and replies ok.
type fakeHistoryResponder struct {
	mu      sync.Mutex
	deletes []model.MigrationDeleteRequest
}

func (f *fakeHistoryResponder) snapshot() []model.MigrationDeleteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.MigrationDeleteRequest, len(f.deletes))
	copy(out, f.deletes)
	return out
}

// TestTransformer_SoftDeleteToHistory drives a soft-delete (t:"rm") update through a handler wired
// to a real natsHistoryClient, and asserts the fake history responder receives the delete request.
func TestTransformer_SoftDeleteToHistory(t *testing.T) {
	const (
		site = "site1"
		coll = "rocketchat_message"
	)
	client := startSourceMongo(t)
	source := client.Database("rocketchat").Collection(coll)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const msgID = "del123def456ghi78"
	_, err := source.InsertOne(ctx, bson.M{
		"_id":      msgID,
		"rid":      "room9",
		"msg":      "",
		"t":        "rm",
		"ts":       time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC),
		"editedAt": time.Date(2023, 1, 2, 5, 0, 0, 0, time.UTC),
		"u":        bson.M{"_id": "u1", "username": "alice", "name": "Alice A"},
	})
	require.NoError(t, err)

	nc, err := natsutil.Connect(testutil.NATS(t), "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()

	responder := &fakeHistoryResponder{}
	// Subscribe on the raw *nats.Conn so the callback is a plain func(*nats.Msg) — the
	// otelnats wrapper's Subscribe takes its own MsgHandler shape and we want raw test plumbing.
	sub, err := nc.NatsConn().Subscribe(subject.MigrationInternalMsgDelete(site), func(msg *nats.Msg) {
		var req model.MigrationDeleteRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond([]byte(`{"ok":false}`))
			return
		}
		responder.mu.Lock()
		responder.deletes = append(responder.deletes, req)
		responder.mu.Unlock()
		_ = msg.Respond([]byte(`{"ok":true}`))
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, sub.Unsubscribe()) }()

	h := &handler{
		collection:     coll,
		softDeleteType: "rm",
		publisher:      &canonicalPublisher{siteID: site, publish: nil, now: nowMs}, // unused on delete path
		history:        &natsHistoryClient{nc: nc.NatsConn(), siteID: site, timeout: 5 * time.Second},
		lookup:         migration.NewMongoSourceLookup(source),
	}

	require.NoError(t, h.handle(ctx, oplogEvent{
		Collection:  coll,
		Op:          "update",
		DocumentKey: []byte(fmt.Sprintf(`{"_id":%q}`, msgID)),
	}))

	got := responder.snapshot()
	require.Len(t, got, 1, "history responder must receive exactly one soft-delete request")
	assert.Equal(t, msgID, got[0].MessageID)
	// MigrationDeleteRequest is id-only now — history resolves room/createdAt by GetMessageByID.
}
