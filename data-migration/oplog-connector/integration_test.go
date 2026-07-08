//go:build integration

package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// snapshot returns a copy of the captured messages under lock. Defined here
// (integration-tagged) because only the integration tests read concurrently.
func (p *fakePublisher) snapshot() []*nats.Msg {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*nats.Msg, len(p.msgs))
	copy(out, p.msgs)
	return out
}

// startReplicaSet stands up a single-node Mongo replica set (required for change streams)
// and returns a client + URI. Inline per the CLAUDE.md exception: testutil only offers standalone Mongo.
func startReplicaSet(t *testing.T) (*mongo.Client, string) {
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
	return client, uri
}

func createSourceCollection(t *testing.T, db *mongo.Database, coll string) *mongo.Collection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// No changeStreamPreAndPostImages: the connector does no pre-image/lookup.
	require.NoError(t, db.CreateCollection(ctx, coll))
	return db.Collection(coll)
}

// TestConnector_RealPublishEndToEnd runs the full connector (start → real NATS publish) and reads
// the envelope back off MIGRATION_OPLOG — covering main.go wiring and the real o11y/nats JetStream path.
func TestConnector_RealPublishEndToEnd(t *testing.T) {
	const coll = "rocketchat_message"
	client, uri := startReplicaSet(t)
	source := createSourceCollection(t, client.Database("rocketchat"), coll)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config{
		SiteID:           "site1",
		SourceMongoURI:   uri,
		SourceDB:         "rocketchat",
		CheckpointDB:     "migration",
		NatsURL:          testutil.NATS(t),
		WatchCollections: []string{coll},
		ReadPreference:   "primaryPreferred",
		CheckpointEvery:  1,
		StartMode:        "now",
		Bootstrap:        bootstrapConfig{Enabled: true},
	}

	t.Setenv("OTEL_SERVICE_NAME", "oplog-connector-test")
	t.Setenv("O11Y_TRACE_ENABLED", "false")
	t.Setenv("O11Y_METRICS_ENABLED", "false")
	t.Setenv("O11Y_LOG_ENABLED", "false")
	sdk, sdkShutdown, err := obs.Init(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkShutdown(context.Background()) })

	conn, err := start(ctx, &cfg, nil, sdk, sdk.Propagator)
	require.NoError(t, err)
	defer conn.Close()

	_, err = source.InsertOne(ctx, bson.M{"_id": "m1", "msg": "hi"})
	require.NoError(t, err)

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, "", sdk.TracerProvider(), sdk.Propagator)
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := jetstream.New(nc.NatsConn())
	require.NoError(t, err)

	var gotID string
	require.Eventually(t, func() bool {
		cons, cerr := js.CreateOrUpdateConsumer(ctx, "MIGRATION_OPLOG_site1", jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{"chat.migration.oplog.site1.>"},
		})
		if cerr != nil {
			return false
		}
		batch, berr := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for m := range batch.Messages() {
			assert.NoError(t, m.Ack())
			if m.Subject() == "chat.migration.oplog.site1.rocketchat_message.insert" {
				gotID = m.Headers().Get("Nats-Msg-Id")
			}
		}
		return gotID != ""
	}, 40*time.Second, 500*time.Millisecond, "insert envelope must land on MIGRATION_OPLOG")

	assert.NotEmpty(t, gotID, "published event carries a Nats-Msg-Id dedup key")
}

// TestOplogConnector_ChangeStreamEndToEnd drives a real change stream through the real watcher
// and asserts the published envelopes — exercising source_mongo + handler + envelope against live CDC.
func TestOplogConnector_ChangeStreamEndToEnd(t *testing.T) {
	const coll = "rocketchat_message"
	client, _ := startReplicaSet(t)
	source := createSourceCollection(t, client.Database("rocketchat"), coll)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Open the change stream BEFORE mutating so "from now" captures everything.
	// No federation filter here — this test exercises the dumb-pump pass-through of all ops.
	src, err := openMongoChangeSource(ctx, source, startPoint{Kind: startFromNow}, false)
	require.NoError(t, err)

	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := newWatcher("site1", coll, src, pub, store, 1, time.Hour)
	w.initialBackoff = time.Millisecond

	runCtx, runCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- w.run(runCtx) }()

	// insert → update → delete.
	_, err = source.InsertOne(ctx, bson.M{"_id": "m1", "msg": "hello"})
	require.NoError(t, err)
	_, err = source.UpdateOne(ctx, bson.M{"_id": "m1"}, bson.M{"$set": bson.M{"msg": "edited"}})
	require.NoError(t, err)
	_, err = source.DeleteOne(ctx, bson.M{"_id": "m1"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(pub.snapshot()) >= 3 }, 30*time.Second, 50*time.Millisecond,
		"expected insert+update+delete envelopes")

	runCancel()
	require.NoError(t, <-runErr, "watcher exits cleanly on ctx cancel")

	msgs := pub.snapshot()
	require.GreaterOrEqual(t, len(msgs), 3)

	// Assert the first three ops in oplog order.
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.insert", msgs[0].Subject)
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.update", msgs[1].Subject)
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.delete", msgs[2].Subject)

	for _, m := range msgs[:3] {
		assert.NotEmpty(t, m.Header.Get("Nats-Msg-Id"), "every event carries a dedup id")
	}

	var insertEvt, updateEvt, deleteEvt struct {
		Op                string          `json:"op"`
		Collection        string          `json:"coll"`
		FullDocument      json.RawMessage `json:"fullDocument"`
		UpdateDescription json.RawMessage `json:"updateDescription"`
		PreImage          json.RawMessage `json:"preImage"`
		ClusterTime       int64           `json:"clusterTime"`
	}
	require.NoError(t, json.Unmarshal(msgs[0].Data, &insertEvt))
	assert.Equal(t, "insert", insertEvt.Op)
	assert.Equal(t, "rocketchat_message", insertEvt.Collection)
	assert.NotEmpty(t, insertEvt.FullDocument, "insert carries the native document")
	assert.Greater(t, insertEvt.ClusterTime, int64(0))

	// Update carries the raw delta, NOT a looked-up post-image.
	require.NoError(t, json.Unmarshal(msgs[1].Data, &updateEvt))
	assert.Equal(t, "update", updateEvt.Op)
	assert.Empty(t, updateEvt.FullDocument, "update carries no post-image (no updateLookup)")
	assert.NotEmpty(t, updateEvt.UpdateDescription, "update carries the change delta")
	assert.Contains(t, string(updateEvt.UpdateDescription), "edited")

	// Delete carries only the documentKey — no post-image, no pre-image.
	require.NoError(t, json.Unmarshal(msgs[2].Data, &deleteEvt))
	assert.Equal(t, "delete", deleteEvt.Op)
	assert.Empty(t, deleteEvt.FullDocument, "delete carries no post-image")
	assert.Empty(t, deleteEvt.PreImage, "delete carries no pre-image (connector does no lookups)")

	// A checkpoint was persisted (post-ack) for the published events.
	assert.NotEmpty(t, saved.ids())
}

func TestOplogConnector_FederationFilterDropsForeignInserts(t *testing.T) {
	const coll = "rocketchat_message"
	client, _ := startReplicaSet(t)
	source := createSourceCollection(t, client.Database("rocketchat"), coll)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Federation filter ON (this collection is the message collection in the test).
	src, err := openMongoChangeSource(ctx, source, startPoint{Kind: startFromNow}, true)
	require.NoError(t, err)

	pub := &fakePublisher{}
	store, _ := captureStore(t)
	w := newWatcher("site1", coll, src, pub, store, 1, time.Hour)
	w.initialBackoff = time.Millisecond

	runCtx, runCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- w.run(runCtx) }()

	// A foreign-origin insert (must be dropped by the $match) BEFORE a local insert (must pass).
	// Ordering matters: if the foreign event were going to publish, it would precede local1.
	_, err = source.InsertOne(ctx, bson.M{"_id": "foreign1", "msg": "from site-a",
		"federation": bson.M{"origin": "site-a.example.internal"}})
	require.NoError(t, err)
	_, err = source.InsertOne(ctx, bson.M{"_id": "local1", "msg": "from here"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, m := range pub.snapshot() {
			if strings.Contains(string(m.Data), "local1") {
				return true
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "local-origin insert should be published")

	runCancel()
	require.NoError(t, <-runErr, "watcher exits cleanly on ctx cancel")

	for _, m := range pub.snapshot() {
		assert.NotContains(t, string(m.Data), "foreign1",
			"foreign-origin insert must be dropped by the federation $match")
		assert.NotContains(t, string(m.Data), "site-a.example.internal")
	}
}

func TestMongoCheckpointStore_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "oplogcp")
	store := NewMongoCheckpointStore(db.Collection(checkpointCollection), "site1")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Absent → (nil, nil).
	got, err := store.Load(ctx, "rocketchat_message")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Save then Load round-trips, resume token byte-identical.
	token, err := bson.Marshal(bson.M{"_data": "RT-ABC"})
	require.NoError(t, err)
	cp := &Checkpoint{
		SiteID:      "site1",
		Collection:  "rocketchat_message",
		ResumeToken: token,
		ClusterTime: 1718100000000,
		EventID:     "EVT1",
		Source:      "seed",
		UpdatedAt:   1718100000123,
	}
	require.NoError(t, store.Save(ctx, cp))

	loaded, err := store.Load(ctx, "rocketchat_message")
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, "site1:rocketchat_message", loaded.ID)
	assert.Equal(t, "EVT1", loaded.EventID)
	assert.Equal(t, "seed", loaded.Source)
	assert.Equal(t, int64(1718100000000), loaded.ClusterTime)
	assert.Equal(t, token, []byte(loaded.ResumeToken), "resume token round-trips byte-identical")

	// Second Save with same key upserts (one doc, updated fields).
	cp2 := &Checkpoint{
		SiteID: "site1", Collection: "rocketchat_message",
		ResumeToken: token, EventID: "EVT2", Source: "runtime", UpdatedAt: 1718100001000,
	}
	require.NoError(t, store.Save(ctx, cp2))

	count, err := db.Collection(checkpointCollection).CountDocuments(ctx, bson.M{"_id": "site1:rocketchat_message"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "Save upserts in place")

	reloaded, err := store.Load(ctx, "rocketchat_message")
	require.NoError(t, err)
	assert.Equal(t, "EVT2", reloaded.EventID)
	assert.Equal(t, "runtime", reloaded.Source)
}

// TestConnector_CollectionsRole_DisjointSet starts a collections-role connector (message collection
// configured but NOT watched) and asserts it publishes only its own collections' subjects.
func TestConnector_CollectionsRole_DisjointSet(t *testing.T) {
	client, uri := startReplicaSet(t)
	rooms := createSourceCollection(t, client.Database("rocketchat"), "rocketchat_room")
	msgs := createSourceCollection(t, client.Database("rocketchat"), "rocketchat_message")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config{
		SiteID:            "sitecr",
		SourceMongoURI:    uri,
		SourceDB:          "rocketchat",
		CheckpointDB:      "migration",
		NatsURL:           testutil.NATS(t),
		WatchCollections:  []string{"rocketchat_room"},
		MessageCollection: "rocketchat_message", // not watched — collections role
		ReadPreference:    "primaryPreferred",
		CheckpointEvery:   1,
		StartMode:         "now",
		Bootstrap:         bootstrapConfig{Enabled: true},
	}
	conn, err := start(ctx, &cfg, nil)
	require.NoError(t, err)
	defer conn.Close()

	_, err = rooms.InsertOne(ctx, bson.M{"_id": "r1", "name": "general"})
	require.NoError(t, err)
	_, err = msgs.InsertOne(ctx, bson.M{"_id": "m1", "msg": "hi"}) // no watcher — must not be forwarded
	require.NoError(t, err)

	nc, err := natsutil.Connect(cfg.NatsURL, "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	var subjects []string
	require.Eventually(t, func() bool {
		cons, cerr := js.CreateOrUpdateConsumer(ctx, "MIGRATION_OPLOG_sitecr", jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{"chat.migration.oplog.sitecr.>"},
		})
		if cerr != nil {
			return false
		}
		batch, berr := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for m := range batch.Messages() {
			assert.NoError(t, m.Ack())
			subjects = append(subjects, m.Subject())
		}
		return slices.Contains(subjects, "chat.migration.oplog.sitecr.rocketchat_room.insert")
	}, 40*time.Second, 500*time.Millisecond, "room insert must land on MIGRATION_OPLOG")

	for _, s := range subjects {
		assert.NotContains(t, s, "rocketchat_message",
			"collections-role deployment must never publish message subjects")
	}
}
