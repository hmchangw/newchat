//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// testSiteID derives a per-test site id from t.Name() so each test gets its
// own MIGRATION-OPLOG stream/consumer and never collides with siblings.
func testSiteID(t *testing.T) string {
	t.Helper()
	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name())) // hash.Hash.Write never returns an error.
	return fmt.Sprintf("it-%x", h.Sum32())
}

// startPipeline wires a real verifier + watcher against a fresh test-owned
// stream; cass may be nil, cfgOverride tweaks the fast-poll defaults.
func startPipeline(t *testing.T, mapping *Mapping, srcDB, tgtDB *mongo.Database, cass CassStore,
	cfgOverride func(*verifierConfig),
) (results *resultsStore, siteID string, publish func(subject string, ev model.OplogEvent)) {
	t.Helper()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	siteID = testSiteID(t)
	streamCfg := stream.MigrationOplog(siteID)
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer setupCancel()
	_, err = js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name:     streamCfg.Name,
		Subjects: streamCfg.Subjects,
	})
	require.NoError(t, err)

	cfg := verifierConfig{Poll: 200 * time.Millisecond, Timeout: 5 * time.Second, MaxChecks: 8, SamplePercent: 100}
	if cfgOverride != nil {
		cfgOverride(&cfg)
	}

	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	results = newResultsStore(200, 1000, nil)
	v := newVerifier(mapping, newMongoStore(srcDB), newMongoStore(tgtDB), cass, reg, results, cfg)

	w := newWatcher(js, streamCfg.Name, time.Time{}, v)
	runCtx, runCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- w.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		select {
		case err := <-runErr:
			assert.NoError(t, err, "watcher must exit cleanly on cancel")
		case <-time.After(5 * time.Second):
			t.Error("watcher did not exit after cancel")
		}
	})

	require.Eventually(t, w.Live, 5*time.Second, 20*time.Millisecond, "watcher must come up live before publishing")

	publish = func(subj string, ev model.OplogEvent) {
		data, merr := json.Marshal(ev)
		require.NoError(t, merr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		_, perr := js.Publish(pubCtx, subj, data)
		require.NoError(t, perr)
	}
	return results, siteID, publish
}

// oplogEvent builds a minimal CDC envelope (documentKey only), matching what
// oplog-connector sends for update/delete — the verifier re-reads state anyway.
func oplogEvent(siteID, collection, op, docID string) model.OplogEvent {
	nowMs := time.Now().UTC().UnixMilli()
	return model.OplogEvent{
		EventID:     idgen.GenerateID(),
		Op:          op,
		DB:          "rocketchat",
		Collection:  collection,
		DocumentKey: json.RawMessage(fmt.Sprintf(`{"_id":%q}`, docID)),
		ClusterTime: nowMs,
		SiteID:      siteID,
		Timestamp:   nowMs,
	}
}

// findResult returns the most recent row for docID, if any.
func findResult(results *resultsStore, docID string) (CheckResult, bool) {
	for _, r := range results.Recent() {
		if r.DocID == docID {
			return r, true
		}
	}
	return CheckResult{}, false
}

// awaitState polls until docID's row reaches want, failing the test on timeout.
func awaitState(t *testing.T, results *resultsStore, docID string, want CheckState, timeout time.Duration) CheckResult {
	t.Helper()
	var got CheckResult
	require.Eventually(t, func() bool {
		r, ok := findResult(results, docID)
		if !ok || r.State != want {
			return false
		}
		got = r
		return true
	}, timeout, 100*time.Millisecond, "expected doc %q to reach state %q", docID, want)
	return got
}

// mongoOnlyMapping is a single-source, single-mongo-target mapping comparing
// one "name" field, used by the plain match/mismatch/delayed scenarios.
func mongoOnlyMapping(collection, targetColl string) *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: collection,
		Ops: map[string]OpAction{
			"insert": OpVerify, "update": OpVerify, "replace": OpVerify, "delete": OpVerifyAbsent,
		},
		Targets: map[string]Target{
			"target": {Kind: "mongo", Collection: targetColl,
				Key: map[string]KeyFrom{"_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"name": {{Dest: "target.name"}}},
	}}}
}

// TestEndToEnd_Match publishes an insert whose source and target already
// agree — full pipeline: NATS publish -> watcher -> verifier -> results.
func TestEndToEnd_Match(t *testing.T) {
	srcDB := testutil.MongoDB(t, "cdcverify_src")
	tgtDB := testutil.MongoDB(t, "cdcverify_tgt")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srcDB.Collection("rocketchat_room").InsertOne(ctx, bson.M{"_id": "r1", "name": "general"})
	require.NoError(t, err)
	_, err = tgtDB.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r1", "name": "general"})
	require.NoError(t, err)

	results, siteID, publish := startPipeline(t, mongoOnlyMapping("rocketchat_room", "rooms"), srcDB, tgtDB, nil, nil)

	publish(subject.MigrationOplog(siteID, "rocketchat_room", "insert"),
		oplogEvent(siteID, "rocketchat_room", "insert", "r1"))

	got := awaitState(t, results, "r1", StateMatched, 15*time.Second)
	require.Len(t, got.Targets, 1)
	assert.True(t, got.Targets[0].Matched)
}

// TestEndToEnd_DelayedConvergence inserts the target doc only after a couple
// of polls, asserting convergence to matched (Attempts > 1), not early failure.
func TestEndToEnd_DelayedConvergence(t *testing.T) {
	srcDB := testutil.MongoDB(t, "cdcverify_src")
	tgtDB := testutil.MongoDB(t, "cdcverify_tgt")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srcDB.Collection("rocketchat_room").InsertOne(ctx, bson.M{"_id": "r2", "name": "general"})
	require.NoError(t, err)
	// Target doc intentionally not inserted yet.

	const poll = 200 * time.Millisecond
	results, siteID, publish := startPipeline(t, mongoOnlyMapping("rocketchat_room", "rooms"), srcDB, tgtDB, nil,
		func(cfg *verifierConfig) { cfg.Poll = poll; cfg.Timeout = 10 * time.Second })

	publish(subject.MigrationOplog(siteID, "rocketchat_room", "insert"),
		oplogEvent(siteID, "rocketchat_room", "insert", "r2"))

	// Let a couple of poll cycles observe the target missing before the doc
	// lands, so the test exercises convergence, not a same-attempt match.
	time.Sleep(2 * poll)
	_, err = tgtDB.Collection("rooms").InsertOne(context.Background(), bson.M{"_id": "r2", "name": "general"})
	require.NoError(t, err)

	got := awaitState(t, results, "r2", StateMatched, 15*time.Second)
	assert.Greater(t, got.Attempts, 1, "must have polled more than once before converging")
}

// TestEndToEnd_MismatchFailure asserts a genuinely differing target field
// fails at VerifyTimeout with a per-target diff carrying want/got.
func TestEndToEnd_MismatchFailure(t *testing.T) {
	srcDB := testutil.MongoDB(t, "cdcverify_src")
	tgtDB := testutil.MongoDB(t, "cdcverify_tgt")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srcDB.Collection("rocketchat_room").InsertOne(ctx, bson.M{"_id": "r3", "name": "general"})
	require.NoError(t, err)
	_, err = tgtDB.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r3", "name": "wrong-name"})
	require.NoError(t, err)

	results, siteID, publish := startPipeline(t, mongoOnlyMapping("rocketchat_room", "rooms"), srcDB, tgtDB, nil,
		func(cfg *verifierConfig) { cfg.Poll = 200 * time.Millisecond; cfg.Timeout = 5 * time.Second })

	publish(subject.MigrationOplog(siteID, "rocketchat_room", "insert"),
		oplogEvent(siteID, "rocketchat_room", "insert", "r3"))

	got := awaitState(t, results, "r3", StateFailed, 15*time.Second)
	require.Len(t, got.Targets, 1)
	tr := got.Targets[0]
	assert.False(t, tr.Matched)
	assert.Equal(t, "mismatch", tr.LastCause)
	require.Len(t, tr.Diffs, 1)
	assert.Equal(t, "general", tr.Diffs[0].Want)
	assert.Equal(t, "wrong-name", tr.Diffs[0].Got)
}

// cassTargetHandle bundles the CassStore the pipeline consumes with the raw
// session the test uses to seed rows directly.
type cassTargetHandle struct {
	store CassStore
	raw   *gocql.Session
}

// connectCassHandle opens a keyspace-scoped session and returns a store handle,
// closing the session on cleanup.
func connectCassHandle(t *testing.T, keyspace, host string) *cassTargetHandle {
	t.Helper()
	session, err := cassutil.Connect(cassutil.Config{Hosts: host, Keyspace: keyspace})
	require.NoError(t, err)
	t.Cleanup(func() { cassutil.Close(session) })
	return &cassTargetHandle{store: newCassStore(session), raw: session}
}

// setupCassandraTarget creates an isolated keyspace + messages_by_id table.
func setupCassandraTarget(t *testing.T) *cassTargetHandle {
	t.Helper()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "cdcverify")
	require.NoError(t, admin.Query(
		`CREATE TABLE IF NOT EXISTS `+keyspace+`.messages_by_id (message_id text PRIMARY KEY, body text, created_at bigint)`,
	).Exec())
	return connectCassHandle(t, keyspace, host)
}

// TestEndToEnd_CassandraTarget covers the cassandra lookup path end to end.
func TestEndToEnd_CassandraTarget(t *testing.T) {
	cass := setupCassandraTarget(t)
	require.NoError(t, cass.raw.Query(
		`INSERT INTO messages_by_id (message_id, body, created_at) VALUES (?, ?, ?)`,
		"m4", "hello", int64(1700000000000),
	).Exec())

	srcDB := testutil.MongoDB(t, "cdcverify_src")
	tgtDB := testutil.MongoDB(t, "cdcverify_tgt") // unused by this mapping (no mongo target)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srcDB.Collection("rocketchat_message").InsertOne(ctx, bson.M{"_id": "m4", "msg": "hello"})
	require.NoError(t, err)

	mapping := &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"byId": {Kind: "cassandra", Table: "messages_by_id",
				Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"msg": {{Dest: "byId.body"}}},
	}}}

	results, siteID, publish := startPipeline(t, mapping, srcDB, tgtDB, cass.store, nil)

	publish(subject.MigrationOplog(siteID, "rocketchat_message", "insert"),
		oplogEvent(siteID, "rocketchat_message", "insert", "m4"))

	got := awaitState(t, results, "m4", StateMatched, 15*time.Second)
	require.Len(t, got.Targets, 1)
	assert.True(t, got.Targets[0].Matched)
}

// TestEndToEnd_VerifyAbsentDelete publishes a delete for a doc whose target
// row was never present, asserting it is verified matched by absence.
func TestEndToEnd_VerifyAbsentDelete(t *testing.T) {
	srcDB := testutil.MongoDB(t, "cdcverify_src")
	tgtDB := testutil.MongoDB(t, "cdcverify_tgt")
	// Target row intentionally absent — a delete verifies the dest is gone;
	// no source read happens for verify-absent.

	results, siteID, publish := startPipeline(t, mongoOnlyMapping("rocketchat_room", "rooms"), srcDB, tgtDB, nil, nil)

	publish(subject.MigrationOplog(siteID, "rocketchat_room", "delete"),
		oplogEvent(siteID, "rocketchat_room", "delete", "r5"))

	got := awaitState(t, results, "r5", StateMatched, 15*time.Second)
	require.Len(t, got.Targets, 1)
	assert.True(t, got.Targets[0].Matched)
}

// resolverMapping exercises a resolver hop: the target key is built from
// "@user.username", resolved from a "users" lookup keyed by "u._id".
func resolverMapping() *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_subscription",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Resolvers: map[string]Resolver{
			"user": {DB: "source", Collection: "users",
				Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}, Fields: []string{"username"}},
		},
		Targets: map[string]Target{
			"byUser": {Kind: "mongo", Collection: "userdocs",
				Key: map[string]KeyFrom{"_id": {From: []string{"@user.username"}}}},
		},
	}}}
}

// TestEndToEnd_ResolverHop covers both the resolved-match path and the
// resolver-miss failure path.
func TestEndToEnd_ResolverHop(t *testing.T) {
	t.Run("match", func(t *testing.T) {
		srcDB := testutil.MongoDB(t, "cdcverify_src")
		tgtDB := testutil.MongoDB(t, "cdcverify_tgt")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := srcDB.Collection("rocketchat_subscription").InsertOne(ctx,
			bson.M{"_id": "s1", "u": bson.M{"_id": "u1"}})
		require.NoError(t, err)
		_, err = srcDB.Collection("users").InsertOne(ctx, bson.M{"_id": "u1", "username": "alice"})
		require.NoError(t, err)
		_, err = tgtDB.Collection("userdocs").InsertOne(ctx, bson.M{"_id": "alice"})
		require.NoError(t, err)

		results, siteID, publish := startPipeline(t, resolverMapping(), srcDB, tgtDB, nil, nil)
		publish(subject.MigrationOplog(siteID, "rocketchat_subscription", "insert"),
			oplogEvent(siteID, "rocketchat_subscription", "insert", "s1"))

		got := awaitState(t, results, "s1", StateMatched, 15*time.Second)
		require.Len(t, got.Targets, 1)
		assert.True(t, got.Targets[0].Matched)
	})

	t.Run("missing user fails with resolver-miss", func(t *testing.T) {
		srcDB := testutil.MongoDB(t, "cdcverify_src")
		tgtDB := testutil.MongoDB(t, "cdcverify_tgt")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := srcDB.Collection("rocketchat_subscription").InsertOne(ctx,
			bson.M{"_id": "s2", "u": bson.M{"_id": "u-missing"}})
		require.NoError(t, err)
		// "users" collection intentionally has no matching doc.

		results, siteID, publish := startPipeline(t, resolverMapping(), srcDB, tgtDB, nil,
			func(cfg *verifierConfig) { cfg.Poll = 200 * time.Millisecond; cfg.Timeout = 3 * time.Second })
		publish(subject.MigrationOplog(siteID, "rocketchat_subscription", "insert"),
			oplogEvent(siteID, "rocketchat_subscription", "insert", "s2"))

		got := awaitState(t, results, "s2", StateFailed, 15*time.Second)
		require.Len(t, got.Targets, 1)
		assert.Equal(t, "resolver-miss: user", got.Targets[0].LastCause)
	})
}

// setupCassandraWithReactions builds messages_by_id with a struct-keyed
// `reactions map<frozen<udt>,...>` column that gocql's MapScan can't represent.
func setupCassandraWithReactions(t *testing.T) *cassTargetHandle {
	t.Helper()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "cdcinspect")
	require.NoError(t, admin.Query(
		`CREATE TYPE IF NOT EXISTS `+keyspace+`.reaction_key (emoji text)`).Exec())
	require.NoError(t, admin.Query(
		`CREATE TABLE IF NOT EXISTS `+keyspace+`.messages_by_id (
			message_id text PRIMARY KEY, body text, created_at bigint,
			reactions map<frozen<reaction_key>, text>)`).Exec())
	return connectCassHandle(t, keyspace, host)
}

// TestInspect_CassandraStructKeyedMap reproduces the inspect crash: a whole-row
// read of a struct-keyed map column panicked MapScan; it must skip, not crash.
func TestInspect_CassandraStructKeyedMap(t *testing.T) {
	cass := setupCassandraWithReactions(t)
	require.NoError(t, cass.raw.Query(
		`INSERT INTO messages_by_id (message_id, body, created_at) VALUES (?, ?, ?)`,
		"m9", "hi", int64(1700000000000)).Exec())

	srcDB := testutil.MongoDB(t, "cdcinspect_src")
	tgtDB := testutil.MongoDB(t, "cdcinspect_tgt")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srcDB.Collection("rocketchat_message").InsertOne(ctx, bson.M{"_id": "m9", "msg": "hi"})
	require.NoError(t, err)

	mapping := &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"byId": {Kind: "cassandra", Table: "messages_by_id",
				Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"msg": {{Dest: "byId.body"}}},
	}}}
	v := newVerifier(mapping, newMongoStore(srcDB), newMongoStore(tgtDB), cass.store,
		newTransformRegistry(msgbucket.New(72*time.Hour)), newResultsStore(10, 10, nil),
		verifierConfig{Poll: time.Second, Timeout: time.Minute, MaxChecks: 4, SamplePercent: 100})

	got, err := v.Inspect(ctx, "rocketchat_message", "m9")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"_id": "m9", "msg": "hi"}, got.Source)
	require.Len(t, got.Targets, 1)
	assert.Empty(t, got.Targets[0].Error, "struct-keyed map column must be skipped, not error")
	assert.Equal(t, "hi", got.Targets[0].Doc["body"])
	assert.NotContains(t, got.Targets[0].Doc, "reactions", "unrepresentable column is omitted")
}
