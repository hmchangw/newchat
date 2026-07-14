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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// --- Shared helpers for inbox-based collection integration tests ---

// createInboxStream creates the INBOX_{siteID} stream using pkg/stream.Inbox
// as the canonical baseline (name + local/aggregate subject patterns), with
// no cross-site Sources. Cross-site Sources are a production deployment
// concern owned by inbox-worker; tests simulate federated events by
// publishing directly to the aggregate subject instead.
func createInboxStream(t *testing.T, ctx context.Context, js jetstream.JetStream, siteID string) {
	t.Helper()
	cfg := stream.Inbox(siteID)
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	})
	require.NoError(t, err, "create INBOX stream for %s", siteID)
}

// buildInboxMemberEvent constructs an InboxMemberEvent payload for tests.
// `historySharedSince` is nil for unrestricted; non-nil with a positive
// value marks the bulk as restricted. See parseMemberEvent for how each
// collection consumes the flag. `joinedAt` is only meaningful on add
// events; pass 0 for removes.
func buildInboxMemberEvent(
	roomID, roomName, siteID string,
	accounts []string,
	historySharedSince *int64,
	joinedAt int64,
	timestamp int64,
) model.InboxMemberEvent {
	return model.InboxMemberEvent{
		RoomID:             roomID,
		RoomName:           roomName,
		RoomType:           model.RoomTypeChannel,
		SiteID:             siteID,
		Accounts:           accounts,
		HistorySharedSince: historySharedSince,
		JoinedAt:           joinedAt,
		Timestamp:          timestamp,
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}

// publishInboxMemberEvent wraps an InboxMemberEvent inside an InboxEvent
// with the given Type and publishes it to `subj`. Caller picks `subj`
// (local vs. aggregate, added vs. removed) via pkg/subject builders.
func publishInboxMemberEvent(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	subj, eventType string,
	payload model.InboxMemberEvent,
) {
	t.Helper()
	payloadData, err := json.Marshal(payload)
	require.NoError(t, err)

	evt := model.InboxEvent{
		Type:       eventType,
		SiteID:     payload.SiteID,
		DestSiteID: payload.SiteID,
		Payload:    payloadData,
		Timestamp:  payload.Timestamp,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	_, err = js.Publish(ctx, subj, data)
	require.NoError(t, err, "publish to %s", subj)
}

// drainConsumer fetches exactly `expected` JetStream messages from the
// consumer and feeds them through the handler (Add + Flush). Fails if fewer
// messages are delivered within the fetch timeout.
func drainConsumer(
	t *testing.T,
	ctx context.Context,
	cons jetstream.Consumer,
	handler *Handler,
	expected int,
) {
	t.Helper()
	if expected == 0 {
		handler.Flush(ctx)
		return
	}

	received := 0
	for attempts := 0; attempts < 5 && received < expected; attempts++ {
		batch, err := cons.Fetch(expected-received, jetstream.FetchMaxWait(5*time.Second))
		require.NoError(t, err)
		for msg := range batch.Messages() {
			handler.Add(msg)
			received++
		}
		// Surface any mid-batch error (consumer deleted, leader change,
		// transient connection). Without this, batch.Messages() just closes
		// silently and only the outer Equal would fail — losing the root cause.
		require.NoError(t, batch.Error(), "batch error after draining (attempt %d, received %d of %d)", attempts, received, expected)
	}
	require.Equal(t, expected, received, "drained %d of %d expected messages", received, expected)
	handler.Flush(ctx)
}

// toStringSlice converts a JSON-decoded array (`[]any`) to `[]string`.
// Fails the test if any element is not a string.
func toStringSlice(t *testing.T, v any) []string {
	t.Helper()
	if v == nil {
		return nil
	}
	slice, ok := v.([]any)
	require.True(t, ok, "expected []any, got %T", v)
	out := make([]string, 0, len(slice))
	for _, item := range slice {
		s, ok := item.(string)
		require.True(t, ok, "expected string element, got %T", item)
		out = append(out, s)
	}
	return out
}

// --- Spotlight integration test ---

func TestSpotlightSync_Integration(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	siteID := "site-spot"
	indexName := "spotlight-singular-v1"

	// --- ES template + index ---
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newSpotlightCollection(indexName, true)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(coll.TemplateBody())))
	preCreateIndex(t, esURL, indexName)
	waitForClusterGreen(t, esURL, 120*time.Second)

	// --- NATS INBOX stream + consumer ---
	createInboxStream(t, ctx, js, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Inbox(siteID).Name, jetstream.ConsumerConfig{
		Durable:        "spotlight-sync-inttest",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: coll.FilterSubjects(siteID),
	})
	require.NoError(t, err, "create spotlight consumer")

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	const joinedAt int64 = 1744286400000 // 2026-04-10 12:00 UTC

	// --- Publish events covering local + federated + remove ---

	// Local member_added: alice joins engineering
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-eng", "engineering", siteID, []string{"alice"}, nil, joinedAt, 1000),
	)

	// Local member_added: alice joins platform
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-platform", "platform", siteID, []string{"alice"}, nil, joinedAt, 1100),
	)

	// Federated (aggregate) member_added: bob joins engineering via a cross-site event
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxExternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-eng", "engineering", siteID, []string{"bob"}, nil, joinedAt, 1200),
	)

	// Federated (aggregate) member_removed: alice leaves platform
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxExternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved,
		buildInboxMemberEvent("r-platform", "platform", siteID, []string{"alice"}, nil, 0, 1300),
	)

	drainConsumer(t, ctx, cons, handler, 4)
	refreshIndex(t, esURL, indexName)

	// --- Verify ---

	t.Run("two subscriptions remain after one removal", func(t *testing.T) {
		// Added 3 (alice-eng, alice-platform, bob-eng), removed 1 (alice-platform)
		assert.Equal(t, 2, countDocs(t, esURL, indexName))
	})

	t.Run("alice engineering doc shape", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice_r-eng")
		require.NotNil(t, doc, "alice_r-eng should be indexed")
		assert.Equal(t, "alice", doc["userAccount"])
		assert.Equal(t, "r-eng", doc["roomId"])
		assert.Equal(t, "engineering", doc["roomName"])
		assert.Equal(t, "channel", doc["roomType"])
		assert.Equal(t, siteID, doc["siteId"])
	})

	t.Run("federated bob doc was indexed", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "bob_r-eng")
		require.NotNil(t, doc, "bob's federated subscription should be indexed via aggregate filter")
		assert.Equal(t, "bob", doc["userAccount"])
		assert.Equal(t, "r-eng", doc["roomId"])
	})

	t.Run("removed alice-platform doc is gone", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice_r-platform")
		assert.Nil(t, doc, "removed subscription should not exist in the index")
	})
}

// TestSpotlightSync_BulkInvite verifies the fan-out path end-to-end: a single
// JetStream message carrying N accounts must produce N spotlight docs in one
// ES bulk request.
func TestSpotlightSync_BulkInvite(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	siteID := "site-spot-bulk"
	indexName := "spotlight-bulk-v1"

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newSpotlightCollection(indexName, true)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(coll.TemplateBody())))
	preCreateIndex(t, esURL, indexName)
	waitForClusterGreen(t, esURL, 120*time.Second)

	createInboxStream(t, ctx, js, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Inbox(siteID).Name, jetstream.ConsumerConfig{
		Durable:        "spotlight-sync-bulk-inttest",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: coll.FilterSubjects(siteID),
	})
	require.NoError(t, err, "create spotlight consumer")

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	const joinedAt int64 = 1744286400000

	// One bulk-invite event adds 3 users to r-platform at once.
	payload := buildInboxMemberEvent("r-platform", "platform", siteID,
		[]string{"dave", "erin", "frank"}, nil, joinedAt, 5000)
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded, payload)

	// Only ONE JetStream message is drained, but it produces THREE ES index
	// actions — handler.ActionCount() > MessageCount() is the whole point
	// of the fan-out path.
	drainConsumer(t, ctx, cons, handler, 1)
	refreshIndex(t, esURL, indexName)

	t.Run("all three accounts landed in the index", func(t *testing.T) {
		assert.Equal(t, 3, countDocs(t, esURL, indexName),
			"1 message × 3 accounts = 3 spotlight docs")
	})

	t.Run("each account has the correct doc shape", func(t *testing.T) {
		for _, account := range []string{"dave", "erin", "frank"} {
			docID := account + "_r-platform"
			doc := getDoc(t, esURL, indexName, docID)
			require.NotNil(t, doc, "%s should be indexed", docID)
			assert.Equal(t, account, doc["userAccount"])
			assert.Equal(t, "r-platform", doc["roomId"])
			assert.Equal(t, "platform", doc["roomName"])
		}
	})

	t.Run("bulk remove evicts all three docs", func(t *testing.T) {
		// Same 3 accounts, now removed in one event.
		remove := buildInboxMemberEvent("r-platform", "platform", siteID,
			[]string{"dave", "erin", "frank"}, nil, 0, 6000)
		publishInboxMemberEvent(t, ctx, js,
			subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved, remove)
		drainConsumer(t, ctx, cons, handler, 1)
		refreshIndex(t, esURL, indexName)

		assert.Equal(t, 0, countDocs(t, esURL, indexName),
			"1 message × 3 account deletes = 0 docs remaining")
	})
}

// --- User-room integration test ---

func TestUserRoomSync_Integration(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	siteID := "site-ur"
	indexName := "user-room-site-ur"

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newUserRoomCollection(indexName)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(userRoomTemplateBody(indexName))))
	registerStoredScripts(t, ctx, engine, coll)
	preCreateIndex(t, esURL, indexName)
	waitForClusterGreen(t, esURL, 120*time.Second)

	createInboxStream(t, ctx, js, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Inbox(siteID).Name, jetstream.ConsumerConfig{
		Durable:        "user-room-sync-inttest",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: coll.FilterSubjects(siteID),
	})
	require.NoError(t, err, "create user-room consumer")

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	const joinedAt int64 = 1744286400000

	// --- Publish ---

	// alice joins 3 rooms via local events
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r1", "general", siteID, []string{"alice"}, nil, joinedAt, 1000))
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r2", "random", siteID, []string{"alice"}, nil, joinedAt, 1100))
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r3", "eng", siteID, []string{"alice"}, nil, joinedAt, 1200))

	// bob joins r1 via a federated (aggregate) event
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxExternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r1", "general", siteID, []string{"bob"}, nil, joinedAt, 1300))

	// alice leaves r2 via a local event
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved,
		buildInboxMemberEvent("r2", "random", siteID, []string{"alice"}, nil, 0, 1400))

	// alice joins a restricted room. user-room now stores it in
	// `restrictedRooms{}` instead of skipping.
	const restrictedHSS int64 = 1743984000000
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-restricted", "archives", siteID, []string{"alice"}, int64Ptr(restrictedHSS), joinedAt, 1500))

	drainConsumer(t, ctx, cons, handler, 6)
	refreshIndex(t, esURL, indexName)

	// --- Verify ---

	t.Run("alice rooms reflect unrestricted adds and one remove", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice")
		require.NotNil(t, doc, "alice user-room doc should exist")
		rooms := toStringSlice(t, doc["rooms"])
		assert.ElementsMatch(t, []string{"r1", "r3"}, rooms,
			"alice rooms[] should hold unrestricted adds minus the remove")
	})

	t.Run("bob created via federated event", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "bob")
		require.NotNil(t, doc, "bob should be upserted from aggregate.member_added")
		rooms := toStringSlice(t, doc["rooms"])
		assert.ElementsMatch(t, []string{"r1"}, rooms)
	})

	t.Run("roomTimestamps retained after remove and for restricted", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice")
		require.NotNil(t, doc)
		rts, ok := doc["roomTimestamps"].(map[string]any)
		require.True(t, ok, "roomTimestamps should be a flattened map")

		// r1, r2, r3 all get their last-seen timestamps stored. r2's
		// entry is KEPT after the remove (preserves LWW monotonicity so a
		// late-arriving stale add can't re-insert r2).
		assert.Equal(t, float64(1000), rts["r1"])
		assert.Equal(t, float64(1400), rts["r2"],
			"r2 timestamp should be bumped to the remove's event timestamp, not deleted")
		assert.Equal(t, float64(1200), rts["r3"])

		// Restricted-room adds also stamp roomTimestamps so LWW guards both
		// paths uniformly.
		assert.Equal(t, float64(1500), rts["r-restricted"])
	})

	t.Run("restricted room lands in restrictedRooms map", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice")
		require.NotNil(t, doc)
		rooms := toStringSlice(t, doc["rooms"])
		assert.NotContains(t, rooms, "r-restricted",
			"restricted rooms must NOT appear in rooms[]")
		restricted, ok := doc["restrictedRooms"].(map[string]any)
		require.True(t, ok, "restrictedRooms should be a flattened map")
		assert.EqualValues(t, restrictedHSS, restricted["r-restricted"],
			"restrictedRooms[r-restricted] should equal the event HSS")
	})

	t.Run("createdAt and updatedAt stamped from upsert", func(t *testing.T) {
		doc := getDoc(t, esURL, indexName, "alice")
		require.NotNil(t, doc)
		assert.NotEmpty(t, doc["createdAt"], "upsert should seed createdAt")
		assert.NotEmpty(t, doc["updatedAt"], "add path should stamp updatedAt")
	})
}

// TestUserRoomSync_BulkInvite verifies the fan-out path for user-room: a
// single JetStream message with N accounts produces N distinct user-room
// updates (different DocIDs since each account targets a different user).
// Also covers the all-restricted event case where the whole bulk is skipped.
func TestUserRoomSync_BulkInvite(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	siteID := "site-ur-bulk"
	indexName := "user-room-site-ur-bulk"

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newUserRoomCollection(indexName)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(userRoomTemplateBody(indexName))))
	registerStoredScripts(t, ctx, engine, coll)
	preCreateIndex(t, esURL, indexName)
	waitForClusterGreen(t, esURL, 120*time.Second)

	createInboxStream(t, ctx, js, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Inbox(siteID).Name, jetstream.ConsumerConfig{
		Durable:        "user-room-sync-bulk-inttest",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: coll.FilterSubjects(siteID),
	})
	require.NoError(t, err, "create user-room consumer")

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	const joinedAt int64 = 1744286400000

	// Unrestricted bulk: 3 users to r-platform in one event → 3 user-room docs.
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-platform", "platform", siteID,
			[]string{"dave", "erin", "frank"}, nil, joinedAt, 5000))

	// Restricted bulk: 3 users to r-archives with non-nil HistorySharedSince.
	// User-room now routes these into `restrictedRooms{}` instead of skipping.
	const archivesHSS int64 = 1743984000000
	publishInboxMemberEvent(t, ctx, js,
		subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded,
		buildInboxMemberEvent("r-archives", "archives", siteID,
			[]string{"heidi", "ivan", "judy"}, int64Ptr(archivesHSS), joinedAt, 5100))

	drainConsumer(t, ctx, cons, handler, 2)
	refreshIndex(t, esURL, indexName)

	t.Run("unrestricted bulk upserts all three users", func(t *testing.T) {
		for _, account := range []string{"dave", "erin", "frank"} {
			doc := getDoc(t, esURL, indexName, account)
			require.NotNil(t, doc, "%s should be upserted", account)
			rooms := toStringSlice(t, doc["rooms"])
			assert.ElementsMatch(t, []string{"r-platform"}, rooms)
			assert.Empty(t, doc["restrictedRooms"],
				"%s must not have any restricted rooms", account)
		}
	})

	t.Run("restricted bulk routes all three users into restrictedRooms", func(t *testing.T) {
		for _, account := range []string{"heidi", "ivan", "judy"} {
			doc := getDoc(t, esURL, indexName, account)
			require.NotNil(t, doc, "restricted %s should still be upserted", account)
			assert.Empty(t, toStringSlice(t, doc["rooms"]),
				"%s rooms[] must not contain the restricted room", account)
			restricted, ok := doc["restrictedRooms"].(map[string]any)
			require.True(t, ok, "%s must have restrictedRooms map", account)
			assert.EqualValues(t, archivesHSS, restricted["r-archives"],
				"%s restrictedRooms[r-archives] should equal event HSS", account)
		}
	})

	t.Run("bulk remove evicts rooms from unrestricted users", func(t *testing.T) {
		publishInboxMemberEvent(t, ctx, js,
			subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved,
			buildInboxMemberEvent("r-platform", "platform", siteID,
				[]string{"dave", "erin", "frank"}, nil, 0, 6000))
		drainConsumer(t, ctx, cons, handler, 1)
		refreshIndex(t, esURL, indexName)

		for _, account := range []string{"dave", "erin", "frank"} {
			doc := getDoc(t, esURL, indexName, account)
			require.NotNil(t, doc, "%s user doc should still exist (ghost)", account)
			assert.Empty(t, toStringSlice(t, doc["rooms"]),
				"%s rooms should be empty after bulk remove", account)
		}
	})

	t.Run("bulk remove evicts rooms from restricted users", func(t *testing.T) {
		publishInboxMemberEvent(t, ctx, js,
			subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved,
			buildInboxMemberEvent("r-archives", "archives", siteID,
				[]string{"heidi", "ivan", "judy"}, int64Ptr(archivesHSS), 0, 6100))
		drainConsumer(t, ctx, cons, handler, 1)
		refreshIndex(t, esURL, indexName)

		for _, account := range []string{"heidi", "ivan", "judy"} {
			doc := getDoc(t, esURL, indexName, account)
			require.NotNil(t, doc, "%s user doc should still exist (ghost)", account)
			restricted, _ := doc["restrictedRooms"].(map[string]any)
			_, stillHas := restricted["r-archives"]
			assert.False(t, stillHas,
				"%s restrictedRooms[r-archives] should be evicted after remove", account)
		}
	})
}

// --- User-room LWW guard integration test ---

// TestUserRoomSync_LWWGuard drives a single user doc through a sequence of
// in-order and out-of-order events to prove the per-room timestamp guard
// converges on highest-event-timestamp-wins state regardless of physical
// arrival order.
//
// Implemented as one linear test body (not split into t.Run subtests)
// because the scenario is inherently stateful — each step depends on the
// prior ES state.
func TestUserRoomSync_LWWGuard(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	siteID := "site-lww"
	indexName := "user-room-site-lww"

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newUserRoomCollection(indexName)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(userRoomTemplateBody(indexName))))
	registerStoredScripts(t, ctx, engine, coll)
	preCreateIndex(t, esURL, indexName)
	waitForClusterGreen(t, esURL, 120*time.Second)

	createInboxStream(t, ctx, js, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Inbox(siteID).Name, jetstream.ConsumerConfig{
		Durable:        "user-room-sync-lww-inttest",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: coll.FilterSubjects(siteID),
	})
	require.NoError(t, err)

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	const joinedAt int64 = 1744286400000

	publish := func(subj, eventType, roomID string, ts int64) {
		var j int64
		if eventType == model.InboxMemberAdded {
			j = joinedAt
		}
		publishInboxMemberEvent(t, ctx, js, subj, eventType,
			buildInboxMemberEvent(roomID, "room "+roomID, siteID, []string{"charlie"}, nil, j, ts))
	}

	getCharlieState := func() ([]string, map[string]any) {
		refreshIndex(t, esURL, indexName)
		doc := getDoc(t, esURL, indexName, "charlie")
		require.NotNil(t, doc, "charlie user-room doc should exist")
		rooms := toStringSlice(t, doc["rooms"])
		rts, _ := doc["roomTimestamps"].(map[string]any)
		return rooms, rts
	}

	// Step 1: initial add at ts=2000 creates the doc via upsert.
	publish(subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded, "rA", 2000)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts := getCharlieState()
	assert.Contains(t, rooms, "rA", "step 1: initial add should put rA in rooms")
	assert.Equal(t, float64(2000), rts["rA"], "step 1: stored ts should be 2000")

	// Step 2: stale add at ts=1000 should be a no-op via ctx.op='none'.
	publish(subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded, "rA", 1000)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts = getCharlieState()
	assert.Contains(t, rooms, "rA", "step 2: rA should still be in rooms after stale add")
	assert.Equal(t, float64(2000), rts["rA"],
		"step 2: stale add must not overwrite a newer stored timestamp")

	// Step 3: stale remove at ts=1500 should also be a no-op.
	publish(subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved, "rA", 1500)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts = getCharlieState()
	assert.Contains(t, rooms, "rA", "step 3: rA should survive stale remove")
	assert.Equal(t, float64(2000), rts["rA"], "step 3: stored timestamp unchanged after stale remove")

	// Step 4: newer remove at ts=3000 evicts the room.
	publish(subject.InboxInternal(siteID, model.InboxMemberRemoved), model.InboxMemberRemoved, "rA", 3000)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts = getCharlieState()
	assert.NotContains(t, rooms, "rA", "step 4: newer remove should evict rA")
	assert.Equal(t, float64(3000), rts["rA"],
		"step 4: remove must bump stored timestamp to the remove's ts")

	// Step 5: re-add with newer ts=4000 puts the room back.
	publish(subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded, "rA", 4000)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts = getCharlieState()
	assert.Contains(t, rooms, "rA", "step 5: re-add with newer ts should restore rA")
	assert.Equal(t, float64(4000), rts["rA"], "step 5: stored ts should bump to 4000")

	// Step 6: stale add at ts=2500 after the re-add is still a no-op.
	publish(subject.InboxInternal(siteID, model.InboxMemberAdded), model.InboxMemberAdded, "rA", 2500)
	drainConsumer(t, ctx, cons, handler, 1)
	rooms, rts = getCharlieState()
	assert.Contains(t, rooms, "rA", "step 6: rA should still be present")
	assert.Equal(t, float64(4000), rts["rA"], "step 6: stored ts unchanged after stale add")
	rACount := 0
	for _, r := range rooms {
		if r == "rA" {
			rACount++
		}
	}
	assert.Equal(t, 1, rACount, "step 6: rA should not be duplicated by stale add")
}
