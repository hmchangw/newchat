//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// Package-level NATS connection + JetStream client. Connected once in
// TestMain and shared by every test. The underlying NATS and ES
// containers come from pkg/testutil.
var (
	testJS         jetstream.JetStream
	testNATSCon    *nats.Conn
	testNATSConErr error
	testNATSOnce   sync.Once
)

// TestMain pre-warms shared containers in parallel; fails fast on error.
// Custom wrap (not testutil.RunTestsWithPrewarm) so we can close the
// lazy-init JetStream conn between m.Run and TerminateAll.
func TestMain(m *testing.M) {
	if err := testutil.PrewarmFailFast(testutil.EnsureElasticsearch, testutil.EnsureNATS); err != nil {
		fmt.Fprintf(os.Stderr, "prewarm shared containers: %v\n", err)
		testutil.TerminateAll()
		os.Exit(1)
	}
	code := m.Run()
	if testNATSCon != nil {
		testNATSCon.Close()
	}
	testutil.TerminateAll()
	os.Exit(code)
}

// setupElasticsearch returns the shared ES URL. Tests must use unique index
// names to stay isolated — the existing suite does.
func setupElasticsearch(t *testing.T) string {
	t.Helper()
	return testutil.Elasticsearch(t)
}

// setupNATSJetStream returns the shared (JetStream, Conn). Tests must use
// unique stream names to stay isolated — the existing suite does.
func setupNATSJetStream(t *testing.T) (jetstream.JetStream, *nats.Conn) {
	t.Helper()
	testNATSOnce.Do(func() {
		nc, err := nats.Connect(testutil.NATS(t))
		if err != nil {
			testNATSConErr = fmt.Errorf("connect nats: %w", err)
			return
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			testNATSConErr = fmt.Errorf("init jetstream: %w", err)
			return
		}
		testNATSCon = nc
		testJS = js
	})
	if testNATSConErr != nil {
		t.Fatalf("nats jetstream setup: %v", testNATSConErr)
	}
	return testJS, testNATSCon
}

// loadTestEvents reads MessageEvent fixtures from testdata/events.json.
func loadTestEvents(t *testing.T) []model.MessageEvent {
	t.Helper()
	data, err := os.ReadFile("testdata/events.json")
	require.NoError(t, err, "read testdata/events.json")

	var events []model.MessageEvent
	require.NoError(t, json.Unmarshal(data, &events), "unmarshal events")
	return events
}

// esURLFor returns a URL rooted at the parsed ES base, with `segments`
// appended via url.URL.JoinPath. Parsing locks the scheme + host before any
// path data is appended, so test inputs (index names, doc IDs, query
// patterns) cannot rewrite the request target — closes the
// CodeQL go/request-forgery / gosec G107 sink that flagged the previous
// fmt.Sprintf-built URLs.
func esURLFor(t *testing.T, esURL string, segments ...string) *url.URL {
	t.Helper()
	u, err := url.Parse(esURL)
	require.NoError(t, err, "parse es base url")
	return u.JoinPath(segments...)
}

// refreshIndex forces ES to make all indexed docs searchable.
func refreshIndex(t *testing.T, esURL, pattern string) {
	t.Helper()
	u := esURLFor(t, esURL, pattern, "_refresh")
	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "refresh index %s: %s", pattern, body)
}

// countDocs returns the number of documents matching the index pattern.
func countDocs(t *testing.T, esURL, pattern string) int {
	t.Helper()
	u := esURLFor(t, esURL, pattern, "_count")
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// 404 means no indices exist yet
	if resp.StatusCode == http.StatusNotFound {
		return 0
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result struct {
		Count int `json:"count"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	return result.Count
}

// waitForClusterGreen polls ES cluster health until status is green or timeout.
func waitForClusterGreen(t *testing.T, esURL string, timeout time.Duration) {
	t.Helper()
	u := esURLFor(t, esURL, "_cluster", "health")
	u.RawQuery = "wait_for_status=green&timeout=5s"
	healthURL := u.String()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, healthURL, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			var health struct {
				Status string `json:"status"`
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if json.Unmarshal(body, &health) == nil && health.Status == "green" {
				t.Logf("ES cluster health: %s", health.Status)
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("ES cluster did not reach green status within timeout")
}

// preCreateIndex creates an ES index so shard allocation completes early.
func preCreateIndex(t *testing.T, esURL, index string) {
	t.Helper()
	u := esURLFor(t, esURL, index)
	req, err := http.NewRequest(http.MethodPut, u.String(), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "pre-create index %s: %s", index, body)
}

// overrideIndexSettings replaces `template.settings.index` on a marshaled ES
// index template with single-node-friendly values (1 shard, 0 replicas, 1s
// refresh) while leaving analysis + mappings intact. Shared by message,
// spotlight, and user-room integration tests so a single ES test container
// can host all three.
func overrideIndexSettings(body json.RawMessage) json.RawMessage {
	var tmpl map[string]any
	_ = json.Unmarshal(body, &tmpl)
	template := tmpl["template"].(map[string]any)
	settings := template["settings"].(map[string]any)
	settings["index"] = map[string]any{
		"number_of_shards":   1,
		"number_of_replicas": 0,
		"refresh_interval":   "1s",
	}
	data, _ := json.Marshal(tmpl)
	return data
}

// getDoc retrieves a single document from ES by ID. Returns nil if not found.
func getDoc(t *testing.T, esURL, index, docID string) map[string]any {
	t.Helper()
	u := esURLFor(t, esURL, index, "_doc", docID)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result struct {
		Source map[string]any `json:"_source"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	return result.Source
}

func TestSearchSyncIntegration(t *testing.T) {
	esURL := setupElasticsearch(t)
	js, _ := setupNATSJetStream(t)
	ctx := context.Background()

	// --- Setup search engine + template ---
	prefix := "msgs-inttest-v1"
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err, "create search engine")

	// Wait for cluster to be green before creating indices.
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newMessageCollection(prefix)
	err = engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(messageTemplateBody(prefix)))
	require.NoError(t, err, "upsert template")

	// Pre-create indices so shard allocation completes before bulk indexing.
	preCreateIndex(t, esURL, prefix+"-2026-01")
	preCreateIndex(t, esURL, prefix+"-2026-02")
	waitForClusterGreen(t, esURL, 120*time.Second)

	// --- Setup NATS stream + consumer ---
	siteID := "site-test"
	canonicalCfg := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     canonicalCfg.Name,
		Subjects: canonicalCfg.Subjects,
	})
	require.NoError(t, err, "create stream")

	// --- Load and publish test events ---
	events := loadTestEvents(t)
	for _, evt := range events {
		data, marshalErr := json.Marshal(evt)
		require.NoError(t, marshalErr)

		// Route to correct canonical subject based on event type
		var subj string
		switch evt.Event {
		case model.EventCreated:
			subj = subject.MsgCanonicalCreated(siteID)
		case model.EventUpdated:
			subj = subject.MsgCanonicalUpdated(siteID)
		case model.EventDeleted:
			subj = subject.MsgCanonicalDeleted(siteID)
		default:
			t.Fatalf("unsupported event type in fixture: %q", evt.Event)
		}
		_, pubErr := js.Publish(ctx, subj, data)
		require.NoError(t, pubErr, "publish event %s", evt.Message.ID)
	}

	// --- Create consumer and process all messages ---
	cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, jetstream.ConsumerConfig{
		Durable:   "search-sync-worker-test",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err, "create consumer")

	handler := NewHandler(&engineAdapter{engine: engine}, coll, 100)

	// Fetch all published messages
	batch, err := cons.Fetch(len(events), jetstream.FetchMaxWait(10*time.Second))
	require.NoError(t, err, "fetch messages")

	for msg := range batch.Messages() {
		handler.Add(msg)
	}

	// Flush to ES
	handler.Flush(ctx)

	// --- Verify results in Elasticsearch ---
	refreshIndex(t, esURL, prefix+"-*")

	// Total doc count: 5 created - 1 deleted = 4
	t.Run("total doc count", func(t *testing.T) {
		total := countDocs(t, esURL, prefix+"-*")
		assert.Equal(t, 4, total, "expected 4 docs total (5 created, 1 update replacing, 1 delete)")
	})

	// January index: msg-001 (updated), msg-003 → 2 docs
	t.Run("january index count", func(t *testing.T) {
		janCount := countDocs(t, esURL, prefix+"-2026-01")
		assert.Equal(t, 2, janCount, "expected 2 docs in 2026-01 index")
	})

	// February index: msg-004, msg-005 → 2 docs
	t.Run("february index count", func(t *testing.T) {
		febCount := countDocs(t, esURL, prefix+"-2026-02")
		assert.Equal(t, 2, febCount, "expected 2 docs in 2026-02 index")
	})

	// Verify msg-001 was updated (content should be edited version)
	t.Run("msg-001 updated content", func(t *testing.T) {
		doc := getDoc(t, esURL, prefix+"-2026-01", "msg-001")
		require.NotNil(t, doc, "msg-001 should exist")
		assert.Equal(t, "hello world (edited)", doc["content"])
		assert.Equal(t, "alice", doc["userAccount"])
		assert.Equal(t, "room-1", doc["roomId"])
	})

	// Verify msg-002 was deleted
	t.Run("msg-002 deleted", func(t *testing.T) {
		doc := getDoc(t, esURL, prefix+"-2026-01", "msg-002")
		assert.Nil(t, doc, "msg-002 should be deleted")
	})

	// Verify msg-003 exists with correct content
	t.Run("msg-003 exists", func(t *testing.T) {
		doc := getDoc(t, esURL, prefix+"-2026-01", "msg-003")
		require.NotNil(t, doc, "msg-003 should exist")
		assert.Equal(t, "different room", doc["content"])
		assert.Equal(t, "room-2", doc["roomId"])
	})

	// Verify msg-004 in february index
	t.Run("msg-004 in february", func(t *testing.T) {
		doc := getDoc(t, esURL, prefix+"-2026-02", "msg-004")
		require.NotNil(t, doc, "msg-004 should exist")
		assert.Equal(t, "february message", doc["content"])
		assert.Equal(t, "charlie", doc["userAccount"])
	})
}

// searchHits queries ES for docs where the content field matches the given query string.
// Returns the number of matching hits.
func searchHits(t *testing.T, esURL, indexPattern, query string) int {
	t.Helper()
	body := fmt.Sprintf(`{"query":{"match":{"content":{"query":%q}}}}`, query)
	u := esURLFor(t, esURL, indexPattern, "_search")
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
		} `json:"hits"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result.Hits.Total.Value
}

// searchHitsWildcard queries ES using a wildcard query on the content field.
// The search service uses this when the query contains underscores — it operates
// on the preserved original token so cost is bounded (one token per compound word).
func searchHitsWildcard(t *testing.T, esURL, indexPattern, pattern string) int {
	t.Helper()
	body := fmt.Sprintf(`{"query":{"wildcard":{"content":{"value":%q}}}}`, pattern)
	u := esURLFor(t, esURL, indexPattern, "_search")
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
		} `json:"hits"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result.Hits.Total.Value
}

// TestCustomAnalyzer verifies the underscore-preserving analyzer with HTML stripping.
// Indexes a doc with content "<b>error_handler</b> and <i>log_parser</i>" then searches
// for various subword and underscore combinations.
func TestCustomAnalyzer(t *testing.T) {
	esURL := setupElasticsearch(t)
	ctx := context.Background()

	prefix := "analyzer-test-v1"
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)

	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newMessageCollection(prefix)
	err = engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(messageTemplateBody(prefix)))
	require.NoError(t, err, "upsert template")

	preCreateIndex(t, esURL, prefix+"-2026-03")
	waitForClusterGreen(t, esURL, 120*time.Second)

	store := &engineAdapter{engine: engine}
	handler := NewHandler(store, coll, 100)

	// Doc 1: two-part underscore compounds with HTML
	evt1 := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "analyzer-msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "<b>error_handler</b> and <i>log_parser</i>",
			CreatedAt: time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC),
		},
		SiteID: "site-test", Timestamp: 2000001,
	}

	// Doc 2: three-part underscore compound
	evt2 := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "analyzer-msg-2", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "check the user_input_validator for issues",
			CreatedAt: time.Date(2026, 3, 10, 12, 5, 0, 0, time.UTC),
		},
		SiteID: "site-test", Timestamp: 2000002,
	}

	for _, evt := range []model.MessageEvent{evt1, evt2} {
		data, marshalErr := json.Marshal(evt)
		require.NoError(t, marshalErr)
		handler.Add(&stubMsg{data: data})
	}
	handler.Flush(ctx)

	refreshIndex(t, esURL, prefix+"-*")

	indexPattern := prefix + "-*"

	// Verify both docs were indexed
	require.NotNil(t, getDoc(t, esURL, prefix+"-2026-03", "analyzer-msg-1"))
	require.NotNil(t, getDoc(t, esURL, prefix+"-2026-03", "analyzer-msg-2"))

	// --- HTML stripping ---
	t.Run("html tags are stripped", func(t *testing.T) {
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "error_handler"),
			"should find doc with HTML-wrapped content")
	})

	// --- Two-part compound: error_handler, log_parser ---

	t.Run("exact compound word (match)", func(t *testing.T) {
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "error_handler"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "log_parser"))
	})

	t.Run("subword matches (match)", func(t *testing.T) {
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "error"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "handler"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "log"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "parser"))
	})

	// --- Underscore queries use wildcard ---
	// Search service rule: if query contains "_", use wildcard (append * if needed).
	// Wildcard operates on the preserved original token — bounded cost.

	t.Run("trailing underscore prefix (wildcard)", func(t *testing.T) {
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "error_*"),
			"'error_*' matches error_handler")
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "log_*"),
			"'log_*' matches log_parser")
	})

	t.Run("leading underscore suffix (wildcard)", func(t *testing.T) {
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "*_handler"),
			"'*_handler' matches error_handler")
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "*_parser"),
			"'*_parser' matches log_parser")
	})

	// --- Three-part compound: user_input_validator ---

	t.Run("three-part exact compound (match)", func(t *testing.T) {
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "user_input_validator"))
	})

	t.Run("three-part subwords (match)", func(t *testing.T) {
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "user"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "input"))
		assert.Equal(t, 1, searchHits(t, esURL, indexPattern, "validator"))
	})

	t.Run("three-part partial compound (wildcard)", func(t *testing.T) {
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "user_input*"),
			"'user_input*' matches user_input_validator")
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "*input_validator"),
			"'*input_validator' matches user_input_validator")
		assert.Equal(t, 1, searchHitsWildcard(t, esURL, indexPattern, "user_inp*"),
			"'user_inp*' partial match on original token")
	})

	t.Run("no false positives", func(t *testing.T) {
		assert.Equal(t, 0, searchHits(t, esURL, indexPattern, "nonexistent_term"))
		assert.Equal(t, 0, searchHitsWildcard(t, esURL, indexPattern, "error_parser*"),
			"cross-compound should not match")
	})
}
