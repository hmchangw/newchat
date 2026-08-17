package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeState struct{}

func (fakeState) Snapshot() ([]CheckResult, []CheckResult, Counters) {
	return []CheckResult{{ID: "1", State: StateMatched}},
		[]CheckResult{{ID: "2", State: StateFailed}},
		Counters{Checked: 2, Matched: 1, Failed: 1}
}

type fakeStats struct{}

func (fakeStats) Last() StreamStats { return StreamStats{Stream: "S", Msgs: 42} }

var testPairs = []TargetPair{
	{Source: "rocketchat_room", Alias: "rooms", Kind: "mongo", Dest: "rooms"},
	{Source: "rocketchat_message", Alias: "msgById", Kind: "cassandra", Dest: "messages_by_id"},
}

func ptrConnInfo() *ConnInfo {
	c := testConnInfo()
	return &c
}

func testConnInfo() ConnInfo {
	return buildConnInfo(&config{
		SiteID: "site1", NATSURL: "nats://n:4222",
		SourceMongoURI: "mongodb://u:p@s:27017", SourceDB: "rocketchat",
		TargetMongoURI: "mongodb://t:27017", TargetDB: "chat",
	}, "MIGRATION-OPLOG-site1", false)
}

var testMappingJSON = []byte(`{"sources":[{"collection":"rocketchat_room",` +
	`"ops":{"insert":"verify"},"targets":{"rooms":{"kind":"mongo","collection":"rooms",` +
	`"key":{"_id":"_id"}}},"fields":{"name":["rooms.name"]}}]}`)

func testServer(t *testing.T) (*httptest.Server, *hub) {
	t.Helper()
	h := newHub()
	handler := newHandler(h, fakeState{}, fakeStats{}, 200, 1000, testPairs, fakeInspector{}, ptrConnInfo(), testMappingJSON)
	mux := http.NewServeMux()
	handler.registerRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, h
}

func TestHealthz(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPIState(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/state")
	require.NoError(t, err)
	defer resp.Body.Close()
	var body struct {
		Stats     StreamStats   `json:"stats"`
		Recent    []CheckResult `json:"recent"`
		Failures  []CheckResult `json:"failures"`
		Counters  Counters      `json:"counters"`
		RecentCap int           `json:"recentCap"`
		FailedCap int           `json:"failedCap"`
		Pairs     []TargetPair  `json:"pairs"`
		ConnInfo  ConnInfo      `json:"connInfo"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, uint64(42), body.Stats.Msgs)
	require.Len(t, body.Recent, 1)
	require.Len(t, body.Failures, 1)
	assert.Equal(t, uint64(2), body.Counters.Checked)
	assert.Equal(t, 200, body.RecentCap)
	assert.Equal(t, 1000, body.FailedCap)
	assert.Equal(t, testPairs, body.Pairs)
	assert.Equal(t, "site1", body.ConnInfo.SiteID)
	assert.Equal(t, "mongodb://***@s:27017", body.ConnInfo.SourceMongo.URI)
}

func TestFailuresDownload(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/failures.json")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Contains(t, resp.Header.Get("Content-Disposition"), "cdc-verify-failures.json")
	var failures []CheckResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&failures))
	require.Len(t, failures, 1)
	assert.Equal(t, "2", failures[0].ID)
}

func TestIndexServed(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
}

func TestSSEDeliversEvents(t *testing.T) {
	srv, h := testServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// give the handler a beat to register, then broadcast
	deadline := time.Now().Add(time.Second)
	for h.clientCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, h.clientCount())
	h.broadcastResult(CheckResult{ID: "r1", State: StateMatched})

	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	chunk := string(buf[:n])
	assert.True(t, strings.Contains(chunk, `"kind":"result"`), "got: %s", chunk)
	assert.Contains(t, chunk, `"r1"`)
}

// noFlushWriter implements http.ResponseWriter but not http.Flusher, to
// exercise the "streaming unsupported" branch of events().
type noFlushWriter struct {
	header http.Header
	status int
}

func (n *noFlushWriter) Header() http.Header {
	if n.header == nil {
		n.header = http.Header{}
	}
	return n.header
}

func (n *noFlushWriter) Write(b []byte) (int, error) { return len(b), nil }
func (n *noFlushWriter) WriteHeader(status int)      { n.status = status }

func TestEventsHandler_StreamingUnsupported(t *testing.T) {
	h := newHandler(newHub(), fakeState{}, fakeStats{}, 200, 1000, testPairs, fakeInspector{}, ptrConnInfo(), testMappingJSON)
	req := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	w := &noFlushWriter{}
	h.events(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.status)
}

func TestWriteJSON_EncodeError(t *testing.T) {
	rec := httptest.NewRecorder()
	assert.NotPanics(t, func() { writeJSON(rec, make(chan int)) })
}

func TestHub_RegisterUnregisterAndDrop(t *testing.T) {
	h := newHub()
	id, ch := h.register()
	assert.Equal(t, 1, h.clientCount())
	h.broadcastStats(StreamStats{Msgs: 1})
	frame := <-ch
	var ev sseEvent
	require.NoError(t, json.Unmarshal(frame, &ev))
	assert.Equal(t, "stats", ev.Kind)

	// Fill the buffer past capacity: the hub disconnects the unread client
	// rather than blocking or silently dropping frames.
	for i := 0; i < 100; i++ {
		h.broadcastStats(StreamStats{Msgs: uint64(i)})
	}
	assert.Equal(t, 0, h.clientCount())
	h.unregister(id) // handler's deferred unregister is a harmless no-op
	assert.Equal(t, 0, h.clientCount())
}

type fakeInspector struct{}

func (fakeInspector) Inspect(_ context.Context, collection, docID string) (InspectResult, error) {
	if collection == "unmapped_coll" {
		return InspectResult{}, fmt.Errorf("%w: %q", errUnmapped, collection)
	}
	if collection == "boom_coll" {
		return InspectResult{}, fmt.Errorf("dial mongodb://internal-host-9:27017: refused")
	}
	return InspectResult{
		Collection: collection,
		DocID:      docID,
		Source:     map[string]any{"_id": docID, "name": "general"},
		Targets: []InspectTarget{
			{Alias: "rooms", Kind: "mongo", Dest: "rooms", Key: map[string]any{"_id": docID},
				Doc: map[string]any{"_id": docID, "name": "general"}},
		},
	}, nil
}

func TestAPIInspect(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/inspect?collection=rocketchat_room&doc=r1")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body InspectResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "rocketchat_room", body.Collection)
	assert.Equal(t, "r1", body.DocID)
	assert.Equal(t, "general", body.Source["name"])
	require.Len(t, body.Targets, 1)
	assert.Equal(t, "rooms", body.Targets[0].Alias)
}

func TestAPIInspect_BadRequest(t *testing.T) {
	srv, _ := testServer(t)
	for _, q := range []string{"", "?collection=x", "?doc=y"} {
		resp, err := http.Get(srv.URL + "/api/inspect" + q)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "query %q", q)
	}
}

func TestAPIInspect_UnmappedIs404(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/inspect?collection=unmapped_coll&doc=x")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// A store failure returns a generic 500 body: the raw error can carry internal
// URIs/hosts that must never reach the browser.
func TestAPIInspect_InternalErrorBodyIsGeneric(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/inspect?collection=boom_coll&doc=x")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "internal-host-9")
	assert.Contains(t, string(body), "inspect failed")
}

// The server log must not retain credentials either: URI userinfo inside a
// store error is scrubbed before logging.
func TestScrubLogValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"mongo uri with creds", "dial mongodb://user:s3cret@host:27017: refused", "dial mongodb://***@host:27017: refused"},
		{"nats token", "connect nats://t0ken@nats:4222 failed", "connect nats://***@nats:4222 failed"},
		{"no credentials untouched", "context deadline exceeded", "context deadline exceeded"},
		{"plain uri untouched", "dial mongodb://host:27017: refused", "dial mongodb://host:27017: refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, scrubLogValue(tt.in))
		})
	}
}

func TestAPIMapping(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/mapping")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	var m Mapping
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	require.Len(t, m.Sources, 1)
	assert.Equal(t, "rocketchat_room", m.Sources[0].Collection)
}
