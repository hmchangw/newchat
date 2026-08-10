package main

import (
	"encoding/json"
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

func testServer(t *testing.T) (*httptest.Server, *hub) {
	t.Helper()
	h := newHub()
	handler := newHandler(h, fakeState{}, fakeStats{})
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
		Stats    StreamStats   `json:"stats"`
		Recent   []CheckResult `json:"recent"`
		Failures []CheckResult `json:"failures"`
		Counters Counters      `json:"counters"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, uint64(42), body.Stats.Msgs)
	require.Len(t, body.Recent, 1)
	require.Len(t, body.Failures, 1)
	assert.Equal(t, uint64(2), body.Counters.Checked)
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

func TestHub_RegisterUnregisterAndDrop(t *testing.T) {
	h := newHub()
	id, ch := h.register()
	assert.Equal(t, 1, h.clientCount())
	h.broadcastStats(StreamStats{Msgs: 1})
	ev := <-ch
	assert.Equal(t, "stats", ev.Kind)

	// fill the buffer; further broadcasts must not block
	for i := 0; i < 100; i++ {
		h.broadcastStats(StreamStats{Msgs: uint64(i)})
	}
	h.unregister(id)
	assert.Equal(t, 0, h.clientCount())
}
