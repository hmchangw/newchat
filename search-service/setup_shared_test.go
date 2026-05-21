//go:build integration

package main

// ES / NATS / Valkey / Mongo come from pkg/testutil. CCS tests bring
// their own ES pair (integration_ccs_test.go).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

const testUserRoomIndex = "user-room"

// NATS queue groups. Each search-service router gets its own so a slow
// drain after one test can't deliver to a sibling test's handler.
const (
	testQueueGroup     = "search-service-test"      // apps, users, CCS
	testQueueGroupSubs = "search-service-test-subs" // rooms
	testQueueGroupV2   = "search-service-test-v2"   // messages v2
)

// Bounded HTTP client for ES control-plane calls.
var testHTTPClient = &http.Client{Timeout: 10 * time.Second}

func seedDoc(t *testing.T, esURL, index, id string, doc any) {
	t.Helper()
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	url := fmt.Sprintf("%s/%s/_doc/%s?refresh=true", esURL, index, id)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Truef(t, resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK,
		"seedDoc %s/%s: status=%d body=%s", index, id, resp.StatusCode, body)
}

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m,
		testutil.EnsureElasticsearch,
		testutil.EnsureNATS,
		testutil.EnsureValkey,
		testutil.EnsureMongo,
	)
}

// setupRouter wires the NATS plumbing shared by every search-service
// fixture: server+client conns against the shared NATS, a router with the
// given queue group, RequestID middleware, register, flush, and cleanups.
// The Flush is required because otelnats wraps the conn — subscriptions
// don't reach the server otherwise before tests publish.
func setupRouter(t *testing.T, queueGroup string, register func(*natsrouter.Router)) *nats.Conn {
	t.Helper()
	natsURL := testutil.NATS(t)
	serverNC, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err, "connect nats (server side)")
	t.Cleanup(func() { _ = serverNC.Drain() })

	clientNC, err := nats.Connect(natsURL)
	require.NoError(t, err, "connect nats (client side)")
	t.Cleanup(func() { clientNC.Close() })

	router := natsrouter.New(serverNC, queueGroup)
	router.Use(natsrouter.RequestID())
	register(router)
	require.NoError(t, serverNC.NatsConn().Flush())
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })

	return clientNC
}
