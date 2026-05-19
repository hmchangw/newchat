//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	natsmod "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

const testUserRoomIndex = "user-room"

// --- Fixture -----------------------------------------------------------------

// ccsFixture is the full stack for cross-cluster integration tests: two ES
// containers on a shared Docker network (with CCS configured from local →
// remote), plus Valkey and NATS, plus the wired search-service router.
//
// localURL / remoteURL are the host-mapped HTTP URLs for seeding; the
// search-service itself sees only localURL. `clientNATS` is the raw NATS
// client used to issue request/reply calls.
type ccsFixture struct {
	localURL   string
	remoteURL  string
	localES    searchengine.SearchEngine
	remoteES   searchengine.SearchEngine
	clientNATS *nats.Conn
}

// setupCCSFixture stands up the whole CCS environment. Total cost is ~ES
// container start × 2 (~60-90s) so tests that use it should reuse via
// TestMain when added.
//
// Every major step emits a `t.Logf` so a CI failure (where raw logs are
// often opaque on public runs) leaves enough breadcrumbs in the `go test`
// output to pinpoint which phase broke.
func setupCCSFixture(t *testing.T) *ccsFixture {
	t.Helper()
	ctx := context.Background()

	t.Logf("CCS fixture: creating docker network")
	nw, err := network.New(ctx)
	require.NoError(t, err, "create docker network")
	t.Cleanup(func() { _ = nw.Remove(ctx) })
	t.Logf("CCS fixture: network %q created", nw.Name)

	t.Logf("CCS fixture: starting remote ES container (alias=es-remote)")
	remoteURL := startESForCCS(t, nw, "es-remote", "remote-cluster")
	t.Logf("CCS fixture: remote ES up at %s", remoteURL)

	t.Logf("CCS fixture: starting local ES container (alias=es-local)")
	localURL := startESForCCS(t, nw, "es-local", "local-cluster")
	t.Logf("CCS fixture: local ES up at %s", localURL)

	// Wire local ES to reach the remote in PROXY mode. Proxy mode opens a
	// single direct connection to the configured address and skips the
	// sniff-then-reconnect dance that sniff mode does — that dance requires
	// each remote node to advertise a reachable publish address, which is
	// fragile when docker containers bind transport on 0.0.0.0 and the
	// publish address defaults to an interface the peer can't route to.
	// Proxy mode is the robust choice for CCS over an ephemeral docker
	// network. Ref: ES docs "Remote cluster settings" → `mode=proxy`.
	t.Logf("CCS fixture: configuring cluster.remote.remote1 (proxy mode → es-remote:9300)")
	putClusterSetting(t, localURL, map[string]any{
		"persistent": map[string]any{
			"cluster.remote.remote1.mode":          "proxy",
			"cluster.remote.remote1.proxy_address": "es-remote:9300",
		},
	})
	t.Logf("CCS fixture: waiting for remote1 to report connected=true (timeout 120s)")
	waitForRemoteConnected(t, localURL, "remote1", 120*time.Second)
	t.Logf("CCS fixture: remote1 connected")

	localEngine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: localURL})
	require.NoError(t, err, "build searchengine for local")
	remoteEngine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: remoteURL})
	require.NoError(t, err, "build searchengine for remote")

	t.Logf("CCS fixture: starting valkey")
	valkeyAddr := startValkey(t)
	valkeyClient, err := valkeyutil.Connect(ctx, valkeyAddr, "")
	require.NoError(t, err, "connect valkey")
	t.Cleanup(func() { valkeyutil.Disconnect(valkeyClient) })
	t.Logf("CCS fixture: valkey at %s", valkeyAddr)

	t.Logf("CCS fixture: starting NATS")
	natsURL := startNATS(t)
	serverNC, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err, "connect nats (server side)")
	t.Cleanup(func() { _ = serverNC.Drain() })

	clientNC, err := nats.Connect(natsURL)
	require.NoError(t, err, "connect nats (client side)")
	t.Cleanup(func() { clientNC.Close() })
	t.Logf("CCS fixture: NATS at %s", natsURL)

	userRoomIndex := testUserRoomIndex
	store := newESStore(localEngine, userRoomIndex)
	cache := newValkeyCache(valkeyClient)
	handler := newHandler(store, nil, nil, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		UserRoomIndex:           userRoomIndex,
		SpotlightReadPattern:    "spotlight-test-*",
	})

	router := natsrouter.New(serverNC, "search-service-test")
	router.Use(natsrouter.RequestID())
	handler.Register(router)
	// Flush — see setupAppsFixture for the rationale.
	require.NoError(t, serverNC.NatsConn().Flush())

	return &ccsFixture{
		localURL:   localURL,
		remoteURL:  remoteURL,
		localES:    localEngine,
		remoteES:   remoteEngine,
		clientNATS: clientNC,
	}
}

// startESForCCS starts one ES node on the shared network with the given
// network alias so the peer can reach it at `{alias}:9300`. Returns the
// host-mapped HTTP URL for seeding.
//
// `transport.host: 0.0.0.0` is required so the transport port binds on all
// interfaces, including the bridge network (ES 8.x defaults to `_site_`
// which excludes the container's bridge IP in some setups). CCS itself
// uses `proxy` mode to avoid publish-address sensitivity — see
// setupCCSFixture. `xpack.security.enabled=false` matches the local dev
// deps compose.
func startESForCCS(t *testing.T, nw *testcontainers.DockerNetwork, alias, clusterName string) string {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.Elasticsearch,
			ExposedPorts: []string{"9200/tcp", "9300/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {alias},
			},
			Env: map[string]string{
				"cluster.name":           clusterName,
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
				"network.host":           "0.0.0.0",
				"transport.host":         "0.0.0.0",
				"cluster.routing.allocation.disk.threshold_enabled": "false",
				"ES_JAVA_OPTS": "-Xms512m -Xmx512m",
			},
			WaitingFor: wait.ForAll(
				wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(120*time.Second),
				wait.ForHTTP("/_cluster/health?wait_for_status=yellow&timeout=60s").
					WithPort("9200/tcp").
					WithStartupTimeout(120*time.Second),
			),
		},
		Started: true,
	})
	require.NoError(t, err, "start elasticsearch (%s)", alias)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9200")
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func startValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.Valkey,
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"valkey-server", "--save", "", "--appendonly", "no"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start valkey")
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379")
	require.NoError(t, err)
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func startNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := natsmod.Run(ctx, testimages.NATS)
	require.NoError(t, err, "start nats")
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	url, err := c.ConnectionString(ctx)
	require.NoError(t, err, "nats connection string")
	return url
}

// --- Index templates ---------------------------------------------------------

// buildTestTemplate wraps a pattern + property map with single-node-friendly
// index settings (1 shard, 0 replicas, 1s refresh) and `dynamic: false`
// mappings. The templates below hand-roll their property sets so the tests
// remain independent of search-sync-worker's custom-analyzer configuration.
func buildTestTemplate(pattern string, properties map[string]any) json.RawMessage {
	body := map[string]any{
		"index_patterns": []string{pattern},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   1,
					"number_of_replicas": 0,
					"refresh_interval":   "1s",
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": properties,
			},
		},
	}
	data, _ := json.Marshal(body)
	return data
}

func messageTestTemplate() json.RawMessage {
	return buildTestTemplate("messages-*", map[string]any{
		"messageId":   map[string]any{"type": "keyword"},
		"roomId":      map[string]any{"type": "keyword"},
		"siteId":      map[string]any{"type": "keyword"},
		"userId":      map[string]any{"type": "keyword"},
		"userAccount": map[string]any{"type": "keyword"},
		"content": map[string]any{
			"type": "text",
			"fields": map[string]any{
				"keyword": map[string]any{"type": "keyword"},
			},
		},
		"createdAt":                    map[string]any{"type": "date"},
		"threadParentMessageId":        map[string]any{"type": "keyword"},
		"threadParentMessageCreatedAt": map[string]any{"type": "date"},
		"tshow":                        map[string]any{"type": "boolean"},
	})
}

func userRoomTestTemplate() json.RawMessage {
	return buildTestTemplate(testUserRoomIndex, map[string]any{
		"userAccount": map[string]any{"type": "keyword"},
		"rooms": map[string]any{
			"type": "text",
			"fields": map[string]any{
				"keyword": map[string]any{"type": "keyword", "ignore_above": 256},
			},
		},
		"restrictedRooms": map[string]any{"type": "flattened"},
		"roomTimestamps":  map[string]any{"type": "flattened"},
		"createdAt":       map[string]any{"type": "date"},
		"updatedAt":       map[string]any{"type": "date"},
	})
}

// --- HTTP helpers ------------------------------------------------------------

// testHTTPClient is a bounded HTTP client for ES control-plane calls —
// stalled containers shouldn't be able to hang the integration job past
// the per-call deadline. Kept small on purpose: these calls hit localhost
// (docker-mapped port) and are cheap when they succeed.
var testHTTPClient = &http.Client{Timeout: 10 * time.Second}

// putClusterSetting pushes a /_cluster/settings update. Used to configure
// the CCS remote after both clusters are up.
func putClusterSetting(t *testing.T, esURL string, body map[string]any) {
	t.Helper()
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, esURL+"/_cluster/settings", bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err, "put cluster settings")
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "put cluster settings: %s", respBody)
}

// waitForRemoteConnected polls /_remote/info until the given remote cluster
// reports connected=true. CCS registration is async — the settings call
// returns immediately but the transport handshake happens in the
// background. On timeout, the last-seen /_remote/info body is captured in
// the failure message so CI can diagnose whether the remote was ever
// registered, what mode it ended up in, or why it couldn't connect.
func waitForRemoteConnected(t *testing.T, localURL, remoteName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastBody string
	for time.Now().Before(deadline) {
		resp, err := testHTTPClient.Get(localURL + "/_remote/info")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastBody = string(body)
			var info map[string]struct {
				Connected bool `json:"connected"`
			}
			if json.Unmarshal(body, &info) == nil {
				if entry, ok := info[remoteName]; ok && entry.Connected {
					return
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("remote cluster %q never became connected within %s\nlast /_remote/info body: %s",
		remoteName, timeout, lastBody)
}

// seedDoc PUTs a JSON document into ES, synchronously refreshing the index
// so the next search sees it.
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

// --- Templates on both clusters ---------------------------------------------

func (f *ccsFixture) installTemplates(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	t.Logf("templates: upserting messages_template on local")
	require.NoError(t, f.localES.UpsertTemplate(ctx, "messages_template", messageTestTemplate()),
		"upsert messages_template on local")
	t.Logf("templates: upserting messages_template on remote")
	require.NoError(t, f.remoteES.UpsertTemplate(ctx, "messages_template", messageTestTemplate()),
		"upsert messages_template on remote")
	// user-room is local-only per the search-service architecture.
	t.Logf("templates: upserting user_room_template on local")
	require.NoError(t, f.localES.UpsertTemplate(ctx, "user_room_template", userRoomTestTemplate()),
		"upsert user_room_template on local")
	t.Logf("templates: all upserted")
}

// --- Test --------------------------------------------------------------------

// TestSearchService_SearchMessages_CCS_CrossCluster_Unrestricted verifies
// the core CCS promise: a user's search crosses from the local cluster
// (`messages-*`) to a remote cluster (`*:messages-*`) and the service
// returns the merged result set. Both rooms are unrestricted — they live in
// the user-room doc's `rooms[]` — and the terms-lookup clause handles them
// uniformly regardless of which site hosts the message.
func TestSearchService_SearchMessages_CCS_CrossCluster_Unrestricted(t *testing.T) {
	f := setupCCSFixture(t)
	f.installTemplates(t)

	// --- Seed --------------------------------------------------------------
	//
	// Alice is a member of two unrestricted rooms: one lives on the local
	// site, the other on the remote site. The user-room doc (local-only)
	// lists BOTH in `rooms[]` — the sync-worker would normally populate
	// this via INBOX events; here we seed directly.
	const account = "alice"
	const localRoomID = "room-local-1"
	const remoteRoomID = "room-remote-1"

	now := time.Now().UTC()
	createdAt := now.Add(-time.Hour)
	monthIdx := "messages-" + createdAt.Format("2006-01")

	// user-room doc: unrestricted memberships in both rooms.
	seedDoc(t, f.localURL, testUserRoomIndex, account, map[string]any{
		"userAccount":     account,
		"rooms":           []string{localRoomID, remoteRoomID},
		"restrictedRooms": map[string]int64{},
		"roomTimestamps": map[string]int64{
			localRoomID:  createdAt.UnixMilli(),
			remoteRoomID: createdAt.UnixMilli(),
		},
		"createdAt": createdAt.Format(time.RFC3339Nano),
		"updatedAt": createdAt.Format(time.RFC3339Nano),
	})

	// Local message in local room.
	seedDoc(t, f.localURL, monthIdx, "msg-local-1", map[string]any{
		"messageId":   "msg-local-1",
		"roomId":      localRoomID,
		"siteId":      "site-local",
		"userId":      "user-bob",
		"userAccount": "bob",
		"content":     "hello from local",
		"createdAt":   createdAt.Format(time.RFC3339Nano),
	})

	// Remote message in remote room. Same index pattern (`messages-*`) on
	// the remote cluster — CCS resolves the `*:messages-*` segment on the
	// local query.
	seedDoc(t, f.remoteURL, monthIdx, "msg-remote-1", map[string]any{
		"messageId":   "msg-remote-1",
		"roomId":      remoteRoomID,
		"siteId":      "site-remote",
		"userId":      "user-carol",
		"userAccount": "carol",
		"content":     "hello from remote",
		"createdAt":   createdAt.Format(time.RFC3339Nano),
	})

	// --- Search via NATS ---------------------------------------------------
	//
	// Round-trips through the real natsrouter: the handler reads
	// restrictedRooms from Valkey (miss → ES prefetch → Valkey SET), then
	// builds the CCS query against `messages-*,*:messages-*` and parses
	// the merged response.
	req := model.SearchMessagesRequest{Query: "hello"}
	reqData, err := json.Marshal(req)
	require.NoError(t, err)

	// Generous timeout: first request is Valkey miss → ES prefetch of
	// user-room doc → CCS fanout → response parse. Tight timeouts mask
	// real latency bugs in integration.
	msg, err := f.clientNATS.Request(subject.SearchMessages(account), reqData, 30*time.Second)
	require.NoError(t, err, "NATS request failed")

	t.Logf("response: %s", msg.Data)

	var resp model.SearchMessagesResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp), "decode response: %s", msg.Data)

	assert.EqualValues(t, 2, resp.Total, "expected both local + remote hits; got body=%s", msg.Data)
	require.Len(t, resp.Messages, 2, "expected 2 hits; got body=%s", msg.Data)

	gotRooms := map[string]string{}
	for _, hit := range resp.Messages {
		gotRooms[hit.RoomID] = hit.SiteID
	}
	assert.Equal(t, "site-local", gotRooms[localRoomID], "local message should be present")
	assert.Equal(t, "site-remote", gotRooms[remoteRoomID], "remote message should be present via CCS")
}

// TestSearchService_SearchMessages_CCS_CrossCluster_Restricted verifies
// the restricted-room access-control clauses fire correctly across the
// CCS boundary. Alice is a member of one UNRESTRICTED local room and one
// RESTRICTED remote room with historySharedSince (HSS) set to a specific
// cutoff. The user-room doc (local-only) routes the remote room into
// `restrictedRooms{rid: hssMillis}`.
//
// Seed on the remote cluster covers every branch the query builder
// encodes for restricted rooms:
//
//   - pre-HSS parent                            → MUST NOT match (Clause A: createdAt < hss)
//   - post-HSS parent                           → MUST match    (Clause A)
//   - post-HSS thread reply, tshow=true         → MUST match    (Clause B1: outer gate passes + tshow=true fires B1, even though parent is pre-HSS)
//   - post-HSS thread reply, tshow=false        → MUST NOT match (Clause B fails: outer gate passes but inner OR fails — tshow=false AND parent < hss so B2 also fails)
//
// Plus one unrestricted local parent to prove the two paths interact
// cleanly on the same search. Total expected hits: 3 (local + post-HSS
// remote parent + post-HSS remote reply with tshow=true).
func TestSearchService_SearchMessages_CCS_CrossCluster_Restricted(t *testing.T) {
	f := setupCCSFixture(t)
	f.installTemplates(t)

	const account = "alice"
	const localRoomID = "room-local-unrestricted"
	const remoteRoomID = "room-remote-restricted"

	// Temporal setup:
	//   - hss is the user's join-time bound for the restricted remote room.
	//   - preHSS is 3 hours before hss (so pre-HSS messages are clearly
	//     older than the gate).
	//   - postHSS is 1 hour after hss.
	// All well within the default 1-year `recent_window` so none of them
	// get filtered out by the global createdAt range filter.
	now := time.Now().UTC()
	hss := now.Add(-2 * time.Hour)
	preHSS := hss.Add(-3 * time.Hour)
	postHSS := hss.Add(time.Hour)
	monthIdxFor := func(ts time.Time) string { return "messages-" + ts.Format("2006-01") }

	// user-room doc: local room unrestricted, remote room restricted with hss.
	t.Logf("seed: upserting user-room doc for %s (restricted %s since %s)", account, remoteRoomID, hss.Format(time.RFC3339))
	seedDoc(t, f.localURL, testUserRoomIndex, account, map[string]any{
		"userAccount": account,
		"rooms":       []string{localRoomID},
		"restrictedRooms": map[string]int64{
			remoteRoomID: hss.UnixMilli(),
		},
		"roomTimestamps": map[string]int64{
			localRoomID:  now.UnixMilli(),
			remoteRoomID: now.UnixMilli(),
		},
		"createdAt": now.Format(time.RFC3339Nano),
		"updatedAt": now.Format(time.RFC3339Nano),
	})

	// --- LOCAL unrestricted room ----------------------------------------
	// One plain message that should always match via the terms-lookup
	// branch (no HSS involved).
	t.Logf("seed: local unrestricted message in %s", localRoomID)
	seedDoc(t, f.localURL, monthIdxFor(postHSS), "msg-local-1", map[string]any{
		"messageId":   "msg-local-1",
		"roomId":      localRoomID,
		"siteId":      "site-local",
		"userId":      "user-bob",
		"userAccount": "bob",
		"content":     "hello from local",
		"createdAt":   postHSS.Format(time.RFC3339Nano),
	})

	// --- REMOTE restricted room -----------------------------------------
	// Four messages, each exercising one branch of the restricted-room
	// clauses. Pre-HSS parent lives at `msg-remote-pre-parent`; its
	// thread replies reference it via threadParentMessageId +
	// threadParentMessageCreatedAt=preHSS.
	t.Logf("seed: remote pre-HSS parent (MUST NOT match)")
	seedDoc(t, f.remoteURL, monthIdxFor(preHSS), "msg-remote-pre-parent", map[string]any{
		"messageId":   "msg-remote-pre-parent",
		"roomId":      remoteRoomID,
		"siteId":      "site-remote",
		"userId":      "user-carol",
		"userAccount": "carol",
		"content":     "hello pre-hss parent",
		"createdAt":   preHSS.Format(time.RFC3339Nano),
	})

	t.Logf("seed: remote post-HSS parent (Clause A match)")
	seedDoc(t, f.remoteURL, monthIdxFor(postHSS), "msg-remote-post-parent", map[string]any{
		"messageId":   "msg-remote-post-parent",
		"roomId":      remoteRoomID,
		"siteId":      "site-remote",
		"userId":      "user-carol",
		"userAccount": "carol",
		"content":     "hello post-hss parent",
		"createdAt":   postHSS.Format(time.RFC3339Nano),
	})

	// Post-HSS reply to a pre-HSS parent, tshow=true → Clause B1 matches.
	// The reply's own createdAt satisfies Clause B's outer gate
	// (createdAt >= hss); tshow=true then fires B1 regardless of the
	// parent's age. If the outer gate weren't there, a pre-HSS tshow=true
	// reply would leak history the user never had access to.
	t.Logf("seed: remote post-HSS reply with tshow=true, pre-HSS parent (Clause B1 match)")
	seedDoc(t, f.remoteURL, monthIdxFor(postHSS), "msg-remote-reply-tshow", map[string]any{
		"messageId":                    "msg-remote-reply-tshow",
		"roomId":                       remoteRoomID,
		"siteId":                       "site-remote",
		"userId":                       "user-carol",
		"userAccount":                  "carol",
		"content":                      "hello tshow reply",
		"createdAt":                    postHSS.Add(time.Minute).Format(time.RFC3339Nano),
		"threadParentMessageId":        "msg-remote-pre-parent",
		"threadParentMessageCreatedAt": preHSS.Format(time.RFC3339Nano),
		"tshow":                        true,
	})

	// Post-HSS reply to a pre-HSS parent, tshow=false → Clause B rejects.
	// Outer gate passes (reply createdAt >= hss) but the inner OR fails:
	// tshow=false blocks B1 and the parent's pre-HSS createdAt blocks B2.
	t.Logf("seed: remote post-HSS reply without tshow, pre-HSS parent (MUST NOT match)")
	seedDoc(t, f.remoteURL, monthIdxFor(postHSS), "msg-remote-reply-plain", map[string]any{
		"messageId":                    "msg-remote-reply-plain",
		"roomId":                       remoteRoomID,
		"siteId":                       "site-remote",
		"userId":                       "user-carol",
		"userAccount":                  "carol",
		"content":                      "hello plain reply",
		"createdAt":                    postHSS.Add(2 * time.Minute).Format(time.RFC3339Nano),
		"threadParentMessageId":        "msg-remote-pre-parent",
		"threadParentMessageCreatedAt": preHSS.Format(time.RFC3339Nano),
	})

	// --- Search ---------------------------------------------------------
	reqData, err := json.Marshal(model.SearchMessagesRequest{Query: "hello"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchMessages(account), reqData, 30*time.Second)
	require.NoError(t, err, "NATS request failed")
	t.Logf("response: %s", msg.Data)

	var resp model.SearchMessagesResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp), "decode response: %s", msg.Data)

	got := map[string]bool{}
	for _, hit := range resp.Messages {
		got[hit.MessageID] = true
	}

	// Expected matches:
	assert.True(t, got["msg-local-1"], "local unrestricted message must match via terms-lookup")
	assert.True(t, got["msg-remote-post-parent"], "post-HSS remote parent must match via Clause A (CCS)")
	assert.True(t, got["msg-remote-reply-tshow"], "post-HSS remote reply with tshow=true must match via Clause B1 (CCS)")

	// Expected exclusions:
	assert.False(t, got["msg-remote-pre-parent"], "pre-HSS remote parent must be excluded by Clause A gate")
	assert.False(t, got["msg-remote-reply-plain"], "post-HSS remote reply without tshow + pre-HSS parent must be excluded (outer gate passes; B1 and B2 both fail)")

	assert.EqualValues(t, 3, resp.Total, "expected exactly 3 hits; got body=%s", msg.Data)
	require.Len(t, resp.Messages, 3, "expected 3 hits; got body=%s", msg.Data)
}

// --- search.apps integration ------------------------------------------------

// setupAppsFixture starts an isolated Mongo container (via pkg/testutil) and
// a single search-service router bound to that DB. ES/Valkey are not used by
// search.apps, so we wire fakes (the existing `fakeStore` / `fakeCache`
// satisfy the interfaces but never get called on the apps path).
type appsFixture struct {
	clientNATS *nats.Conn
	mongoDB    *mongo.Database
}

func setupAppsFixture(t *testing.T) *appsFixture {
	t.Helper()
	ctx := context.Background()

	mongoDB := testutil.MongoDB(t, "search_service_test")

	// Start NATS (reuse the existing NATS container helper).
	natsContainer, err := natsmod.Run(ctx, testimages.NATS,
		testcontainers.WithWaitStrategy(wait.ForLog("Server is ready").WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = natsContainer.Terminate(ctx) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	require.NoError(t, err)

	serverNATS, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverNATS.Drain() })

	clientNATS, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { clientNATS.Close() })

	// Wire the handler with a real mongoStore and stub ES/cache.
	mongoStore := newMongoStore(mongoDB)
	store := &fakeStore{}
	cache := newFakeCache()
	h := newHandler(store, mongoStore, nil, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		SpotlightReadPattern:    "spotlight-*",
	})

	router := natsrouter.New(serverNATS, "search-service-test")
	router.Use(natsrouter.RequestID())
	h.Register(router)
	// Flush ensures subscriptions are registered on the server before the
	// fixture returns. Without this, fast tests that fire a request
	// immediately can hit "no responders available" while subscriptions
	// are still propagating. natsutil.Connect returns an otelnats.Conn
	// wrapper that doesn't expose Flush; reach through to the underlying
	// *nats.Conn.
	require.NoError(t, serverNATS.NatsConn().Flush())
	t.Cleanup(func() {
		_ = router.Shutdown(context.Background())
	})

	return &appsFixture{clientNATS: clientNATS, mongoDB: mongoDB}
}

func TestIntegration_SearchApps_PrototypePipeline(t *testing.T) {
	f := setupAppsFixture(t)
	ctx := context.Background()

	// Seed 3 apps in Mongo. The prototype pipeline matches by `name` regex
	// (case-insensitive) and applies $limit; the full $lookup access-guard
	// pipeline is implemented in a follow-up.
	_, err := f.mongoDB.Collection("apps").InsertMany(ctx, []any{
		map[string]any{"_id": "a1", "name": "Weather Alpha", "assistant": map[string]any{"enabled": true, "name": "weather.bot"}},
		map[string]any{"_id": "a2", "name": "Weatherly", "assistant": map[string]any{"enabled": false, "name": "weatherly.bot"}},
		map[string]any{"_id": "a3", "name": "Calendar"},
	})
	require.NoError(t, err)

	reqBytes, err := json.Marshal(model.SearchAppsRequest{Query: "weather"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var resp model.SearchAppsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Apps, 2, "two apps match the 'weather' regex")
	names := []string{resp.Apps[0].Name, resp.Apps[1].Name}
	assert.Contains(t, names, "Weather Alpha")
	assert.Contains(t, names, "Weatherly")
}

func TestIntegration_SearchApps_AssistantEnabledFilter(t *testing.T) {
	f := setupAppsFixture(t)
	ctx := context.Background()

	_, err := f.mongoDB.Collection("apps").InsertMany(ctx, []any{
		map[string]any{"_id": "a1", "name": "Weather Alpha", "assistant": map[string]any{"enabled": true, "name": "weather.bot"}},
		map[string]any{"_id": "a2", "name": "Weatherly", "assistant": map[string]any{"enabled": false, "name": "weatherly.bot"}},
	})
	require.NoError(t, err)

	enabled := true
	reqBytes, err := json.Marshal(model.SearchAppsRequest{
		Query:            "weather",
		AssistantEnabled: &enabled,
	})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var resp model.SearchAppsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Apps, 1)
	assert.Equal(t, "Weather Alpha", resp.Apps[0].Name)
}

func TestIntegration_SearchApps_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupAppsFixture(t)

	reqBytes, err := json.Marshal(model.SearchAppsRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}

// --- search.users integration ------------------------------------------------

// usersFixture is a minimal fixture for the search.users path: NATS for the
// request/reply layer, and an httptest.Server standing in for the third-party
// HR endpoint. No Mongo or ES containers are needed.
type usersFixture struct {
	clientNATS *nats.Conn
	thirdParty *httptest.Server // controls the stub response
}

func setupUsersFixture(t *testing.T, thirdPartyHandler http.Handler) *usersFixture {
	t.Helper()

	// Start the stub third-party server.
	stub := httptest.NewServer(thirdPartyHandler)
	t.Cleanup(stub.Close)

	// NATS.
	natsURL := startNATS(t)
	serverNC, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err, "connect nats (server side)")
	t.Cleanup(func() { _ = serverNC.Drain() })

	clientNC, err := nats.Connect(natsURL)
	require.NoError(t, err, "connect nats (client side)")
	t.Cleanup(func() { clientNC.Close() })

	// Wire the handler with a real httpUsersClient pointing at the stub.
	usersRC := restyutil.New(stub.URL, restyutil.WithTimeout(5*time.Second))
	usersClient := newHTTPUsersClient(usersRC, "")

	h := newHandler(nil, nil, usersClient, newFakeCache(), handlerConfig{
		DocCounts:      25,
		MaxDocCounts:   100,
		RequestTimeout: 5 * time.Second,
	})

	router := natsrouter.New(serverNC, "search-service-test")
	router.Use(natsrouter.RequestID())
	h.Register(router)
	// Flush — see setupAppsFixture for the rationale.
	require.NoError(t, serverNC.NatsConn().Flush())
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })

	return &usersFixture{clientNATS: clientNC, thirdParty: stub}
}

func TestIntegration_SearchUsers_Happy(t *testing.T) {
	// Stub returns two users matching the query.
	stubResp := `[{"account":"alice","engName":"Alice Wang"},{"account":"alice2","engName":"Alice Chen"}]`

	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(stubResp))
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: "alice"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var users []model.SearchUser
	require.NoError(t, json.Unmarshal(msg.Data, &users))

	require.Len(t, users, 2)
	assert.Equal(t, "alice", users[0].Account)
	assert.Equal(t, "Alice Wang", users[0].EngName)
}

func TestIntegration_SearchUsers_EmptyQueryReturnsBadRequest(t *testing.T) {
	// Stub should never be called for a bad-request scenario.
	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("third-party stub should not be called for empty query")
		w.WriteHeader(http.StatusInternalServerError)
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}

func TestIntegration_SearchUsers_ThirdPartyErrorReturnsInternal(t *testing.T) {
	// Stub returns a 503 to simulate a backend outage.
	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: "alice"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeInternal, envelope.Code,
		"non-2xx from third-party must surface as internal error, not raw status")
	// Raw third-party details must not leak to the caller.
	assert.NotContains(t, envelope.Error, "503", "status code from third-party must not leak")
}

// --- search.rooms integration ----------------------------------------

// roomsFixture wires a real ES container (for the spotlight index) and
// NATS. search.rooms is served directly from the spotlight index, so no
// Mongo is involved.
type roomsFixture struct {
	clientNATS *nats.Conn
	esURL      string
}

// setupRoomsFixture stands up ES (spotlight index) and NATS. It registers
// t.Cleanup for all containers and returns a ready fixture.
func setupRoomsFixture(t *testing.T) *roomsFixture {
	t.Helper()
	ctx := context.Background()

	// Single ES node — no CCS needed; spotlight is always local.
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.Elasticsearch,
			ExposedPorts: []string{"9200/tcp"},
			Env: map[string]string{
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
				"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
				"cluster.routing.allocation.disk.threshold_enabled": "false",
			},
			WaitingFor: wait.ForAll(
				wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(120*time.Second),
				wait.ForHTTP("/_cluster/health?wait_for_status=yellow&timeout=60s").
					WithPort("9200/tcp").
					WithStartupTimeout(120*time.Second),
			),
		},
		Started: true,
	})
	require.NoError(t, err, "start elasticsearch for subs fixture")
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9200")
	require.NoError(t, err)
	esURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	spotlightIndex := "spotlight-subs-test"
	putTestSpotlightIndex(t, esURL, spotlightIndex)

	natsURL := startNATS(t)
	serverNC, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err, "connect nats (server side)")
	t.Cleanup(func() { _ = serverNC.Drain() })

	clientNC, err := nats.Connect(natsURL)
	require.NoError(t, err, "connect nats (client side)")
	t.Cleanup(func() { clientNC.Close() })

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err, "build searchengine for subs fixture")

	esStore := newESStore(engine, testUserRoomIndex)
	cache := newValkeyCache(newSubsValkeyClient(t))
	h := newHandler(esStore, nil, nil, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		SpotlightReadPattern:    spotlightIndex,
	})

	router := natsrouter.New(serverNC, "search-service-test-subs")
	router.Use(natsrouter.RequestID())
	h.Register(router)
	// Flush — see setupAppsFixture for the rationale.
	require.NoError(t, serverNC.NatsConn().Flush())
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })

	return &roomsFixture{clientNATS: clientNC, esURL: esURL}
}

// newSubsValkeyClient starts a Valkey testcontainer and returns a connected
// client for use by the subs fixture. Reuses the existing startValkey helper.
func newSubsValkeyClient(t *testing.T) valkeyutil.Client {
	t.Helper()
	addr := startValkey(t)
	client, err := valkeyutil.Connect(context.Background(), addr, "")
	require.NoError(t, err, "connect valkey for subs fixture")
	t.Cleanup(func() { valkeyutil.Disconnect(client) })
	return client
}

// putTestSpotlightIndex creates a minimal spotlight index in ES with the
// fields needed by the subscription search query.
func putTestSpotlightIndex(t *testing.T, esURL, index string) {
	t.Helper()
	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
			"refresh_interval":   "1s",
		},
		"mappings": map[string]any{
			"dynamic": false,
			"properties": map[string]any{
				"roomId": map[string]any{"type": "keyword"},
				"roomName": map[string]any{
					"type": "search_as_you_type",
				},
				"roomType":    map[string]any{"type": "keyword"},
				"userAccount": map[string]any{"type": "keyword"},
				"siteId":      map[string]any{"type": "keyword"},
				"joinedAt":    map[string]any{"type": "date"},
			},
		},
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, esURL+"/"+index, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	require.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
		"create spotlight index: status=%d body=%s", resp.StatusCode, b)
}

func TestIntegration_SearchRooms_HappyPath(t *testing.T) {
	f := setupRoomsFixture(t)

	const account = "alice"
	now := time.Now().UTC()

	// Seed spotlight docs for two rooms alice is in.
	seedDoc(t, f.esURL, "spotlight-subs-test", "spot-r1", map[string]any{
		"roomId":      "r1",
		"roomName":    "engineering-announcements",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-48 * time.Hour).Format(time.RFC3339),
	})
	seedDoc(t, f.esURL, "spotlight-subs-test", "spot-r2", map[string]any{
		"roomId":      "r2",
		"roomName":    "engineering-random",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-24 * time.Hour).Format(time.RFC3339),
	})
	// A matching room owned by a different account. With the Mongo
	// hydration removed, the spotlight userAccount term filter is the
	// sole access boundary — this must not leak into alice's results.
	seedDoc(t, f.esURL, "spotlight-subs-test", "spot-r3", map[string]any{
		"roomId":      "r3",
		"roomName":    "engineering-secret",
		"roomType":    "channel",
		"userAccount": "mallory",
		"siteId":      "site-local",
		"joinedAt":    now.Add(-12 * time.Hour).Format(time.RFC3339),
	})

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "engineering"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms(account), reqBytes, 10*time.Second)
	require.NoError(t, err)

	var resp model.SearchRoomsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Rooms, 2, "both rooms matching 'engineering' must be returned")
	byID := map[string]model.SearchRoom{}
	for _, r := range resp.Rooms {
		byID[r.RoomID] = r
	}
	assert.Equal(t, model.SearchRoom{RoomID: "r1", Name: "engineering-announcements", RoomType: "channel", SiteID: "site-local"}, byID["r1"])
	assert.Equal(t, model.SearchRoom{RoomID: "r2", Name: "engineering-random", RoomType: "channel", SiteID: "site-local"}, byID["r2"])
	_, leaked := byID["r3"]
	assert.False(t, leaked, "rooms owned by another account must not leak")
}

func TestIntegration_SearchRooms_RoomTypeChannelFilter(t *testing.T) {
	f := setupRoomsFixture(t)

	const account = "bob"
	now := time.Now().UTC()

	seedDoc(t, f.esURL, "spotlight-subs-test", "spot-b-r1", map[string]any{
		"roomId":      "b-r1",
		"roomName":    "bob-alice",
		"roomType":    "dm",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-1 * time.Hour).Format(time.RFC3339),
	})
	seedDoc(t, f.esURL, "spotlight-subs-test", "spot-b-r2", map[string]any{
		"roomId":      "b-r2",
		"roomName":    "bob-channel",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-2 * time.Hour).Format(time.RFC3339),
	})

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "bob", RoomType: "channel"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms(account), reqBytes, 10*time.Second)
	require.NoError(t, err)

	var resp model.SearchRoomsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Rooms, 1)
	assert.Equal(t, model.SearchRoom{RoomID: "b-r2", Name: "bob-channel", RoomType: "channel", SiteID: "site-local"}, resp.Rooms[0],
		"only the channel room must match roomType=channel filter")
}

func TestIntegration_SearchRooms_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupRoomsFixture(t)

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}

func TestIntegration_SearchRooms_RoomTypeAppReturnsBadRequest(t *testing.T) {
	f := setupRoomsFixture(t)

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "x", RoomType: "app"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
	assert.Contains(t, envelope.Error, "invalid roomType")
}

// --- search.messages v2 integration -----------------------------------------

// messagesV2Fixture stubs ES with a fake HTTP server (httptest). The
// messages path is pure ES — no Mongo round-trip — so no Mongo fixture
// is wired.
type messagesV2Fixture struct {
	clientNATS *nats.Conn
}

func setupMessagesV2Fixture(t *testing.T) *messagesV2Fixture {
	t.Helper()
	ctx := context.Background()

	// Stub ES: always return a canned response containing one hit.
	esStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body so the HTTP/1.1 connection stays open.
		_, _ = io.Copy(io.Discard, r.Body)
		// The Elastic Go client performs a "product check" handshake on
		// connect and rejects any server that doesn't advertise itself
		// as Elasticsearch via this header. Set it on every response so
		// the stub passes the check regardless of which endpoint is hit.
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":1},"hits":[{"_source":{` +
			`"messageId":"m1","roomId":"r1","siteId":"site-a","userId":"u1",` +
			`"userAccount":"alice","content":"hello","createdAt":"2026-04-01T12:00:00Z"}}]}}`))
	}))
	t.Cleanup(esStub.Close)

	// Valkey stub — use the fakeCache wired in-process via handler injection.
	fakeValkey := newFakeCache()
	fakeValkey.store["alice"] = map[string]int64{} // empty restricted map, cache hit

	// NATS
	natsURL := startNATS(t)

	serverNATS, err := natsutil.Connect(natsURL, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverNATS.Drain() })

	clientNATS, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { clientNATS.Close() })

	// Wire search-service with the stub ES engine. No Mongo store needed
	// for the messages path.
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esStub.URL})
	require.NoError(t, err)
	esStore := newESStore(engine, testUserRoomIndex)

	h := newHandler(esStore, nil, nil, fakeValkey, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		UserRoomIndex:           testUserRoomIndex,
		SpotlightReadPattern:    "spotlight-*",
	})

	router := natsrouter.New(serverNATS, "search-service-test-v2")
	router.Use(natsrouter.RequestID())
	h.Register(router)
	// Flush — see setupAppsFixture for the rationale.
	require.NoError(t, serverNATS.NatsConn().Flush())
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })

	return &messagesV2Fixture{clientNATS: clientNATS}
}

func TestIntegration_SearchMessages_V2_HitProjection(t *testing.T) {
	f := setupMessagesV2Fixture(t)

	reqBytes, err := json.Marshal(model.SearchMessagesRequest{Query: "hello"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchMessages("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var resp model.SearchMessagesResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Messages, 1)
	assert.EqualValues(t, 1, resp.Total)

	got := resp.Messages[0]
	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, "r1", got.RoomID)
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, "alice", got.UserAccount)
	assert.Equal(t, "hello", got.Content)
}

func TestIntegration_SearchMessages_V2_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupMessagesV2Fixture(t)

	reqBytes, err := json.Marshal(model.SearchMessagesRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchMessages("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}
