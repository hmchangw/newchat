//go:build integration

package main

// CCS integration tests + helpers only CCS uses. The two CCS tests are
// the exception to the shared-container pattern in setup_shared_test.go:
// they need a pair of ES nodes on a shared docker network with
// transport-port aliases. NATS and Valkey are still shared.

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// --- Fixture -----------------------------------------------------------------

// ccsFixture owns the two-ES + Valkey + NATS stack for CCS tests.
// localURL / remoteURL are host-mapped for seeding; the service sees localURL.
type ccsFixture struct {
	localURL   string
	remoteURL  string
	localES    searchengine.SearchEngine
	remoteES   searchengine.SearchEngine
	clientNATS *nats.Conn
}

// setupCCSFixture owns the pair of networked ES containers (can't be
// process-shared — they need a shared docker network with transport-port
// aliases); piggybacks on shared Valkey/NATS.
func setupCCSFixture(t *testing.T) *ccsFixture {
	t.Helper()
	ctx := context.Background()

	nw, err := network.New(ctx)
	require.NoError(t, err, "create docker network")
	t.Cleanup(func() { _ = nw.Remove(ctx) })

	remoteURL := startESForCCS(t, nw, "es-remote", "remote-cluster")
	localURL := startESForCCS(t, nw, "es-local", "local-cluster")

	// Wire local→remote in PROXY mode. Proxy mode skips sniff-then-reconnect,
	// which requires the remote to advertise a reachable publish address —
	// fragile when containers bind transport on 0.0.0.0 and publish defaults
	// to an unreachable interface. Ref: ES "Remote cluster settings" → `mode=proxy`.
	putClusterSetting(t, localURL, map[string]any{
		"persistent": map[string]any{
			"cluster.remote.remote1.mode":          "proxy",
			"cluster.remote.remote1.proxy_address": "es-remote:9300",
		},
	})
	waitForRemoteConnected(t, localURL, "remote1", 120*time.Second)

	localEngine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: localURL})
	require.NoError(t, err, "build searchengine for local")
	remoteEngine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: remoteURL})
	require.NoError(t, err, "build searchengine for remote")

	cacheClient := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	h := newHandler(newESStore(localEngine, testUserRoomIndex), nil, nil, newValkeyCache(cacheClient), handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		UserRoomIndex:           testUserRoomIndex,
		SpotlightReadPattern:    "spotlight-test-*",
	})
	clientNC := setupRouter(t, testQueueGroup, h.Register)

	return &ccsFixture{
		localURL:   localURL,
		remoteURL:  remoteURL,
		localES:    localEngine,
		remoteES:   remoteEngine,
		clientNATS: clientNC,
	}
}

// startESForCCS starts one ES node on the shared network at alias `{alias}`.
// transport.host=0.0.0.0 is required so the transport port binds on the bridge
// network (ES 8.x defaults to `_site_` which excludes the container bridge IP).
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
				"ES_JAVA_OPTS": "-Xms256m -Xmx256m",
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

// --- Index templates ---------------------------------------------------------

// buildTestTemplate wraps properties with single-node-friendly settings
// (1 shard, 0 replicas) so tests don't depend on search-sync-worker's
// analyzer config.
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

// --- CCS HTTP helpers --------------------------------------------------------

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

// --- Templates on both clusters ---------------------------------------------

func (f *ccsFixture) installTemplates(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, f.localES.UpsertTemplate(ctx, "messages_template", messageTestTemplate()),
		"upsert messages_template on local")
	require.NoError(t, f.remoteES.UpsertTemplate(ctx, "messages_template", messageTestTemplate()),
		"upsert messages_template on remote")
	// user-room is local-only per the search-service architecture.
	require.NoError(t, f.localES.UpsertTemplate(ctx, "user_room_template", userRoomTestTemplate()),
		"upsert user_room_template on local")
}

// --- Tests -------------------------------------------------------------------

// TestSearchService_SearchMessages_CCS_CrossCluster_Unrestricted verifies
// the core CCS promise: a user's search crosses from the local cluster
// (`messages-*`) to a remote cluster (`*:messages-*`) and the service
// returns the merged result set. Both rooms are unrestricted — they live in
// the user-room doc's `rooms[]` — and the terms-lookup clause handles them
// uniformly regardless of which site hosts the message.
func TestSearchService_SearchMessages_CCS_CrossCluster_Unrestricted(t *testing.T) {
	f := setupCCSFixture(t)
	f.installTemplates(t)

	// Alice is in two unrestricted rooms (one local, one remote); the
	// local user-room doc lists both. Sync-worker normally populates it via
	// INBOX events — seeded directly here.
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

	seedDoc(t, f.localURL, monthIdx, "msg-local-1", map[string]any{
		"messageId":   "msg-local-1",
		"roomId":      localRoomID,
		"siteId":      "site-local",
		"userId":      "user-bob",
		"userAccount": "bob",
		"content":     "hello from local",
		"createdAt":   createdAt.Format(time.RFC3339Nano),
	})

	// Same index pattern on the remote cluster — CCS resolves `*:messages-*`.
	seedDoc(t, f.remoteURL, monthIdx, "msg-remote-1", map[string]any{
		"messageId":   "msg-remote-1",
		"roomId":      remoteRoomID,
		"siteId":      "site-remote",
		"userId":      "user-carol",
		"userAccount": "carol",
		"content":     "hello from remote",
		"createdAt":   createdAt.Format(time.RFC3339Nano),
	})

	req := model.SearchMessagesRequest{Query: "hello"}
	reqData, err := json.Marshal(req)
	require.NoError(t, err)

	// Long timeout: first request is Valkey miss → ES prefetch → CCS fanout.
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

	// hss is the user's join-time bound for the restricted remote room;
	// preHSS / postHSS straddle it. All within the 1-year recent_window.
	now := time.Now().UTC()
	hss := now.Add(-2 * time.Hour)
	preHSS := hss.Add(-3 * time.Hour)
	postHSS := hss.Add(time.Hour)
	monthIdxFor := func(ts time.Time) string { return "messages-" + ts.Format("2006-01") }

	// user-room doc: local room unrestricted, remote room restricted with hss.
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
	seedDoc(t, f.remoteURL, monthIdxFor(preHSS), "msg-remote-pre-parent", map[string]any{
		"messageId":   "msg-remote-pre-parent",
		"roomId":      remoteRoomID,
		"siteId":      "site-remote",
		"userId":      "user-carol",
		"userAccount": "carol",
		"content":     "hello pre-hss parent",
		"createdAt":   preHSS.Format(time.RFC3339Nano),
	})

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
