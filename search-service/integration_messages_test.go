//go:build integration

package main

// Integration tests for search.messages v2 (ES stubbed via httptest, shared NATS).

import (
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

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

type messagesV2Fixture struct {
	clientNATS *nats.Conn
}

func setupMessagesV2Fixture(t *testing.T) *messagesV2Fixture {
	t.Helper()
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
			`"userAccount":"alice","content":"hello","createdAt":"2026-04-01T12:00:00Z",` +
			`"attachmentText":"q3.pdf numbers",` +
			`"attachments":[{"id":"f1","title":"q3.pdf","type":"file","description":"numbers",` +
			`"titleLink":"api/v1/file/rooms/r1/file/f1","titleLinkDownload":true,"fileType":"application/pdf"}],` +
			`"card":{"template":"expense-v1","data":"eyJhbW91bnQiOjQyfQ=="}}}]}}`))
	}))
	t.Cleanup(esStub.Close)

	fakeValkey := newFakeCache()
	fakeValkey.store["alice"] = map[string]int64{} // empty restricted map, cache hit

	engine, err := searchengine.New(context.Background(), searchengine.Config{Backend: "elasticsearch", URL: esStub.URL})
	require.NoError(t, err)
	h := newHandler(newESStore(engine, testUserRoomIndex), nil, nil, fakeValkey, &handlerConfig{
		SiteID:                  testSiteID,
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		UserRoomIndex:           testUserRoomIndex,
		SpotlightReadPattern:    "spotlight-*",
	})
	clientNATS := setupRouter(t, testQueueGroupV2, h.Register)
	return &messagesV2Fixture{clientNATS: clientNATS}
}

func TestIntegration_SearchMessages_V2_HitProjection(t *testing.T) {
	f := setupMessagesV2Fixture(t)

	reqBytes, err := json.Marshal(model.SearchMessagesRequest{Query: "hello"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchMessages("alice", testSiteID), reqBytes, 5*time.Second)
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
	require.Len(t, got.Attachments, 1)
	assert.Equal(t, "q3.pdf", got.Attachments[0].Title)
	assert.Equal(t, "numbers", got.Attachments[0].Description)
	assert.Equal(t, "application/pdf", got.Attachments[0].FileType)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f1", got.Attachments[0].TitleLink)
	require.NotNil(t, got.Card)
	assert.Equal(t, "expense-v1", got.Card.Template)
	assert.Equal(t, []byte(`{"amount":42}`), got.Card.Data)
}

// Queue group for the real-ES edit/delete propagation fixture — isolated so a
// slow drain can't deliver to a sibling test's handler.
const testQueueGroupDelCheck = "search-service-test-delcheck"

// End-to-end vs real ES: after the worker's externally-versioned write sequence,
// search finds only edited content, never deleted docs; stale replays can't resurrect.
func TestIntegration_SearchMessages_EditedAndDeletedDocs(t *testing.T) {
	esURL := testutil.Elasticsearch(t)
	ctx := context.Background()

	const (
		index   = "messages-delcheck-v1-2026-01"
		account = "delcheck-alice"
		roomID  = "room-delcheck"
	)

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	// roomId must map keyword (dynamic text breaks the terms-lookup room gate);
	// the fixture-scoped pattern keeps it from governing sibling tests.
	require.NoError(t, engine.UpsertTemplate(ctx, "messages-delcheck-template", messageTestTemplate("messages-delcheck-*")))
	t.Cleanup(func() {
		targets := []string{
			"/" + index,
			"/_index_template/messages-delcheck-template",
			fmt.Sprintf("/%s/_doc/%s?refresh=true", testUserRoomIndex, account),
		}
		for _, target := range targets {
			req, _ := http.NewRequest(http.MethodDelete, esURL+target, nil)
			resp, delErr := testHTTPClient.Do(req)
			if delErr == nil {
				resp.Body.Close()
			}
		}
	})

	// Room access: terms-lookup resolves the caller's rooms from this doc.
	seedDoc(t, esURL, testUserRoomIndex, account, map[string]any{
		"userAccount": account,
		"rooms":       []string{roomID},
	})

	msgDoc := func(id, content string, extra map[string]any) json.RawMessage {
		doc := map[string]any{
			"messageId": id, "roomId": roomID, "siteId": testSiteID,
			"userId": "u-del", "userAccount": account,
			"content": content, "createdAt": "2026-01-15T10:30:00Z",
		}
		for k, v := range extra {
			doc[k] = v
		}
		data, mErr := json.Marshal(doc)
		require.NoError(t, mErr)
		return data
	}
	mustBulk := func(actions ...searchengine.BulkAction) []searchengine.BulkResult {
		results, bErr := engine.Bulk(ctx, actions)
		require.NoError(t, bErr)
		return results
	}
	okBulk := func(actions ...searchengine.BulkAction) {
		for _, res := range mustBulk(actions...) {
			require.Less(t, res.Status, 300, "bulk write failed: %+v", res)
		}
	}

	// Worker write sequence: m1 created (v1000) then edited (replace, v2000,
	// +attachment); m2 created (v1000) then deleted (v2000).
	okBulk(searchengine.BulkAction{Action: searchengine.ActionIndex, Index: index, DocID: "m1", Version: 1000,
		Doc: msgDoc("m1", "original words before edit", nil)})
	okBulk(searchengine.BulkAction{Action: searchengine.ActionIndex, Index: index, DocID: "m2", Version: 1000,
		Doc: msgDoc("m2", "secret that must vanish", nil)})
	okBulk(searchengine.BulkAction{Action: searchengine.ActionIndex, Index: index, DocID: "m1", Version: 2000,
		Doc: msgDoc("m1", "edited words after edit", map[string]any{
			"editedAt": "2026-01-15T11:00:00Z", "updatedAt": "2026-01-15T11:00:00Z",
			"attachmentText": "q3-report.pdf",
			"attachments": []map[string]any{{
				"id": "f1", "title": "q3-report.pdf", "type": "file",
				"titleLink":         "api/v1/file/rooms/" + roomID + "/file/f1",
				"titleLinkDownload": true, "fileType": "application/pdf",
			}},
		})})
	okBulk(searchengine.BulkAction{Action: searchengine.ActionDelete, Index: index, DocID: "m2", Version: 2000})

	// Stale redelivery of m2's create (older external version) must be
	// version-rejected, not resurrect the doc.
	staleResults := mustBulk(searchengine.BulkAction{Action: searchengine.ActionIndex, Index: index, DocID: "m2", Version: 1000,
		Doc: msgDoc("m2", "secret that must vanish", nil)})
	require.Len(t, staleResults, 1)
	require.Equal(t, http.StatusConflict, staleResults[0].Status, "stale replay must hit the external-version tombstone")

	refreshReq, err := http.NewRequest(http.MethodPost, esURL+"/"+index+"/_refresh", nil)
	require.NoError(t, err)
	refreshResp, err := testHTTPClient.Do(refreshReq)
	require.NoError(t, err)
	refreshResp.Body.Close()

	// Real handler + real ES store + real NATS RPC.
	fakeValkey := newFakeCache()
	fakeValkey.store[account] = map[string]int64{} // no restricted rooms, cache hit
	h := newHandler(newESStore(engine, testUserRoomIndex), nil, nil, fakeValkey, &handlerConfig{
		SiteID:                  testSiteID,
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            10 * 365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		UserRoomIndex:           testUserRoomIndex,
		SpotlightReadPattern:    "spotlight-*",
	})
	clientNATS := setupRouter(t, testQueueGroupDelCheck, h.Register)

	search := func(query string) model.SearchMessagesResponse {
		reqBytes, mErr := json.Marshal(model.SearchMessagesRequest{Query: query})
		require.NoError(t, mErr)
		msg, rErr := clientNATS.Request(subject.SearchMessages(account, testSiteID), reqBytes, 5*time.Second)
		require.NoError(t, rErr)
		var resp model.SearchMessagesResponse
		require.NoError(t, json.Unmarshal(msg.Data, &resp))
		return resp
	}

	t.Run("edited message found only with updated content", func(t *testing.T) {
		resp := search("edited words")
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "m1", resp.Messages[0].MessageID)
		assert.Equal(t, "edited words after edit", resp.Messages[0].Content)
		require.NotNil(t, resp.Messages[0].EditedAt)
	})

	t.Run("pre-edit content is gone", func(t *testing.T) {
		resp := search("original words")
		assert.Empty(t, resp.Messages)
		assert.Zero(t, resp.Total)
	})

	t.Run("deleted message never surfaces, even after stale replay", func(t *testing.T) {
		resp := search("secret")
		assert.Empty(t, resp.Messages)
		assert.Zero(t, resp.Total)
	})

	t.Run("attachment filename matches and hit carries the full object", func(t *testing.T) {
		resp := search("q3")
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "m1", resp.Messages[0].MessageID)
		require.Len(t, resp.Messages[0].Attachments, 1)
		att := resp.Messages[0].Attachments[0]
		assert.Equal(t, "q3-report.pdf", att.Title)
		assert.Equal(t, "application/pdf", att.FileType)
		assert.Equal(t, "api/v1/file/rooms/"+roomID+"/file/f1", att.TitleLink)
	})
}

func TestIntegration_SearchMessages_V2_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupMessagesV2Fixture(t)

	reqBytes, err := json.Marshal(model.SearchMessagesRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchMessages("alice", testSiteID), reqBytes, 5*time.Second)
	require.NoError(t, err)

	errtest.AssertCode(t, msg.Data, errcode.CodeBadRequest)
}
