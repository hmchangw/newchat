package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMembersTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-mem", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestMembers(tokenURL, baseURL string, opts ...Option) ChatMembersReader {
	all := append([]Option{WithTokenURL(tokenURL), WithBaseURL(baseURL)}, opts...)
	return NewChatMembersClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"}, all...)
}

func TestListChatMembers_Success_QueryShape(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-mem", r.Header.Get("Authorization"))
		assert.Equal(t, "/chats/19:abc@thread.v2/members", r.URL.Path)
		q := r.URL.Query()
		assert.Equal(t, "userId,userPrincipalName,visibleHistoryStartDateTime", q.Get("$select"))
		assert.Equal(t, "50", q.Get("$top"), "default page size")
		_, _ = w.Write([]byte(`{"value":[
			{"userId":"u1","userPrincipalName":"Alice@corp.example","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"},
			{"userId":"u2","userPrincipalName":null,"visibleHistoryStartDateTime":"0001-01-01T00:00:00Z"}
		]}`))
	}))
	defer graphSrv.Close()

	members, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "u1", members[0].UserID)
	assert.Equal(t, "Alice@corp.example", members[0].UserPrincipalName)
	assert.Equal(t, "u2", members[1].UserID)
	assert.Equal(t, "", members[1].UserPrincipalName, "null UPN decodes to empty")
	assert.True(t, members[1].VisibleHistoryStartDateTime.IsZero())
}

func TestListChatMembers_CustomPageSize(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "10", r.URL.Query().Get("$top"))
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL, WithMembersPageSize(10)).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
}

func TestListChatMembers_FollowsNextLink(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("$skiptoken") == "" {
			_, _ = fmt.Fprintf(w, `{"value":[{"userId":"u1","userPrincipalName":"a@x","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"}],
				"@odata.nextLink":"%s/chats/19:abc@thread.v2/members?$skiptoken=page2"}`, graphSrv.URL)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"userId":"u2","userPrincipalName":"b@x","visibleHistoryStartDateTime":"2026-04-03T08:00:00Z"}]}`))
	}))
	defer graphSrv.Close()

	members, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, members, 2)
	assert.Equal(t, "u1", members[0].UserID)
	assert.Equal(t, "u2", members[1].UserID)
}

func TestListChatMembers_RetriesOn429(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestListChatMembers_GraphError(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"nope"}}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
	assert.Contains(t, err.Error(), "Forbidden")
	assert.NotContains(t, err.Error(), "nope", "raw Graph message must not be surfaced")
}

func TestListChatMembers_EmptyChatID(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	_, err := newTestMembers(tokenSrv.URL, "http://unused.invalid").
		ListChatMembers(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chatID is required")
}
