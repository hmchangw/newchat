package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newChatsTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-chats", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestChats(tokenURL, baseURL string) ChatsReader {
	return NewChatsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

var (
	chatsFrom = time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	chatsTo   = time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
)

func TestListUserChats_Success_QueryShape(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-chats", r.Header.Get("Authorization"))
		assert.Equal(t, "/users/aad-user-1/chats", r.URL.Path)
		q := r.URL.Query()
		assert.Equal(t,
			"lastUpdatedDateTime gt 2026-04-01T00:00:00Z and lastUpdatedDateTime lt 2026-07-14T00:00:00Z",
			q.Get("$filter"))
		assert.Equal(t, "members", q.Get("$expand"))
		assert.Equal(t, "id,chatType,topic,createdDateTime,lastUpdatedDateTime", q.Get("$select"))
		_, _ = w.Write([]byte(`{"value":[{
			"id":"19:chat1@thread.v2","chatType":"group","topic":"Project X",
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z",
			"members":[
				{"@odata.type":"#microsoft.graph.aadUserConversationMember","userId":"aad-user-1","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"},
				{"@odata.type":"#microsoft.graph.aadUserConversationMember","userId":"aad-user-2","visibleHistoryStartDateTime":"0001-01-01T00:00:00Z"}
			]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	require.Len(t, chats, 1)
	assert.Equal(t, "19:chat1@thread.v2", chats[0].ID)
	assert.Equal(t, "group", chats[0].ChatType)
	assert.Equal(t, "Project X", chats[0].Topic)
	assert.Equal(t, time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC), chats[0].CreatedDateTime)
	require.Len(t, chats[0].Members, 2)
	assert.Equal(t, "aad-user-1", chats[0].Members[0].UserID)
	assert.Equal(t, "aad-user-2", chats[0].Members[1].UserID)
}

func TestListUserChats_NullTopicBecomesEmpty(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":[{
			"id":"19:one1","chatType":"oneOnOne","topic":null,
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z",
			"members":[{"userId":"aad-user-1","visibleHistoryStartDateTime":null}]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	require.Len(t, chats, 1)
	assert.Equal(t, "", chats[0].Topic)
	assert.True(t, chats[0].Members[0].VisibleHistoryStartDateTime.IsZero())
}

func TestListUserChats_FollowsNextLink(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("$skiptoken") == "" {
			_, _ = fmt.Fprintf(w, `{"value":[{"id":"19:p1","chatType":"group","topic":"a",
				"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z","members":[]}],
				"@odata.nextLink":"%s/users/aad-user-1/chats?$skiptoken=page2"}`, graphSrv.URL)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"id":"19:p2","chatType":"group","topic":"b",
			"createdDateTime":"2026-04-03T08:00:00Z","lastUpdatedDateTime":"2026-07-02T09:00:00Z","members":[]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, chats, 2)
	assert.Equal(t, "19:p1", chats[0].ID)
	assert.Equal(t, "19:p2", chats[1].ID)
}

func TestListUserChats_RetriesOn429(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"id":"19:ok","chatType":"group","topic":"a",
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z","members":[]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, chats, 1)
}

func TestListUserChats_429Exhausted(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
	assert.Equal(t, 4, calls, "chatsMaxAttempts requests then give up")
}

func TestListUserChats_GraphError(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"nope"}}`))
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
	assert.Contains(t, err.Error(), "Forbidden")
	assert.NotContains(t, err.Error(), "nope", "raw Graph message must not be surfaced")
}

func TestListUserChats_TokenError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer tokenSrv.Close()

	_, err := newTestChats(tokenSrv.URL, "http://unused.invalid").
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire graph token")
}

func TestListUserChats_EmptyUserID(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	_, err := newTestChats(tokenSrv.URL, "http://unused.invalid").
		ListUserChats(context.Background(), "", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userID is required")
}

// --- tenant-wide throttle gate ---

func newGateClient(t *testing.T) *graphClient {
	t.Helper()
	return New(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"}).(*graphClient)
}

func TestThrottleGate_NoGateReturnsImmediately(t *testing.T) {
	g := newGateClient(t)
	start := time.Now()
	require.NoError(t, g.waitThrottle(context.Background()))
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestThrottleGate_ArmedGateBlocks(t *testing.T) {
	g := newGateClient(t)
	g.noteThrottle("1")
	start := time.Now()
	require.NoError(t, g.waitThrottle(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond,
		"waitThrottle must wait out the armed gate")
}

func TestThrottleGate_ExtendsMonotonically(t *testing.T) {
	g := newGateClient(t)
	g.noteThrottle("2")
	until := g.throttleDeadline()
	g.noteThrottle("0") // a later, shorter Retry-After must not shrink the gate
	assert.True(t, g.throttleDeadline().Equal(until), "gate must never shrink")
	g.noteThrottle("3")
	assert.True(t, g.throttleDeadline().After(until), "longer Retry-After extends the gate")
}

func TestThrottleGate_CapsHostileHeader(t *testing.T) {
	g := newGateClient(t)
	g.noteThrottle("86400") // 24h — must be capped to chatsMaxThrottleWait
	remaining := time.Until(g.throttleDeadline())
	assert.LessOrEqual(t, remaining, chatsMaxThrottleWait)
	assert.Greater(t, remaining, chatsMaxThrottleWait-time.Minute)
}

func TestThrottleGate_CtxCancelAborts(t *testing.T) {
	g := newGateClient(t)
	g.noteThrottle("30")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := g.waitThrottle(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second, "cancel must abort the wait early")
}

func TestListUserChats_429ArmsGateForNextCall(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	g := NewChatsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	).(*graphClient)

	start := time.Now()
	_, err := g.ListUserChats(context.Background(), "u1", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond,
		"the retry must have waited out the gate armed by the 429")

	// The gate is client-wide: after the successful retry it has expired, so a
	// second user's call goes straight through.
	start = time.Now()
	_, err = g.ListUserChats(context.Background(), "u2", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.Equal(t, 3, calls)
}
