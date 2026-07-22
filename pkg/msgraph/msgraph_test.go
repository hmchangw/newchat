package msgraph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestClient wires a graphClient at the given token + graph servers.
func newTestClient(tokenURL, baseURL string) Client {
	return New(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

// newTestDirectory wires a DirectoryReader at the given token + graph servers.
func newTestDirectory(tokenURL, baseURL string) DirectoryReader {
	return NewDirectoryClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

func TestCreateOnlineMeeting_Success(t *testing.T) {
	var tokenCalls, meetingCalls int

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls++
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "client_credentials", r.Form.Get("grant_type"))
		assert.Equal(t, graphScope, r.Form.Get("scope"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-123", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meetingCalls++
		assert.Equal(t, "Bearer tok-123", r.Header.Get("Authorization"))
		// Idempotent endpoint: the organizer-scoped createOrGet path.
		assert.True(t, strings.Contains(r.URL.Path, "/users/alice%40corp.com/onlineMeetings/createOrGet") ||
			strings.Contains(r.URL.Path, "/users/alice@corp.com/onlineMeetings/createOrGet"),
			"organizer-scoped createOrGet path expected, got %s", r.URL.Path)
		// externalId is the per-room idempotency key and must be sent.
		var body onlineMeetingPayload
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "room-key-1", body.ExternalID, "externalId must be sent to createOrGet")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	m, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "room-key-1", Subject: "Standup", OrganizerEmail: "alice@corp.com", AttendeeEmails: []string{"bob@corp.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "m1", m.ID)
	assert.Equal(t, "https://join/1", m.JoinURL)

	// Second call reuses the cached token (no second token fetch).
	_, err = c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "room-key-1", OrganizerEmail: "alice@corp.com"})
	require.NoError(t, err)
	assert.Equal(t, 1, tokenCalls, "token should be cached across calls")
	assert.Equal(t, 2, meetingCalls)
}

// TestCreateOnlineMeeting_Idempotent_SameExternalID asserts the client hits
// createOrGet and that a repeat call with the same externalId returns the same
// meeting Graph already holds for that key (Graph is the idempotency source of
// truth — the server returns the existing meeting on the second createOrGet).
func TestCreateOnlineMeeting_Idempotent_SameExternalID(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	defer tokenSrv.Close()

	// Server mimics Graph createOrGet: one meeting per externalId, returned on
	// every call with that key.
	byExternalID := map[string]OnlineMeeting{}
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/onlineMeetings/createOrGet", "must use createOrGet endpoint")
		var body onlineMeetingPayload
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.NotEmpty(t, body.ExternalID, "externalId required")
		m, ok := byExternalID[body.ExternalID]
		if !ok {
			m = OnlineMeeting{ID: "mtg-" + body.ExternalID, JoinURL: "https://join/" + body.ExternalID}
			byExternalID[body.ExternalID] = m
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusOK) // existing meeting returned
		}
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	first, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "k", OrganizerEmail: "a@b.com",
	})
	require.NoError(t, err)
	second, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "k", OrganizerEmail: "a@b.com",
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID, "same externalId returns the same meeting")
	assert.Equal(t, first.JoinURL, second.JoinURL)
	assert.Len(t, byExternalID, 1, "only one meeting created for one externalId")
}

// TestCreateOnlineMeeting_RequiresExternalID guards the createOrGet contract:
// an empty externalId is rejected before any network call.
func TestCreateOnlineMeeting_RequiresExternalID(t *testing.T) {
	c := newTestClient("http://unused", "http://unused")
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{OrganizerEmail: "a@b.com"}) // no ExternalID
	require.Error(t, err)
	assert.Contains(t, err.Error(), "externalId")
}

func TestCreateOnlineMeeting_TokenError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(tokenResponse{Error: "invalid_client", ErrorDesc: "bad secret"}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	defer tokenSrv.Close()

	c := New(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "super-secret-value"},
		WithTokenURL(tokenSrv.URL), WithBaseURL("http://unused"),
	)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerEmail: "a@b.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_client")
	// Never leak the secret in the error.
	assert.NotContains(t, err.Error(), "super-secret-value")
}

func TestCreateOnlineMeeting_GraphError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		// The message carries sensitive-looking detail that must NOT leak into the error.
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"secret-internal-detail-xyz"}}`))
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerEmail: "a@b.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
	assert.Contains(t, err.Error(), "Forbidden", "sanitized error code should be surfaced")
	assert.NotContains(t, err.Error(), "secret-internal-detail-xyz", "raw response message must not leak")
}

func TestCreateOnlineMeeting_MissingJoinURL(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1"}) // no joinWebUrl
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerEmail: "a@b.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "joinWebUrl")
}

func TestNew_TLSInsecureSkipVerify(t *testing.T) {
	// Default: no custom transport, so the stdlib default (verifying) is used.
	def := New(Config{TenantID: "t"}).(*graphClient)
	assert.Nil(t, def.httpClient.Transport, "default client must keep TLS verification")

	// Enabled: transport carries InsecureSkipVerify.
	ins := New(Config{TenantID: "t", TLSInsecureSkipVerify: true}).(*graphClient)
	tr, ok := ins.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.TLSClientConfig)
	assert.True(t, tr.TLSClientConfig.InsecureSkipVerify)
}

func TestResolveAccountIDs_BatchesAndKeysByAccount(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		q := r.URL.Query()
		// startsWith is served as an advanced query: eventual consistency + $count
		// satisfy Graph's advanced-query contract. $top stays off; mail is not
		// selected.
		assert.Equal(t, "eventual", r.Header.Get("ConsistencyLevel"))
		assert.Equal(t, "true", q.Get("$count"))
		assert.Empty(t, q.Get("$top"))
		assert.Equal(t, "id,userPrincipalName", q.Get("$select"), "mail is not selected")
		filter := q.Get("$filter")
		// Domain-agnostic prefix match; both lower- and upper-cased variants OR'd.
		assert.Contains(t, filter, "startsWith(userPrincipalName,'alice@')")
		assert.Contains(t, filter, "startsWith(userPrincipalName,'ALICE@')")
		assert.Contains(t, filter, "startsWith(userPrincipalName,'bob@')")
		assert.Contains(t, filter, " or ")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
			{ID: "ida", UserPrincipalName: "Alice@corp.com"}, // mixed-case UPN
			{ID: "idb", UserPrincipalName: "bob@partner.io"}, // different domain
		}})
	}))
	defer graphSrv.Close()

	c := newTestDirectory(tokenSrv.URL, graphSrv.URL)
	got, err := c.ResolveAccountIDs(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	// Keyed by account (lowercased UPN local-part), so mixed-case UPN still maps.
	assert.Equal(t, map[string]string{"alice": "ida", "bob": "idb"}, got)
}

func TestResolveAccountIDs_SkipsUnrequestedAndDupes(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
			{ID: "ida1", UserPrincipalName: "alice@corp.com"},
			{ID: "ida2", UserPrincipalName: "alice@partner.io"}, // same local-part -> first wins
			{ID: "idx", UserPrincipalName: "stranger@corp.com"}, // not requested -> skipped
		}})
	}))
	defer graphSrv.Close()

	c := newTestDirectory(tokenSrv.URL, graphSrv.URL)
	got, err := c.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida1"}, got)
}

func TestResolveAccountIDs_ChunksLargeInput(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{}})
	}))
	defer graphSrv.Close()

	accounts := make([]string, maxAccountsPerQuery+1) // one over a chunk -> 2 requests
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%d", i)
	}
	c := newTestDirectory(tokenSrv.URL, graphSrv.URL)
	_, err := c.ResolveAccountIDs(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "accounts beyond one chunk trigger a second query")
}

func TestResolveAccountIDs_Empty(t *testing.T) {
	c := NewDirectoryClient(Config{TenantID: "t"})
	got, err := c.ResolveAccountIDs(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCasedVariants(t *testing.T) {
	assert.Equal(t, []string{"alice", "ALICE"}, casedVariants("alice"))
	assert.Equal(t, []string{"alice", "ALICE"}, casedVariants("Alice"))
	assert.Equal(t, []string{"123"}, casedVariants("123"), "caseless value -> single clause")
}

func TestLocalPart(t *testing.T) {
	cases := []struct {
		name, in, want string
		ok             bool
	}{
		{"upn", "alice@corp.com", "alice", true},
		{"mixed", "Alice@corp.com", "Alice", true},
		{"no at", "nodomain", "", false},
		{"leading at", "@corp.com", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := localPart(tc.in)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.ok, ok)
		})
	}
}
func TestListUsers_MultiPageFollowsNextLink(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var requests []string
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.String())
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"value":[{"id":"u3","userPrincipalName":"carol@corp.example","displayName":"Carol Jones"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"value":[` +
			`{"id":"u1","userPrincipalName":"alice@corp.example","displayName":"Alice Smith"},` +
			`{"id":"u2","userPrincipalName":"bob@corp.example","displayName":"Bob Wu"}],` +
			`"@odata.nextLink":"` + graphSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister, err := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	require.NoError(t, err)

	var pages [][]GraphUser
	err = lister.ListUsers(context.Background(), 500, func(users []GraphUser) error {
		pages = append(pages, users)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, pages, 2)
	assert.Equal(t, []GraphUser{
		{ID: "u1", UserPrincipalName: "alice@corp.example", DisplayName: "Alice Smith"},
		{ID: "u2", UserPrincipalName: "bob@corp.example", DisplayName: "Bob Wu"},
	}, pages[0])
	assert.Equal(t, []GraphUser{{ID: "u3", UserPrincipalName: "carol@corp.example", DisplayName: "Carol Jones"}}, pages[1])

	// first request carries $top and $select
	require.NotEmpty(t, requests)
	first, err := url.Parse(requests[0])
	require.NoError(t, err)
	assert.Equal(t, "500", first.Query().Get("$top"))
	assert.Equal(t, "id,userPrincipalName,displayName", first.Query().Get("$select"))
}

func TestListUsers_CallbackErrorAbortsWalk(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"value":[{"id":"u1","userPrincipalName":"a@x"}],` +
			`"@odata.nextLink":"` + graphSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister, err := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	require.NoError(t, err)

	err = lister.ListUsers(context.Background(), 500, func([]GraphUser) error {
		return errors.New("boom")
	})
	require.ErrorContains(t, err, "boom")
	assert.Equal(t, 1, calls, "must not fetch further pages after fn error")
}

func TestListUsers_Non200IsError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer graphSrv.Close()

	lister, err := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	require.NoError(t, err)

	err = lister.ListUsers(context.Background(), 500, func([]GraphUser) error { return nil })
	require.ErrorContains(t, err, "status 403")
}

func TestListUsers_RejectsCrossOriginNextLink(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var attackerHit bool
	attackerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attackerHit = true
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer attackerSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Point the next page at a foreign origin — a tampered/intercepted nextLink.
		_, _ = w.Write([]byte(`{"value":[{"id":"u1","userPrincipalName":"a@x"}],` +
			`"@odata.nextLink":"` + attackerSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister, err := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	require.NoError(t, err)

	var pages [][]GraphUser
	err = lister.ListUsers(context.Background(), 500, func(users []GraphUser) error {
		pages = append(pages, users)
		return nil
	})
	require.ErrorContains(t, err, "deviates from configured graph origin")
	assert.False(t, attackerHit, "must not forward the bearer token to a foreign origin")
	assert.Len(t, pages, 1, "first (valid) page is still delivered before the walk aborts")
}

// TestGraphClients_InvalidProxyURL asserts every app-only constructor that
// honors ProxyURL fails fast at construction on a malformed value, rather than
// silently falling back to direct egress or surfacing an opaque per-request error.
func TestGraphClients_InvalidProxyURL(t *testing.T) {
	for _, proxy := range []string{"://nope", "proxy.corp:8080", "http://"} {
		t.Run(proxy, func(t *testing.T) {
			cfg := Config{TenantID: "t", ProxyURL: proxy}
			_, err := NewChatsClient(cfg)
			require.Error(t, err)
			_, err = NewChatMembersClient(cfg)
			require.Error(t, err)
			_, err = NewUserListerClient(cfg)
			require.Error(t, err)
		})
	}
}

func TestCreateOnlineMeeting_SendsDefaultUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "token request must carry User-Agent")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "meeting request must carry User-Agent")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID:     "room-key-1",
		Subject:        "Standup",
		OrganizerEmail: "alice@corp.com",
	})
	require.NoError(t, err)
}

func TestCreateOnlineMeeting_UserAgentOverride(t *testing.T) {
	const custom = "chat-room-service/9.9"
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	c := New(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", UserAgent: custom},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "room-key-1", OrganizerEmail: "alice@corp.com",
	})
	require.NoError(t, err)
}

func TestResolveAccountIDs_SendsUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "directory request must carry User-Agent")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
			{ID: "ida", UserPrincipalName: "alice@corp.com"},
		}})
	}))
	defer graphSrv.Close()

	c := newTestDirectory(tokenSrv.URL, graphSrv.URL)
	_, err := c.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
}

func TestListUsers_SendsUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "list-users request must carry User-Agent")
		_, _ = w.Write([]byte(`{"value":[{"id":"u1","userPrincipalName":"alice@corp.example"}]}`))
	}))
	defer graphSrv.Close()

	lister, err := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	require.NoError(t, err)
	err = lister.ListUsers(context.Background(), 500, func([]GraphUser) error { return nil })
	require.NoError(t, err)
}

func TestWithMaxIdleConns_SetsIdlePool(t *testing.T) {
	// Only the idle keep-alive pool is tuned: MaxIdleConnsPerHost (the stdlib
	// default is 2, which forces a fresh TLS handshake for every worker beyond the
	// second) plus the global MaxIdleConns that bounds it. The hard
	// MaxConnsPerHost cap is deliberately left unset — worker concurrency already
	// bounds in-flight requests, so a cap would only risk blocking a worker.
	g := New(Config{TenantID: "t"}, WithMaxIdleConns(10)).(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok, "WithMaxIdleConns must install a concrete *http.Transport")
	assert.Equal(t, 10, tr.MaxIdleConnsPerHost, "idle keep-alives retained for reuse")
	assert.GreaterOrEqual(t, tr.MaxIdleConns, 10, "global idle budget must cover the per-host budget")
	assert.Zero(t, tr.MaxConnsPerHost, "no hard connection cap — concurrency is bounded by the worker count")
}

func TestWithMaxIdleConns_PreservesTLSSkip(t *testing.T) {
	// Idle-pool tuning must mutate the existing TLS-skip transport in place, not
	// clone a fresh one — otherwise InsecureSkipVerify (the on-prem default) is
	// dropped.
	g := New(Config{TenantID: "t", TLSInsecureSkipVerify: true}, WithMaxIdleConns(5)).(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.TLSClientConfig, "TLS-skip config must survive idle-pool tuning")
	assert.True(t, tr.TLSClientConfig.InsecureSkipVerify, "idle-pool tuning must not drop the TLS-skip config")
	assert.Equal(t, 5, tr.MaxIdleConnsPerHost)
	assert.Zero(t, tr.MaxConnsPerHost, "no hard connection cap")
}

func TestWithMaxIdleConns_NonPositiveNoop(t *testing.T) {
	// n<=0 leaves the pool at Go's defaults (no concrete transport installed).
	g := New(Config{TenantID: "t"}, WithMaxIdleConns(0)).(*graphClient)
	assert.Nil(t, g.httpClient.Transport, "n<=0 must leave the default transport untouched")
	gneg := New(Config{TenantID: "t"}, WithMaxIdleConns(-1)).(*graphClient)
	assert.Nil(t, gneg.httpClient.Transport, "negative n must leave the default transport untouched")
}

func TestNewChatsClient_MaxIdleConnsSurvivesProxy(t *testing.T) {
	// NewChatsClient applies the options (WithMaxIdleConns) inside New, then wires
	// the proxy afterwards. applyProxyURL must reuse that transport so the idle
	// pool size is not lost when a proxy is configured.
	c, err := NewChatsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
		WithMaxIdleConns(7),
	)
	require.NoError(t, err)
	g := c.(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 7, tr.MaxIdleConnsPerHost, "idle pool must survive proxy application")
	assert.Zero(t, tr.MaxConnsPerHost, "no hard connection cap")
	require.NotNil(t, tr.Proxy, "proxy must still be configured alongside the idle pool")
}

// stubRoundTripper is a non-*http.Transport RoundTripper, used to verify that
// connection-pool tuning leaves custom transports untouched.
type stubRoundTripper struct{}

func (stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("stub round tripper")
}

func TestWithMaxIdleConns_LeavesCustomRoundTripper(t *testing.T) {
	// A client whose Transport is not an *http.Transport (a custom RoundTripper
	// injected via WithHTTPClient) can't have idle-conn fields tuned; the option
	// must leave it in place rather than replacing it with a default transport,
	// which would silently drop its TLS/auth/mock behavior.
	g := New(Config{TenantID: "t"},
		WithHTTPClient(&http.Client{Transport: stubRoundTripper{}}),
		WithMaxIdleConns(10),
	).(*graphClient)
	assert.IsType(t, stubRoundTripper{}, g.httpClient.Transport, "custom RoundTripper must be preserved unchanged")
}

func TestWithMaxIdleConns_PreservesUnlimitedAndClonesSupplied(t *testing.T) {
	// MaxIdleConns == 0 means "unlimited"; raising it to n would turn unlimited
	// into a finite cap, so it must stay 0. And a caller-supplied transport must
	// be cloned before tuning, never mutated in place.
	supplied := &http.Transport{MaxIdleConns: 0} // 0 == unlimited
	g := New(Config{TenantID: "t"},
		WithHTTPClient(&http.Client{Transport: supplied}),
		WithMaxIdleConns(10),
	).(*graphClient)

	assert.Zero(t, supplied.MaxIdleConnsPerHost, "supplied transport must not be mutated in place")
	assert.Zero(t, supplied.MaxIdleConns, "supplied transport left untouched")

	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.NotSame(t, supplied, tr, "tuning must operate on a clone of the supplied transport")
	assert.Equal(t, 10, tr.MaxIdleConnsPerHost)
	assert.Zero(t, tr.MaxIdleConns, "unlimited (0) MaxIdleConns must be preserved, not lowered to n")
}

func TestNewChatsClient_ProxyRejectsCustomRoundTripper(t *testing.T) {
	// A proxy can only be applied to an *http.Transport; when the caller injected
	// a custom RoundTripper, construction fails fast rather than discarding it.
	_, err := NewChatsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
		WithHTTPClient(&http.Client{Transport: stubRoundTripper{}}),
	)
	require.Error(t, err)
}
