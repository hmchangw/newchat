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
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

// newTestDirectory wires a DirectoryReader at the given token + graph servers.
func newTestDirectory(tokenURL, baseURL string) DirectoryReader {
	return NewDirectoryClient(
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
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
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "super-secret-value"},
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
	def := New(&Config{TenantID: "t"}).(*graphClient)
	assert.Nil(t, def.httpClient.Transport, "default client must keep TLS verification")

	// Enabled: transport carries InsecureSkipVerify.
	ins := New(&Config{TenantID: "t", TLSInsecureSkipVerify: true}).(*graphClient)
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
	c := NewDirectoryClient(&Config{TenantID: "t"})
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
			_, _ = w.Write([]byte(`{"value":[{"id":"u3","userPrincipalName":"carol@corp.example"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"value":[` +
			`{"id":"u1","userPrincipalName":"alice@corp.example"},` +
			`{"id":"u2","userPrincipalName":"bob@corp.example"}],` +
			`"@odata.nextLink":"` + graphSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister := NewUserListerClient(
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	var pages [][]GraphUser
	err := lister.ListUsers(context.Background(), 500, func(users []GraphUser) error {
		pages = append(pages, users)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, pages, 2)
	assert.Equal(t, []GraphUser{
		{ID: "u1", UserPrincipalName: "alice@corp.example"},
		{ID: "u2", UserPrincipalName: "bob@corp.example"},
	}, pages[0])
	assert.Equal(t, []GraphUser{{ID: "u3", UserPrincipalName: "carol@corp.example"}}, pages[1])

	// first request carries $top and $select
	require.NotEmpty(t, requests)
	first, err := url.Parse(requests[0])
	require.NoError(t, err)
	assert.Equal(t, "500", first.Query().Get("$top"))
	assert.Equal(t, "id,userPrincipalName", first.Query().Get("$select"))
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

	lister := NewUserListerClient(
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	err := lister.ListUsers(context.Background(), 500, func([]GraphUser) error {
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

	lister := NewUserListerClient(
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	err := lister.ListUsers(context.Background(), 500, func([]GraphUser) error { return nil })
	require.ErrorContains(t, err, "status 403")
}
