package msgraph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
func newTestDirectory(t *testing.T, tokenURL, baseURL string) DirectoryReader {
	t.Helper()
	c, err := NewDirectoryClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
	require.NoError(t, err)
	return c
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
		ExternalID: "room-key-1", Subject: "Standup", OrganizerID: "alice@corp.com", AttendeeIDs: []string{"bob@corp.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "m1", m.ID)
	assert.Equal(t, "https://join/1", m.JoinURL)

	// Second call reuses the cached token (no second token fetch).
	_, err = c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "room-key-1", OrganizerID: "alice@corp.com"})
	require.NoError(t, err)
	assert.Equal(t, 1, tokenCalls, "token should be cached across calls")
	assert.Equal(t, 2, meetingCalls)
}

func TestCreateOnlineMeeting_StripsBaggageBeforeMicrosoftEgress(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	var tokenBaggage, tokenTraceparent, graphBaggage, graphTraceparent string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenBaggage = r.Header.Get("baggage")
		tokenTraceparent = r.Header.Get("traceparent")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	t.Cleanup(tokenSrv.Close)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		graphBaggage = r.Header.Get("baggage")
		graphTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	t.Cleanup(graphSrv.Close)

	member, err := baggage.NewMember("user.name", "alice")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	}))

	client := New(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenSrv.URL),
		WithBaseURL(graphSrv.URL),
		WithHTTPClient(&http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}),
	)
	_, err = client.CreateOnlineMeeting(ctx, CreateOnlineMeetingRequest{ExternalID: "room-42"})
	require.NoError(t, err)
	assert.Empty(t, tokenBaggage)
	assert.Empty(t, graphBaggage)
	assert.NotEmpty(t, tokenTraceparent, "trace correlation must survive baggage stripping")
	assert.NotEmpty(t, graphTraceparent, "trace correlation must survive baggage stripping")
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
		ExternalID: "k", OrganizerID: "a@b.com",
	})
	require.NoError(t, err)
	second, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "k", OrganizerID: "a@b.com",
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
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{OrganizerID: "a@b.com"}) // no ExternalID
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
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerID: "a@b.com"})
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
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerID: "a@b.com"})
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
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerID: "a@b.com"})
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

	c := newTestDirectory(t, tokenSrv.URL, graphSrv.URL)
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

	c := newTestDirectory(t, tokenSrv.URL, graphSrv.URL)
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
	c := newTestDirectory(t, tokenSrv.URL, graphSrv.URL)
	_, err := c.ResolveAccountIDs(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "accounts beyond one chunk trigger a second query")
}

func TestResolveAccountIDs_Empty(t *testing.T) {
	c, err := NewDirectoryClient(Config{TenantID: "t"})
	require.NoError(t, err)
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
			_, err = NewMeetingsClient(cfg)
			require.Error(t, err)
		})
	}
}

func TestNewMeetingsClient_RoutesThroughProxy(t *testing.T) {
	c, err := NewMeetingsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
	)
	require.NoError(t, err)
	g := c.(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.Proxy, "proxy must be configured on the transport")

	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	require.NoError(t, err)
	proxyURL, err := tr.Proxy(req)
	require.NoError(t, err)
	require.NotNil(t, proxyURL)
	assert.Equal(t, "http://proxy.corp:8080", proxyURL.String())
}

func TestNewMeetingsClient_EmptyProxyIsNoOp(t *testing.T) {
	c, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNewMeetingsClient_TLSInsecureAndProxyCompose(t *testing.T) {
	c, err := NewMeetingsClient(Config{
		TenantID:              "t",
		ClientID:              "c",
		ClientSecret:          "s",
		TLSInsecureSkipVerify: true,
		ProxyURL:              "http://proxy.corp:8080",
	})
	require.NoError(t, err)
	g := c.(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.TLSClientConfig)
	assert.True(t, tr.TLSClientConfig.InsecureSkipVerify, "TLS-insecure must survive proxy application")
	require.NotNil(t, tr.Proxy, "proxy must be configured alongside TLS-insecure")
}

func TestNewMeetingsClient_ProxyRejectsCustomRoundTripper(t *testing.T) {
	_, err := NewMeetingsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
		WithHTTPClient(&http.Client{Transport: stubRoundTripper{}}),
	)
	require.Error(t, err)
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
		ExternalID:  "room-key-1",
		Subject:     "Standup",
		OrganizerID: "alice@corp.com",
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
		ExternalID: "room-key-1", OrganizerID: "alice@corp.com",
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

	c := newTestDirectory(t, tokenSrv.URL, graphSrv.URL)
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
	// the proxy afterwards. applyProxy must reuse that transport so the idle
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

func TestNewMeetingsDirectoryClient_BothSurfacesUseAppOnlyToken(t *testing.T) {
	var grant string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grant = r.Form.Get("grant_type")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "apptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer apptok", r.Header.Get("Authorization"))
		if r.Method == http.MethodGet { // ResolveAccountIDs
			assert.Equal(t, "eventual", r.Header.Get("ConsistencyLevel"))
			assert.Contains(t, r.URL.Query().Get("$filter"), "startsWith(userPrincipalName,'alice@')")
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
				{ID: "ida", UserPrincipalName: "alice@corp.com"},
			}})
			return
		}
		// CreateOnlineMeeting
		assert.Contains(t, r.URL.Path, "/users/ida/onlineMeetings/createOrGet")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	client, dir, err := NewMeetingsDirectoryClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	require.NoError(t, err)

	got, err := dir.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida"}, got)

	mtg, err := client.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerID: "ida"})
	require.NoError(t, err)
	assert.Equal(t, "https://join/1", mtg.JoinURL)

	assert.Equal(t, "client_credentials", grant)
}

func TestNewMeetingsDirectoryClient_SameInstance(t *testing.T) {
	client, dir, err := NewMeetingsDirectoryClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"})
	require.NoError(t, err)
	assert.Same(t, client, dir, "both surfaces must be the same *graphClient (one token cache)")
}

func TestNewMeetingsDirectoryClient_InvalidProxyURL(t *testing.T) {
	_, _, err := NewMeetingsDirectoryClient(Config{TenantID: "t", ProxyURL: "://nope"})
	require.Error(t, err)
}

func TestCreateOnlineMeeting_UsesObjectIDs(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// organizer object ID in the createOrGet path
		assert.Contains(t, r.URL.Path, "/users/00000000-org/onlineMeetings/createOrGet")
		var body struct {
			ExternalID   string `json:"externalId"`
			Participants struct {
				Attendees []struct {
					Identity struct {
						User struct {
							ID string `json:"id"`
						} `json:"user"`
					} `json:"identity"`
				} `json:"attendees"`
			} `json:"participants"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "k", body.ExternalID)
		require.Len(t, body.Participants.Attendees, 1)
		assert.Equal(t, "11111111-bob", body.Participants.Attendees[0].Identity.User.ID)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	m, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "k", Subject: "Standup", OrganizerID: "00000000-org", AttendeeIDs: []string{"11111111-bob"},
	})
	require.NoError(t, err)
	assert.Equal(t, "m1", m.ID)
}

// proxyTargetOf resolves the proxy URL a constructed client's transport would
// dial for a Graph request, so tests can assert on the credentials carried in
// its userinfo. c must be one of the *graphClient-backed surfaces.
func proxyTargetOf(t *testing.T, c any) *url.URL {
	t.Helper()
	g, ok := c.(*graphClient)
	require.True(t, ok, "client must be backed by *graphClient")
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok, "transport must be *http.Transport")
	require.NotNil(t, tr.Proxy, "proxy must be configured on the transport")
	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	require.NoError(t, err)
	u, err := tr.Proxy(req)
	require.NoError(t, err)
	require.NotNil(t, u, "proxy must resolve for a Graph request")
	return u
}

// TestProxyCredentials_SentAsBasicAuth is the end-to-end assertion behind
// GRAPH_PROXY_USERNAME/GRAPH_PROXY_PASSWORD: an authenticating proxy must
// receive a Proxy-Authorization: Basic header on every hop (token and Graph
// alike). The proxy serves canned responses keyed by path, so neither the token
// host nor the Graph host has to be reachable.
func TestProxyCredentials_SentAsBasicAuth(t *testing.T) {
	var mu sync.Mutex
	proxyAuthByHost := map[string]string{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		proxyAuthByHost[r.Host] = r.Header.Get("Proxy-Authorization")
		mu.Unlock()
		if strings.Contains(r.URL.Path, "/users") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []GraphUser{{ID: "u1", UserPrincipalName: "u1@corp.com"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer proxy.Close()

	lister, err := NewUserListerClient(
		Config{
			TenantID: "t", ClientID: "c", ClientSecret: "s",
			ProxyURL:      proxy.URL,
			ProxyUsername: "proxyuser",
			ProxyPassword: "proxypass",
		},
		WithTokenURL("http://login.example.test/token"),
		WithBaseURL("http://graph.example.test"),
	)
	require.NoError(t, err)
	require.NoError(t, lister.ListUsers(context.Background(), 10, func([]GraphUser) error { return nil }))

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("proxyuser:proxypass"))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, want, proxyAuthByHost["login.example.test"], "token hop must authenticate to the proxy")
	assert.Equal(t, want, proxyAuthByHost["graph.example.test"], "graph hop must authenticate to the proxy")
}

// TestProxyCredentials_SpecialCharactersNeedNoEncoding is the reason the
// credentials are separate env vars rather than userinfo embedded in
// GRAPH_PROXY_URL: a password containing URL metacharacters would corrupt the
// parsed host if it were inlined, so it must survive verbatim here.
func TestProxyCredentials_SpecialCharactersNeedNoEncoding(t *testing.T) {
	// #nosec G101 -- fixture password for a local httptest proxy, not a real credential
	const password = "p@ss:w/rd?#%"
	c, err := NewMeetingsClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL:      "http://proxy.corp:8080",
		ProxyUsername: "corp\\svc",
		ProxyPassword: password,
	})
	require.NoError(t, err)

	u := proxyTargetOf(t, c)
	assert.Equal(t, "proxy.corp:8080", u.Host, "host must survive a password full of URL metacharacters")
	require.NotNil(t, u.User)
	assert.Equal(t, "corp\\svc", u.User.Username())
	got, ok := u.User.Password()
	require.True(t, ok)
	assert.Equal(t, password, got)
}

// TestProxyCredentials_OverrideEmbeddedUserinfo pins precedence: the explicit
// credentials win over anything already inlined in the URL, so rotating the
// password means changing one secret rather than two settings that can drift.
func TestProxyCredentials_OverrideEmbeddedUserinfo(t *testing.T) {
	// #nosec G101 -- fixture userinfo; the test exists to prove it is overridden
	c, err := NewMeetingsClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL:      "http://olduser:oldpass@proxy.corp:8080",
		ProxyUsername: "newuser",
		ProxyPassword: "newpass",
	})
	require.NoError(t, err)

	u := proxyTargetOf(t, c)
	require.NotNil(t, u.User)
	assert.Equal(t, "newuser", u.User.Username())
	got, _ := u.User.Password()
	assert.Equal(t, "newpass", got)
}

// TestProxyCredentials_EmbeddedUserinfoPreserved is the backward-compatibility
// guard: deployments already authenticating via userinfo in GRAPH_PROXY_URL
// keep working when the new vars are unset.
func TestProxyCredentials_EmbeddedUserinfoPreserved(t *testing.T) {
	// #nosec G101 -- fixture userinfo; the test exists to prove it is preserved
	c, err := NewMeetingsClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL: "http://embedded:secret@proxy.corp:8080",
	})
	require.NoError(t, err)

	u := proxyTargetOf(t, c)
	require.NotNil(t, u.User)
	assert.Equal(t, "embedded", u.User.Username())
	got, _ := u.User.Password()
	assert.Equal(t, "secret", got)
}

// TestProxyCredentials_WithoutProxyURLFails fails fast on a half-configured
// deployment: credentials with no GRAPH_PROXY_URL would otherwise be silently
// dropped while traffic egressed through the ambient HTTPS_PROXY unauthenticated.
func TestProxyCredentials_WithoutProxyURLFails(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "username only", username: "proxyuser"},
		{name: "password only", password: "proxypass"},
		{name: "both", username: "proxyuser", password: "proxypass"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				TenantID: "t", ClientID: "c", ClientSecret: "s",
				ProxyUsername: tc.username,
				ProxyPassword: tc.password,
			}
			_, err := NewMeetingsClient(cfg)
			require.Error(t, err)
			_, err = NewChatsClient(cfg)
			require.Error(t, err)
			_, err = NewChatMembersClient(cfg)
			require.Error(t, err)
			_, err = NewUserListerClient(cfg)
			require.Error(t, err)
			_, _, err = NewMeetingsDirectoryClient(cfg)
			require.Error(t, err)
			_, err = NewDirectoryClient(cfg)
			require.Error(t, err)
			_, err = NewGroupReaderClient(cfg)
			require.Error(t, err)
		})
	}
}

// TestProxyCredentials_PasswordWithoutUsernameFails rejects a password with no
// username: Basic auth has no such form, so it is a misconfiguration rather
// than an anonymous-proxy request.
func TestProxyCredentials_PasswordWithoutUsernameFails(t *testing.T) {
	_, err := NewMeetingsClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL:      "http://proxy.corp:8080",
		ProxyPassword: "proxypass",
	})
	require.Error(t, err)
}

// TestProxyCredentials_ErrorNeverLeaksPassword guards the one path where the
// proxy settings reach a log line. errcode.Classify writes construction errors
// to the server log, and CLAUDE.md forbids logging passwords.
func TestProxyCredentials_ErrorNeverLeaksPassword(t *testing.T) {
	const password = "sup3rs3cr3t"
	for _, proxy := range []string{"://nope", "proxy.corp:8080", "http://"} {
		t.Run(proxy, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{
				TenantID: "t", ClientID: "c", ClientSecret: "s",
				ProxyURL:      proxy,
				ProxyUsername: "proxyuser",
				ProxyPassword: password,
			})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), password, "proxy password must never reach an error string")
		})
	}
}

// TestProxyCredentials_HonoredByPresenceClient covers the one constructor
// outside msgraph.go that applies the proxy, so the presence lane authenticates
// with the same credentials as the app-only lanes.
func TestProxyCredentials_HonoredByPresenceClient(t *testing.T) {
	pc, err := NewPresenceClient(
		Config{
			TenantID: "t", ClientID: "c", ClientSecret: "s",
			ProxyURL:      "http://proxy.corp:8080",
			ProxyUsername: "proxyuser",
			ProxyPassword: "proxypass",
		},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
	)
	require.NoError(t, err)

	p, ok := pc.(*presenceClient)
	require.True(t, ok)
	tr, ok := p.hc.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.Proxy)
	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	require.NoError(t, err)
	u, err := tr.Proxy(req)
	require.NoError(t, err)
	require.NotNil(t, u.User)
	assert.Equal(t, "proxyuser", u.User.Username())
}

// TestNewDirectoryClient_RoutesThroughAuthenticatedProxy closes the last
// app-only gap: the standalone directory reader used to ignore every proxy
// setting and fall back to the ambient HTTPS_PROXY, so a deployment moving to
// an authenticating proxy would have had to configure the same credentials
// twice — once here as URL userinfo, once as GRAPH_PROXY_*.
func TestNewDirectoryClient_RoutesThroughAuthenticatedProxy(t *testing.T) {
	c, err := NewDirectoryClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL:      "http://proxy.corp:8080",
		ProxyUsername: "proxyuser",
		ProxyPassword: "p@ss:w/rd",
	})
	require.NoError(t, err)

	u := proxyTargetOf(t, c)
	assert.Equal(t, "proxy.corp:8080", u.Host)
	require.NotNil(t, u.User)
	assert.Equal(t, "proxyuser", u.User.Username())
	got, ok := u.User.Password()
	require.True(t, ok)
	assert.Equal(t, "p@ss:w/rd", got)
}

func TestNewDirectoryClient_EmptyProxyIsNoOp(t *testing.T) {
	c, err := NewDirectoryClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNewDirectoryClient_InvalidProxyURL(t *testing.T) {
	for _, proxy := range []string{"://nope", "proxy.corp:8080", "http://"} {
		t.Run(proxy, func(t *testing.T) {
			c, err := NewDirectoryClient(Config{TenantID: "t", ProxyURL: proxy})
			require.Error(t, err)
			assert.Nil(t, c)
		})
	}
}

// TestProxyCredentials_MalformedURLNeverLeaksEmbeddedPassword covers the path
// the separate-credentials test misses: url.Parse's *url.Error carries the raw
// input, so a malformed URL with embedded userinfo would put the proxy password
// in the returned error — and from there into the server log via
// errcode.Classify. Redacted() cannot help, since it needs a parsed URL.
func TestProxyCredentials_MalformedURLNeverLeaksEmbeddedPassword(t *testing.T) {
	const password = "sup3rs3cr3t"
	for _, proxy := range []string{
		"://user:" + password + "@proxy.corp:8080",
		"http://user:" + password + "@proxy.corp:80 80",
		"ht tp://user:" + password + "@proxy.corp",
	} {
		t.Run(proxy, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), password, "a malformed proxy url must not leak its embedded password")
		})
	}
}

// TestApplyProxy_RejectsUnsupportedScheme keeps the fail-fast promise whole:
// net/http only proxies through http, https, socks5 and socks5h, so any other
// scheme is a startup-time configuration error rather than a surprise on the
// first Graph call.
func TestApplyProxy_RejectsUnsupportedScheme(t *testing.T) {
	for _, proxy := range []string{"ftp://proxy.corp:21", "socks4://proxy.corp:1080", "ws://proxy.corp:80"} {
		t.Run(proxy, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy})
			require.Error(t, err)
		})
	}
}

func TestApplyProxy_AcceptsEverySupportedScheme(t *testing.T) {
	for _, proxy := range []string{
		"http://proxy.corp:8080",
		"https://proxy.corp:8443",
		"socks5://proxy.corp:1080",
		"socks5h://proxy.corp:1080",
	} {
		t.Run(proxy, func(t *testing.T) {
			c, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy})
			require.NoError(t, err)
			assert.Equal(t, proxy, proxyTargetOf(t, c).String())
		})
	}
}

// TestProxyCredentials_MalformedEscapeNeverLeaksPassword closes the hole left by
// the first sanitising attempt: unwrapping to *url.Error.Err is not enough,
// because some underlying parse errors quote the offending input themselves
// (`invalid URL escape "%zz"`). A password beginning with a bad escape would
// still reach the log, so no parse failure may carry any part of the value.
func TestProxyCredentials_MalformedEscapeNeverLeaksPassword(t *testing.T) {
	// #nosec G101 -- deliberately malformed fixture URLs; the test asserts none of
	// them reaches an error message
	tests := []struct {
		name   string
		proxy  string
		secret string
	}{
		{name: "bad escape starts the password", proxy: "http://user:%zzsecret@proxy.corp", secret: "zz"},
		{name: "bad escape inside the password", proxy: "http://user:pw%GGtail@proxy.corp", secret: "GG"},
		{name: "bad escape in the username", proxy: "http://%QQuser:pw@proxy.corp", secret: "QQ"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: tc.proxy})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), tc.secret, "no fragment of the proxy url may reach the error")
		})
	}
}

// TestApplyProxy_RejectsColonInBasicUsername keeps a colon-bearing username from
// failing only on the first request: Go builds the Basic credential as
// "user:pass", so an HTTP(S) proxy splits at the first colon and reads a
// different pair, answering 407. RFC 7617 forbids a colon in the user-id.
func TestApplyProxy_RejectsColonInBasicUsername(t *testing.T) {
	t.Run("explicit username", func(t *testing.T) {
		for _, scheme := range []string{"http", "https"} {
			t.Run(scheme, func(t *testing.T) {
				_, err := NewMeetingsClient(Config{
					TenantID: "t", ClientID: "c", ClientSecret: "s",
					ProxyURL:      scheme + "://proxy.corp:8080",
					ProxyUsername: "corp:svc",
					ProxyPassword: "pw",
				})
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "pw", "the rejection must not carry the password")
			})
		}
	})

	// A percent-encoded colon survives url.Parse into Username(), so the
	// embedded form reaches the same broken credential.
	t.Run("embedded username", func(t *testing.T) {
		_, err := NewMeetingsClient(Config{
			TenantID: "t", ClientID: "c", ClientSecret: "s",
			ProxyURL: "http://corp%3Asvc:pw@proxy.corp:8080",
		})
		require.Error(t, err)
	})
}

// TestApplyProxy_AllowsColonInSocksUsername is the other half: SOCKS5 negotiates
// credentials with length-prefixed fields (RFC 1929), so a colon is ordinary
// data there and must not be rejected.
func TestApplyProxy_AllowsColonInSocksUsername(t *testing.T) {
	for _, scheme := range []string{"socks5", "socks5h"} {
		t.Run(scheme, func(t *testing.T) {
			c, err := NewMeetingsClient(Config{
				TenantID: "t", ClientID: "c", ClientSecret: "s",
				ProxyURL:      scheme + "://proxy.corp:1080",
				ProxyUsername: "corp:svc",
				ProxyPassword: "pw",
			})
			require.NoError(t, err)
			u := proxyTargetOf(t, c)
			require.NotNil(t, u.User)
			assert.Equal(t, "corp:svc", u.User.Username())
		})
	}
}

// TestProxyCredentials_SchemelessURLNeverLeaksPassword covers the third shape of
// this leak. The first two were parse *failures*; this one parses fine. Without
// a scheme, url.Parse reads "user" as the scheme and swallows the rest into
// Opaque, leaving User nil — so Redacted(), which only masks a populated User,
// returns the raw string password and all. No error may interpolate the value.
func TestProxyCredentials_SchemelessURLNeverLeaksPassword(t *testing.T) {
	const password = "supersecret"
	for _, proxy := range []string{
		"user:" + password + "@proxy.corp:8080",
		"proxy.corp:8080/" + password,
	} {
		t.Run(proxy, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), password, "a parsed-but-invalid proxy url must not leak its value")
		})
	}
}

// TestApplyProxy_RejectsEmbeddedEmptyUsername mirrors the explicit
// password-without-username check onto userinfo carried in the URL: Basic would
// send ":secret", which an authenticating proxy answers with 407 — after
// construction has already succeeded.
func TestApplyProxy_RejectsEmbeddedEmptyUsername(t *testing.T) {
	const password = "secret"
	_, err := NewMeetingsClient(Config{
		TenantID: "t", ClientID: "c", ClientSecret: "s",
		ProxyURL: "http://:" + password + "@proxy.corp:8080",
	})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), password, "the rejection must not carry the password")
}

// TestApplyProxy_RejectsPortWithoutHostname closes a hole in the host check:
// "http://:8080" has a non-empty Host (":8080") but no hostname, so a Host==""
// test passes it through and the dial fails later instead of at startup.
func TestApplyProxy_RejectsPortWithoutHostname(t *testing.T) {
	for _, proxy := range []string{"http://:8080", "https://:443", "socks5://:1080"} {
		t.Run(proxy, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy})
			require.Error(t, err)
		})
	}
}

// TestApplyProxy_WarnsOnUnencryptedCredentialHop covers CWE-319: Basic and the
// RFC 1929 sub-negotiation both travel before any TLS to the target, so an
// http/socks5 proxy hands the credentials to anyone on the path. Only an https
// proxy encrypts the hop carrying them. Warned rather than rejected — corporate
// proxies are overwhelmingly plain http, and refusing them would break the
// deployment this setting exists for — so the warning is the whole mitigation
// and must actually fire, without quoting the credentials.
func TestApplyProxy_WarnsOnUnencryptedCredentialHop(t *testing.T) {
	const password = "sup3rs3cr3t"
	tests := []struct {
		name     string
		proxy    string
		wantWarn bool
	}{
		{name: "http warns", proxy: "http://proxy.corp:8080", wantWarn: true},
		{name: "socks5 warns", proxy: "socks5://proxy.corp:1080", wantWarn: true},
		{name: "socks5h warns", proxy: "socks5h://proxy.corp:1080", wantWarn: true},
		{name: "https is silent", proxy: "https://proxy.corp:8443", wantWarn: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			_, err := NewMeetingsClient(Config{
				TenantID: "t", ClientID: "c", ClientSecret: "s",
				ProxyURL:      tc.proxy,
				ProxyUsername: "proxyuser",
				ProxyPassword: password,
			})
			require.NoError(t, err)

			logged := buf.String()
			assert.NotContains(t, logged, password, "the warning must never carry the password")
			assert.NotContains(t, logged, "proxyuser", "the warning must never carry the username")
			if tc.wantWarn {
				assert.Contains(t, logged, "unencrypted", "an unencrypted credential hop must be warned about")
			} else {
				assert.Empty(t, logged, "an https proxy encrypts the credential hop, so nothing to warn about")
			}
		})
	}
}

// TestApplyProxy_NoWarnWithoutCredentials keeps the warning tied to credentials:
// an unauthenticated http proxy exposes nothing and must stay silent.
func TestApplyProxy_NoWarnWithoutCredentials(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"})
	require.NoError(t, err)
	assert.Empty(t, buf.String(), "no credentials means no exposure to warn about")
}

// newConnectProxy starts a proxy that serves only CONNECT, recording the
// Proxy-Authorization header per tunnelled host before splicing the two
// connections together. Production reaches both the token endpoint and Graph
// over https, so the credentials ride CONNECT rather than an ordinary proxied
// request — a different code path in net/http than the one an http:// target
// exercises.
func newConnectProxy(t *testing.T, targets ...string) (*httptest.Server, func(host string) string) {
	t.Helper()
	// The tunnel is only ever opened to the test servers, so the address dialed
	// comes from this set rather than from the request.
	allowed := map[string]string{}
	for _, target := range targets {
		allowed[target] = target
	}
	var mu sync.Mutex
	authByHost := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "this proxy serves CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		authByHost[r.Host] = r.Header.Get("Proxy-Authorization")
		mu.Unlock()

		target, ok := allowed[r.Host]
		if !ok {
			http.Error(w, "host is not a declared test target", http.StatusForbidden)
			return
		}
		upstream, err := net.Dial("tcp", target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		hijacker, hijackable := w.(http.Hijacker)
		if !hijackable {
			_ = upstream.Close()
			http.Error(w, "connection is not hijackable", http.StatusInternalServerError)
			return
		}
		downstream, _, err := hijacker.Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		if _, err := downstream.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
			_ = upstream.Close()
			_ = downstream.Close()
			return
		}
		// Both copies end when either side closes, so the handler returns
		// rather than outliving the test.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(upstream, downstream)
			_ = upstream.Close()
		}()
		go func() {
			defer wg.Done()
			_, _ = io.Copy(downstream, upstream)
			_ = downstream.Close()
		}()
		wg.Wait()
	}))
	t.Cleanup(srv.Close)
	return srv, func(host string) string {
		mu.Lock()
		defer mu.Unlock()
		return authByHost[host]
	}
}

// TestProxyCredentials_SentOnConnectTunnel is the https counterpart of
// TestProxyCredentials_SentAsBasicAuth: that one proxies plain http, where the
// header rides the request itself. Real deployments talk to
// login.microsoftonline.com and graph.microsoft.com over https, so the only
// hop that carries the credentials is CONNECT.
func TestProxyCredentials_SentOnConnectTunnel(t *testing.T) {
	token := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer token.Close()
	graph := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []GraphUser{{ID: "u1", UserPrincipalName: "u1@corp.com"}},
		})
	}))
	defer graph.Close()

	proxy, authFor := newConnectProxy(t, hostOf(t, token.URL), hostOf(t, graph.URL))

	lister, err := NewUserListerClient(
		Config{
			TenantID: "t", ClientID: "c", ClientSecret: "s",
			// The httptest certs are self-signed; the hop under test is the
			// CONNECT to the proxy, not the TLS inside the tunnel.
			TLSInsecureSkipVerify: true,
			ProxyURL:              proxy.URL,
			ProxyUsername:         "proxyuser",
			ProxyPassword:         "proxypass",
		},
		WithTokenURL(token.URL+"/token"),
		WithBaseURL(graph.URL),
	)
	require.NoError(t, err)

	// Idle tunnels keep the proxy handler running, so drop them before the
	// server's own cleanup waits on it.
	tr, ok := lister.(*graphClient).httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	t.Cleanup(tr.CloseIdleConnections)

	require.NoError(t, lister.ListUsers(context.Background(), 10, func([]GraphUser) error { return nil }))

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("proxyuser:proxypass"))
	assert.Equal(t, want, authFor(hostOf(t, token.URL)), "token CONNECT must authenticate to the proxy")
	assert.Equal(t, want, authFor(hostOf(t, graph.URL)), "graph CONNECT must authenticate to the proxy")
}

func hostOf(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	return u.Host
}

// TestApplyProxy_RejectsOutOfRangePort keeps the fail-fast contract honest:
// url.Parse only checks that a port is digits, so an out-of-range one would
// otherwise surface as a dial failure on the first Graph call instead of at
// construction.
func TestApplyProxy_RejectsOutOfRangePort(t *testing.T) {
	tests := []struct {
		name    string
		proxy   string
		wantErr bool
	}{
		{name: "above the range", proxy: "http://proxy.corp:99999", wantErr: true},
		{name: "just above the range", proxy: "http://proxy.corp:65536", wantErr: true},
		{name: "port zero", proxy: "http://proxy.corp:0", wantErr: true},
		{name: "socks5 out of range", proxy: "socks5://proxy.corp:70000", wantErr: true},
		{name: "highest valid port", proxy: "http://proxy.corp:65535"},
		{name: "lowest valid port", proxy: "http://proxy.corp:1"},
		{name: "no port at all", proxy: "http://proxy.corp"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: tc.proxy})
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "port")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestApplyProxy_RejectsUnencodableSocksCredentials pins the RFC 1929 wire
// limits. Go's SOCKS dialer refuses to encode an empty or over-long field, but
// only when it authenticates — on the first Graph request, long after the
// constructor said the config was fine. The equivalent Basic credential has no
// such limit, so the check is scoped to the SOCKS schemes.
func TestApplyProxy_RejectsUnencodableSocksCredentials(t *testing.T) {
	long := strings.Repeat("a", 256)
	max := strings.Repeat("a", 255)
	tests := []struct {
		name     string
		proxy    string
		username string
		password string
		wantErr  bool
	}{
		{name: "socks username over the limit", proxy: "socks5://proxy.corp:1080", username: long, password: "pw", wantErr: true},
		{name: "socks password over the limit", proxy: "socks5://proxy.corp:1080", username: "user", password: long, wantErr: true},
		{name: "socks5h username over the limit", proxy: "socks5h://proxy.corp:1080", username: long, password: "pw", wantErr: true},
		{name: "embedded socks username over the limit", proxy: "socks5://" + long + ":pw@proxy.corp:1080", wantErr: true},
		{name: "embedded socks password over the limit", proxy: "socks5://user:" + long + "@proxy.corp:1080", wantErr: true},
		{name: "embedded empty socks username", proxy: "socks5://:@proxy.corp:1080", wantErr: true},
		{name: "socks username at the limit", proxy: "socks5://proxy.corp:1080", username: max, password: max},
		{name: "http username over the limit", proxy: "http://proxy.corp:8080", username: long, password: long},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMeetingsClient(Config{
				TenantID: "t", ClientID: "c", ClientSecret: "s",
				ProxyURL: tc.proxy, ProxyUsername: tc.username, ProxyPassword: tc.password,
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), long, "the rejection must not echo the credential")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestApplyProxy_RejectsEmptyUserinfo covers the URL shapes that carry a bare
// "@": url.Parse leaves User non-nil with both fields empty, and net/http's
// proxyAuth() keys off nothing but that non-nil — it sends
// "Proxy-Authorization: Basic Og==" (base64 of ":"). An authenticating proxy
// answers 407 on the first request, so the pod starts and every Graph call
// fails. A username is required whenever userinfo is present at all.
func TestApplyProxy_RejectsEmptyUserinfo(t *testing.T) {
	tests := []struct {
		name    string
		proxy   string
		wantErr bool
	}{
		{name: "http empty username and password", proxy: "http://:@proxy.corp:8080", wantErr: true},
		{name: "http bare at sign", proxy: "http://@proxy.corp:8080", wantErr: true},
		{name: "https empty username and password", proxy: "https://:@proxy.corp:8080", wantErr: true},
		{name: "socks5 empty username and password", proxy: "socks5://:@proxy.corp:1080", wantErr: true},
		{name: "username without password stays valid", proxy: "http://user@proxy.corp:8080"},
		{name: "no userinfo at all", proxy: "http://proxy.corp:8080"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: tc.proxy})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			// The valid shapes must still reach the transport unchanged.
			u := proxyTargetOf(t, c)
			assert.Equal(t, "proxy.corp:8080", u.Host)
		})
	}
}
