package msgraph

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestGroupReader wires a GroupReader at the given token + graph servers.
func newTestGroupReader(tokenURL, baseURL string) GroupReader {
	return NewGroupReaderClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

func newTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-g", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestGetGroup_Success(t *testing.T) {
	tokenSrv := newTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-g", r.Header.Get("Authorization"))
		assert.Equal(t, "/groups/g1", r.URL.Path)
		assert.Equal(t, "id,displayName,description", r.URL.Query().Get("$select"))
		_ = json.NewEncoder(w).Encode(GroupProfile{ID: "g1", DisplayName: "Engineering", Description: "eng dept"})
	}))
	defer graphSrv.Close()

	got, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).GetGroup(context.Background(), "g1")
	require.NoError(t, err)
	assert.Equal(t, &GroupProfile{ID: "g1", DisplayName: "Engineering", Description: "eng dept"}, got)
}

func TestGetGroup_Non200IsError(t *testing.T) {
	tokenSrv := newTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer graphSrv.Close()

	_, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).GetGroup(context.Background(), "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
}

func TestListGroupMembers_MultiPageFiltersNonUsers(t *testing.T) {
	tokenSrv := newTokenServer(t)
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/groups/g1/members", r.URL.Path)
		switch r.URL.Query().Get("page") {
		case "": // first page: carries $select/$top, has a nextLink
			assert.Equal(t, memberSelect, r.URL.Query().Get("$select"))
			assert.Equal(t, "2", r.URL.Query().Get("$top"))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]any{
					{"@odata.type": "#microsoft.graph.user", "id": "u1", "userPrincipalName": "Alice@corp.com",
						"displayName": "愛麗絲", "givenName": "Alice", "surname": "Wu", "employeeId": "EMP1"},
					{"@odata.type": "#microsoft.graph.group", "id": "nested-g"},
				},
				"@odata.nextLink": graphSrv.URL + "/groups/g1/members?page=2",
			})
		case "2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]any{
					{"@odata.type": "#microsoft.graph.user", "id": "u2", "userPrincipalName": "bob@corp.com"},
					{"@odata.type": "#microsoft.graph.device", "id": "dev-1"},
				},
			})
		default:
			t.Errorf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer graphSrv.Close()

	var got []GraphUser
	var pages int
	skipped, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).ListGroupMembers(
		context.Background(), "g1", 2, func(users []GraphUser) error {
			pages++
			got = append(got, users...)
			return nil
		})
	require.NoError(t, err)
	assert.Equal(t, 2, pages)
	assert.Equal(t, 2, skipped, "non-user member objects are skipped and counted")
	require.Len(t, got, 2)
	assert.Equal(t, GraphUser{
		ID: "u1", UserPrincipalName: "Alice@corp.com",
		DisplayName: "愛麗絲", GivenName: "Alice", Surname: "Wu", EmployeeID: "EMP1",
	}, got[0])
	assert.Equal(t, "u2", got[1].ID)
}

func TestListGroupMembers_CallbackErrorAbortsWalk(t *testing.T) {
	tokenSrv := newTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []map[string]any{{"@odata.type": "#microsoft.graph.user", "id": "u1", "userPrincipalName": "a@b"}},
		})
	}))
	defer graphSrv.Close()

	boom := errors.New("boom")
	_, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).ListGroupMembers(
		context.Background(), "g1", 10, func([]GraphUser) error { return boom })
	require.ErrorIs(t, err, boom)
}

func TestListGroupMembers_Non200IsError(t *testing.T) {
	tokenSrv := newTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer graphSrv.Close()

	_, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).ListGroupMembers(
		context.Background(), "g1", 10, func([]GraphUser) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
}

func TestListGroupMembers_RejectsCrossOriginNextLink(t *testing.T) {
	tokenSrv := newTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value":           []map[string]any{},
			"@odata.nextLink": "https://evil.example.com/groups/g1/members?page=2",
		})
	}))
	defer graphSrv.Close()

	_, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).ListGroupMembers(
		context.Background(), "g1", 10, func([]GraphUser) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deviates from configured graph origin")
}

func TestGetGroup_RetriesOn429(t *testing.T) {
	tokenSrv := newTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(GroupProfile{ID: "g1", DisplayName: "Engineering"})
	}))
	defer graphSrv.Close()

	got, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).GetGroup(context.Background(), "g1")
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "a 429 must be retried, not surfaced")
	assert.Equal(t, "g1", got.ID)
}

func TestListGroupMembers_RetriesOn503(t *testing.T) {
	tokenSrv := newTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable) // 503 also retried
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"@odata.type":"#microsoft.graph.user","id":"u1","displayName":"Alice"}]}`))
	}))
	defer graphSrv.Close()

	var got []GraphUser
	_, err := newTestGroupReader(tokenSrv.URL, graphSrv.URL).
		ListGroupMembers(context.Background(), "g1", 500, func(users []GraphUser) error { got = append(got, users...); return nil })
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "a 503 must be retried, not surfaced")
	require.Len(t, got, 1)
	assert.Equal(t, "u1", got[0].ID)
}
