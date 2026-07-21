package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgraph"
)

func newTestServer(t *testing.T) (*server, *httptest.Server) {
	t.Helper()
	raw, err := os.ReadFile("fixtures.sample.json")
	require.NoError(t, err)
	s := &server{}
	require.NoError(t, json.Unmarshal(raw, &s.data))
	srv := httptest.NewServer(newRouter(s))
	t.Cleanup(srv.Close)
	return s, srv
}

// graphReader wires the real pkg/msgraph client at the mock, so the walk
// exercises the client's actual token + paging paths.
func graphReader(srv *httptest.Server) msgraph.GroupReader {
	return msgraph.NewGroupReaderClient(
		msgraph.Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		msgraph.WithBaseURL(srv.URL+"/v1.0"),
		msgraph.WithTokenURL(srv.URL+"/t/oauth2/v2.0/token"),
	)
}

func TestToken(t *testing.T) {
	_, srv := newTestServer(t)
	resp, err := http.Post(srv.URL+"/any-tenant/oauth2/v2.0/token", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&tok))
	assert.NotEmpty(t, tok.AccessToken)
	assert.Positive(t, tok.ExpiresIn)
}

func TestGetGroup_ViaMsgraphClient(t *testing.T) {
	_, srv := newTestServer(t)
	got, err := graphReader(srv).GetGroup(context.Background(), "g-eng")
	require.NoError(t, err)
	assert.Equal(t, &msgraph.GroupProfile{ID: "g-eng", DisplayName: "Engineering", Description: "Engineering department"}, got)
}

func TestGetGroup_Unknown404(t *testing.T) {
	_, srv := newTestServer(t)
	_, err := graphReader(srv).GetGroup(context.Background(), "nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
}

func TestListMembers_PagedWalkViaMsgraphClient(t *testing.T) {
	_, srv := newTestServer(t)
	var got []msgraph.GraphUser
	pages := 0
	skipped, err := graphReader(srv).ListGroupMembers(context.Background(), "g-eng", 2,
		func(users []msgraph.GraphUser) error {
			pages++
			got = append(got, users...)
			return nil
		})
	require.NoError(t, err)
	assert.Equal(t, 2, pages, "4 members at $top=2 walks 2 pages")
	assert.Equal(t, 1, skipped, "the nested-group member is skipped")
	require.Len(t, got, 3)
	assert.Equal(t, "alice.wu@corp.example", got[0].UserPrincipalName)
	assert.Equal(t, "EMP001", got[0].EmployeeID)
}

func TestFixtureSwap(t *testing.T) {
	_, srv := newTestServer(t)
	body := []byte(`{"groups":[{"id":"g-new","displayName":"New","description":"d","members":[
		{"@odata.type":"#microsoft.graph.user","id":"u9","userPrincipalName":"zed@corp.example"}]}]}`)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/__fixtures", bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// old group gone, new one served
	_, err = graphReader(srv).GetGroup(context.Background(), "g-eng")
	require.Error(t, err)
	g, err := graphReader(srv).GetGroup(context.Background(), "g-new")
	require.NoError(t, err)
	assert.Equal(t, "New", g.DisplayName)

	// GET /__fixtures reflects the swap
	cur, err := http.Get(srv.URL + "/__fixtures")
	require.NoError(t, err)
	defer cur.Body.Close()
	var f fixtures
	require.NoError(t, json.NewDecoder(cur.Body).Decode(&f))
	require.Len(t, f.Groups, 1)
	assert.Equal(t, "g-new", f.Groups[0].ID)
}
