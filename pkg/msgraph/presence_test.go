package msgraph

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPresencesByUserId_ROPC(t *testing.T) {
	var grant, user string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grant = r.Form.Get("grant_type")
		user = r.Form.Get("username")
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer ptok", r.Header.Get("Authorization"))
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"))
		assert.Contains(t, r.URL.Path, "/communications/getPresencesByUserId")
		var body struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, []string{"id1", "id2"}, body.IDs)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []Presence{
				{ID: "id1", Availability: "Busy", Activity: "InACall"},
				{ID: "id2", Availability: "Available", Activity: "Available"},
			},
		})
	}))
	defer graphSrv.Close()

	pc := NewPresenceClient(
		&Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	require.NoError(t, err)
	res, err := pc.GetPresencesByUserId(context.Background(), []string{"id1", "id2"})
	require.NoError(t, err)
	require.Len(t, res, 2)
	assert.Equal(t, "InACall", res[0].Activity)
	assert.Equal(t, "password", grant)
	assert.Equal(t, "svc@corp.com", user)
}

// TestGetPresencesByUserId_UserAgentOverride verifies Config.UserAgent replaces
// the default browser User-Agent on both the token and presence requests.
func TestGetPresencesByUserId_UserAgentOverride(t *testing.T) {
	const custom = "acme-presence/2.3"
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []Presence{{ID: "id1", Availability: "Available", Activity: "Available"}},
		})
	}))
	defer graphSrv.Close()

	pc, err := NewPresenceClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", UserAgent: custom},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	require.NoError(t, err)
	res, err := pc.GetPresencesByUserId(context.Background(), []string{"id1"})
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestGetPresencesByUserId_Empty(t *testing.T) {
	pc := NewPresenceClient(&Config{TenantID: "t"}, ROPCCredentials{})
	res, err := pc.GetPresencesByUserId(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res)
}

// TestGetPresencesByUserId_ThroughProxy verifies that a configured ProxyURL
// routes both the token acquisition and the Graph presence request through the
// proxy. The proxy serves canned responses keyed by request path, so the token
// and Graph target hosts never need to be reachable — every request is dialed
// to the proxy, which is exactly what we want to assert.
func TestGetPresencesByUserId_ThroughProxy(t *testing.T) {
	var mu sync.Mutex
	var proxiedHosts []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		proxiedHosts = append(proxiedHosts, r.Host)
		mu.Unlock()
		if strings.Contains(r.URL.Path, "getPresencesByUserId") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []Presence{{ID: "id1", Availability: "Available", Activity: "Available"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer proxy.Close()

	pc, err := NewPresenceClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: proxy.URL},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
		WithTokenURL("http://login.example.test/token"), WithBaseURL("http://graph.example.test"),
	)
	require.NoError(t, err)
	res, err := pc.GetPresencesByUserId(context.Background(), []string{"id1"})
	require.NoError(t, err)
	require.Len(t, res, 1)

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, proxiedHosts, "login.example.test", "token request should traverse the proxy")
	assert.Contains(t, proxiedHosts, "graph.example.test", "presence request should traverse the proxy")
}

func TestNewPresenceClient_InvalidProxyURL(t *testing.T) {
	tests := []struct {
		name  string
		proxy string
	}{
		{name: "unparseable", proxy: "://nope"},
		{name: "missing scheme", proxy: "proxy.corp:8080"},
		{name: "missing host", proxy: "http://"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pc, err := NewPresenceClient(
				Config{TenantID: "t", ProxyURL: tc.proxy},
				ROPCCredentials{},
			)
			require.Error(t, err)
			assert.Nil(t, pc)
		})
	}
}
