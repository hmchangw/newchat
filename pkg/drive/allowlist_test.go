package drive

import (
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetGroupImage_UnlistedHostNeverReceivesTheAPIToken is the regression guard
// for the credential leak: drive_host arrives verbatim from a client query
// string and fetchPresignedURL attaches the Drive api-token to whatever host it
// is handed. The client therefore chose which server received the credential.
//
// The assertion that matters is not the returned error — it is that the
// attacker-controlled server was never contacted at all.
func TestGetGroupImage_UnlistedHostNeverReceivesTheAPIToken(t *testing.T) {
	var hits atomic.Int32
	var sawToken atomic.Bool
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("api-token") != "" {
			sawToken.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer attacker.Close()

	c := NewClient(&Config{
		URL:        "https://drive.internal.example",
		Token:      "super-secret-drive-token",
		BaseURLMap: map[string]string{"site-a": "https://drive.site-a.example"},
	})

	_, err := c.GetGroupImage(attacker.URL, "room-1", "file-1")

	require.Error(t, err, "an unlisted host must be refused")
	assert.ErrorIs(t, err, ErrHostNotAllowed)
	assert.Zero(t, hits.Load(), "the unlisted host must never be contacted")
	assert.False(t, sawToken.Load(), "the Drive api-token must never leave the process for an unlisted host")
}

func TestClient_hostAllowed(t *testing.T) {
	c := NewClient(&Config{
		URL:   "https://drive.internal.example",
		Token: "t",
		BaseURLMap: map[string]string{
			"site-a": "https://drive.site-a.example",
			"site-b": "https://drive.site-b.example/base",
			"site-c": "", // an empty entry must not widen the allow-list
		},
	})

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"the configured default is allowed", "https://drive.internal.example", true},
		{"a mapped host is allowed", "https://drive.site-a.example", true},
		{"a mapped host with a path is allowed", "https://drive.site-b.example/base", true},
		{"a trailing slash still matches", "https://drive.site-a.example/", true},

		{"an unknown host is refused", "https://evil.example", false},
		{"an empty host is refused", "", false},
		{"an empty map entry does not allow the empty host", "", false},
		{"userinfo prefixing an allowed host is refused", "https://drive.site-a.example@evil.example", false},
		{"an allowed host in the query string is refused", "https://evil.example/?u=https://drive.site-a.example", false},
		{"an allowed host as a path suffix is refused", "https://evil.example/https://drive.site-a.example", false},
		{"a subdomain of an allowed host is refused", "https://drive.site-a.example.evil.example", false},
		{"a prefix of an allowed host is refused", "https://drive.site-a.exampl", false},
		{"scheme downgrade is refused", "http://drive.site-a.example", false},
		{"a bare host without scheme is refused", "drive.site-a.example", false},
		{"a path traversal off an allowed base is refused", "https://drive.site-b.example/base/../../evil", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, c.hostAllowed(tt.host))
		})
	}
}

// TestClient_hostAllowed_EmptyMapKeepsTheDefault covers the config-load failure
// mode: LoadBaseURLs falls back to an empty map on a missing or malformed file.
// The allow-list must still admit the configured default Drive URL, so a
// single-site deployment keeps working, and must still admit nothing else.
func TestClient_hostAllowed_EmptyMapKeepsTheDefault(t *testing.T) {
	c := NewClient(&Config{URL: "https://drive.internal.example", Token: "t", BaseURLMap: map[string]string{}})

	assert.True(t, c.hostAllowed("https://drive.internal.example"))
	assert.False(t, c.hostAllowed("https://evil.example"))
}

// TestClient_hostAllowed_NoDefaultURL guards the degenerate config: with no
// default URL and no map there is nothing legitimate to reach, and the empty
// string must not become a wildcard.
func TestClient_hostAllowed_NoDefaultURL(t *testing.T) {
	c := NewClient(&Config{Token: "t"})

	assert.False(t, c.hostAllowed(""))
	assert.False(t, c.hostAllowed("https://evil.example"))
}

func TestErrHostNotAllowed_IsMatchable(t *testing.T) {
	c := NewClient(&Config{URL: "https://drive.internal.example", Token: "t"})
	_, err := c.GetGroupImage("https://evil.example", "g", "f")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrHostNotAllowed),
		"the handler maps this to a 400, so it must survive wrapping")
}
