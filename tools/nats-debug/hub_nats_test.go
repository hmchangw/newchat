package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func applyConnectOptions(t *testing.T, opts []nats.Option) nats.Options {
	t.Helper()
	o := nats.GetDefaultOptions()
	for _, opt := range opts {
		require.NoError(t, opt(&o))
	}
	return o
}

// writeFakeCreds writes a syntactically valid decorated NATS user credentials
// file (JWT + NKey seed) so nats.UserCredentials' smoke test passes on apply.
func writeFakeCreds(t *testing.T) string {
	t.Helper()
	userKey, err := nkeys.CreateUser()
	require.NoError(t, err)
	seed, err := userKey.Seed()
	require.NoError(t, err)
	pub, err := userKey.PublicKey()
	require.NoError(t, err)

	accountKey, err := nkeys.CreateAccount()
	require.NoError(t, err)
	token, err := jwt.NewUserClaims(pub).Encode(accountKey)
	require.NoError(t, err)

	contents, err := jwt.FormatUserConfig(token, seed)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "fake.creds")
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	return path
}

func TestDebugHeader(t *testing.T) {
	tests := []struct {
		name string
		dbg  DebugHeaders
		want nats.Header
	}{
		{
			name: "neither set returns nil",
			dbg:  DebugHeaders{},
			want: nil,
		},
		{
			name: "level only",
			dbg:  DebugHeaders{Level: "flow"},
			want: nats.Header{"X-Debug": []string{"flow"}},
		},
		{
			name: "payload only",
			dbg:  DebugHeaders{Payload: true},
			want: nats.Header{"X-Debug-Payload": []string{"1"}},
		},
		{
			name: "both set",
			dbg:  DebugHeaders{Level: "trace", Payload: true},
			want: nats.Header{"X-Debug": []string{"trace"}, "X-Debug-Payload": []string{"1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, debugHeader(tt.dbg))
		})
	}
}

func TestBuildConnectOptions(t *testing.T) {
	tests := []struct {
		name          string
		connName      string
		withCreds     bool
		wantCredsAuth bool
	}{
		{
			name:          "no creds file leaves connection unauthenticated",
			connName:      "nats-debug-source",
			withCreds:     false,
			wantCredsAuth: false,
		},
		{
			name:          "creds file wires user credentials",
			connName:      "nats-debug-dest",
			withCreds:     true,
			wantCredsAuth: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var credsFile string
			if tt.withCreds {
				credsFile = writeFakeCreds(t)
			}
			o := applyConnectOptions(t, buildConnectOptions(tt.connName, credsFile))

			assert.Equal(t, tt.connName, o.Name)
			assert.Equal(t, 0, o.MaxReconnect, "debug tool must not auto-reconnect")

			if tt.wantCredsAuth {
				assert.NotNil(t, o.UserJWT, "expected JWT callback when creds file set")
				assert.NotNil(t, o.SignatureCB, "expected signature callback when creds file set")
			} else {
				assert.Nil(t, o.UserJWT, "expected no JWT callback without creds file")
				assert.Nil(t, o.SignatureCB, "expected no signature callback without creds file")
			}
		})
	}
}
