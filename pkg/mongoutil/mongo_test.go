package mongoutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestBuildClientOptions(t *testing.T) {
	const uri = "mongodb://localhost:27017"

	tests := []struct {
		name         string
		username     string
		password     string
		expectAuth   bool
		expectedUser string
		expectedPass string
	}{
		{
			name:       "no credentials connects without auth",
			username:   "",
			password:   "",
			expectAuth: false,
		},
		{
			name:       "empty username with password skips auth",
			username:   "",
			password:   "secret",
			expectAuth: false,
		},
		{
			name:       "username with empty password skips auth",
			username:   "user",
			password:   "",
			expectAuth: false,
		},
		{
			name:         "both credentials set populates Auth",
			username:     "user",
			password:     "secret",
			expectAuth:   true,
			expectedUser: "user",
			expectedPass: "secret",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := buildClientOptions(uri, tc.username, tc.password)
			require.NotNil(t, opts)

			if !tc.expectAuth {
				assert.Nil(t, opts.Auth)
				return
			}

			require.NotNil(t, opts.Auth)
			assert.Equal(t, tc.expectedUser, opts.Auth.Username)
			assert.Equal(t, tc.expectedPass, opts.Auth.Password)
		})
	}
}

func TestBuildReadClientOptions_SecondaryPreferred(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "user", "pass")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	require.NotNil(t, opts.Auth)
	assert.Equal(t, "user", opts.Auth.Username)
}

func TestBuildReadClientOptions_NoAuthWhenEmpty(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "", "")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	assert.Nil(t, opts.Auth)
}

func TestSanitizeURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{"credentials stripped", "mongodb://user:secret@host:27017/db", "mongodb://host:27017/db"},
		{"username-only stripped", "mongodb://user@host:27017", "mongodb://host:27017"},
		{"no credentials unchanged", "mongodb://host:27017", "mongodb://host:27017"},
		{"srv scheme", "mongodb+srv://user:secret@cluster.example.net/db", "mongodb+srv://cluster.example.net/db"},
		{"query options stripped", "mongodb://host:27017/db?authMechanismProperties=AWS_SESSION_TOKEN:tok&proxyPassword=hunter2", "mongodb://host:27017/db"},
		{"fragment stripped", "mongodb://host:27017/db#frag", "mongodb://host:27017/db"},
		{"unparseable", "mongodb://user:sec ret@%zz", "invalid-uri"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeURI(tc.uri))
		})
	}
}
