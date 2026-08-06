package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTranslator_Mock(t *testing.T) {
	tr, err := newTranslator(&Config{Backend: "mock"})
	require.NoError(t, err)
	_, ok := tr.(mockTranslator)
	assert.True(t, ok)
}

func TestNewTranslator_StreamRequiresEndpoint(t *testing.T) {
	_, err := newTranslator(&Config{Backend: "stream", Endpoint: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TRANSLATION_ENDPOINT")
}

func TestNewTranslator_StreamRequiresAccessTokenURL(t *testing.T) {
	_, err := newTranslator(&Config{Backend: "stream", Endpoint: "http://x", J1Token: "j1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TRANSLATION_ACCESS_TOKEN_URL")
}

func TestNewTranslator_StreamRequiresJ1Token(t *testing.T) {
	_, err := newTranslator(&Config{Backend: "stream", Endpoint: "http://x", AccessTokenURL: "http://a"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TRANSLATION_J1_TOKEN")
}

func TestNewTranslator_StreamRejectsNonPositiveTimeout(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTranslator(&Config{
				Backend:        "stream",
				Endpoint:       "http://x",
				AccessTokenURL: "http://a",
				J1Token:        "j1",
				HTTPTimeout:    tc.timeout,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "TRANSLATION_HTTP_TIMEOUT")
		})
	}
}

func TestNewTranslator_Stream(t *testing.T) {
	tr, err := newTranslator(&Config{
		Backend:        "stream",
		Endpoint:       "http://x",
		AccessTokenURL: "http://a",
		J1Token:        "j1",
		HTTPTimeout:    time.Second,
	})
	require.NoError(t, err)
	_, ok := tr.(*streamTranslator)
	assert.True(t, ok)
}

func TestNewTranslator_StreamJ1FromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("sa-token"), 0o600))

	tr, err := newTranslator(&Config{
		Backend:        "stream",
		Endpoint:       "http://x",
		AccessTokenURL: "http://a",
		J1TokenFile:    path,
		HTTPTimeout:    time.Second,
	})
	require.NoError(t, err)
	_, ok := tr.(*streamTranslator)
	assert.True(t, ok)
}

func TestNewTranslator_StreamFailsWhenJ1FileMissing(t *testing.T) {
	_, err := newTranslator(&Config{
		Backend:        "stream",
		Endpoint:       "http://x",
		AccessTokenURL: "http://a",
		J1TokenFile:    filepath.Join(t.TempDir(), "nope"),
		HTTPTimeout:    time.Second,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate j1 token source")
}

func TestNewTranslator_Unknown(t *testing.T) {
	_, err := newTranslator(&Config{Backend: "bogus"})
	require.Error(t, err)
}
