package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticJ1_YieldsLiteral(t *testing.T) {
	tok, err := staticJ1("abc")()
	require.NoError(t, err)
	assert.Equal(t, "abc", tok)
}

func TestFileJ1_ReadsAndTrims(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("  sa-token-value\n"), 0o600))

	tok, err := fileJ1(path)()
	require.NoError(t, err)
	assert.Equal(t, "sa-token-value", tok)
}

// A projected ServiceAccount token is rotated in place; the same source must
// see the new contents on the next call without being reconstructed.
func TestFileJ1_RereadsCurrentContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("v1"), 0o600))
	src := fileJ1(path)

	tok1, err := src()
	require.NoError(t, err)
	assert.Equal(t, "v1", tok1)

	require.NoError(t, os.WriteFile(path, []byte("v2"), 0o600))
	tok2, err := src()
	require.NoError(t, err)
	assert.Equal(t, "v2", tok2)
}

func TestFileJ1_EmptyFileIsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("  \n"), 0o600))

	_, err := fileJ1(path)()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestFileJ1_MissingFileIsError(t *testing.T) {
	_, err := fileJ1(filepath.Join(t.TempDir(), "nope"))()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read j1 token file")
}

func TestNewJ1Source_LiteralWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("from-file"), 0o600))

	src, err := newJ1Source("from-env", path)
	require.NoError(t, err)
	got, err := src()
	require.NoError(t, err)
	assert.Equal(t, "from-env", got)
}

func TestNewJ1Source_FileWhenNoLiteral(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("from-file"), 0o600))

	src, err := newJ1Source("", path)
	require.NoError(t, err)
	got, err := src()
	require.NoError(t, err)
	assert.Equal(t, "from-file", got)
}

func TestNewJ1Source_NeitherIsError(t *testing.T) {
	_, err := newJ1Source("", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TRANSLATION_J1_TOKEN")
}
