package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadDoc(t *testing.T, name string) []byte {
	t.Helper()
	// #nosec G304 -- test fixture read from the fixed local testdata/ dir with caller-supplied constant names
	b, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return b
}

func TestDecodeAndMap_Insert(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "insert.json"))
	require.NoError(t, err)
	msg := mapToMessage(rc)
	assert.Equal(t, "abc123def456ghi78", msg.ID)
	assert.Equal(t, "room1", msg.RoomID)
	assert.Equal(t, "u1", msg.UserID)
	assert.Equal(t, "alice", msg.UserAccount)
	assert.Equal(t, "Alice A", msg.UserDisplayName)
	assert.Equal(t, "hello world", msg.Content)
	assert.Equal(t, time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC), msg.CreatedAt.UTC())
	assert.False(t, isSoftDeleted(rc, "rm"))
}

func TestClassify_Edit(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "edit.json"))
	require.NoError(t, err)
	assert.False(t, isSoftDeleted(rc, "rm"))
	require.NotNil(t, rc.EditedAt)
}

func TestClassify_SoftDelete(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "softdelete.json"))
	require.NoError(t, err)
	assert.True(t, isSoftDeleted(rc, "rm"))
	assert.False(t, isSystemMessage(rc, "rm"), "rm is the delete marker, not a skipped system message")
}

func TestClassify_SystemMessage(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "system.json"))
	require.NoError(t, err)
	assert.True(t, isSystemMessage(rc, "rm"))
	assert.False(t, isSoftDeleted(rc, "rm"))
}

func TestClassify_RealMessageNotSystem(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "insert.json"))
	require.NoError(t, err)
	assert.False(t, isSystemMessage(rc, "rm"))
}

func TestDecode_Invalid(t *testing.T) {
	_, err := decodeRocketchatMessage([]byte(`{not json`))
	require.Error(t, err)
}

func TestIsForeignOrigin(t *testing.T) {
	foreign, err := decodeRocketchatMessage(loadDoc(t, "foreign.json"))
	require.NoError(t, err)
	assert.True(t, isForeignOrigin(foreign), "federation.origin set => authored at a remote site")

	local, err := decodeRocketchatMessage(loadDoc(t, "insert.json"))
	require.NoError(t, err)
	assert.False(t, isForeignOrigin(local), "no federation object => locally authored")

	emptyOrigin, err := decodeRocketchatMessage([]byte(`{"_id":"x","federation":{"origin":""}}`))
	require.NoError(t, err)
	assert.False(t, isForeignOrigin(emptyOrigin), "empty origin is treated as local (absent-or-empty)")
}
