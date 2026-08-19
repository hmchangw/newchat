package main

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func trackAcceptedSoakMessage(t *testing.T, catalog *soakCatalog, roomID, id, content string) {
	t.Helper()
	candidate := soakCatalogCandidate{
		ID: id, RoomID: roomID, Author: "user-1",
		Content: content, CreatedAt: time.Unix(1000, 0).UTC(),
	}
	require.NoError(t, catalog.TrackPublished(&candidate))
	require.True(t, catalog.Accept(roomID, id))
}

// Nothing that reads the catalog needs the message body: the verifiers compare
// it by hash and the search probe only wants one term out of it. Keeping the
// body was the single largest thing the harness held — measured at 351MB for a
// full catalogue of default-sized messages.
func TestSoakCatalog_KeepsAContentDigestRatherThanTheBody(t *testing.T) {
	catalog := newSoakCatalog(16, 64, 0, nil)
	body := "soakbody alpha bravo charlie"
	trackAcceptedSoakMessage(t, catalog, "room-1", "msg-1", body)

	message, known := catalog.Get("room-1", "msg-1")

	require.True(t, known)
	digest := sha256.Sum256([]byte(body))
	assert.Equal(t, hex.EncodeToString(digest[:]), message.ContentSHA256)
	assert.Equal(t, len(body), message.ContentLength)
	assert.Empty(t, message.Content, "the catalogue must not hand back a body it did not keep")
}

func TestSoakCatalog_KeepsTheTermTheSearchProbeQueriesWith(t *testing.T) {
	catalog := newSoakCatalog(16, 64, 0, nil)
	catalog.RetainSearchTerms(true)
	body := "ab soakterm trailing words"
	trackAcceptedSoakMessage(t, catalog, "room-1", "msg-1", body)

	message, known := catalog.Get("room-1", "msg-1")

	require.True(t, known)
	assert.Equal(t, searchProbeTerm(body), message.SearchTerm)
}

func TestSoakCatalog_RefreshesTheDigestAndTermWhenAMessageIsEdited(t *testing.T) {
	catalog := newSoakCatalog(16, 64, 0, nil)
	catalog.RetainSearchTerms(true)
	trackAcceptedSoakMessage(t, catalog, "room-1", "msg-1", "original wording here")

	require.True(t, catalog.MarkEdited("room-1", "msg-1", "replacement wording here"))

	message, known := catalog.Get("room-1", "msg-1")
	require.True(t, known)
	digest := sha256.Sum256([]byte("replacement wording here"))
	assert.Equal(t, hex.EncodeToString(digest[:]), message.ContentSHA256)
	assert.Equal(t, searchProbeTerm("replacement wording here"), message.SearchTerm)
	assert.Equal(t, len("replacement wording here"), message.ContentLength)
}

// The digest is a fixed 64 characters however large the message is, so a
// catalogue of large messages must not cost more than one of small ones. This
// runs with search-term retention off, as the default configuration does: the
// soak's bodies carry no word boundaries, so the probe term is the whole body
// and keeping it would defeat the point.
func TestSoakCatalog_MemoryDoesNotScaleWithMessageSize(t *testing.T) {
	const messages = 20000
	measure := func(body string) float64 {
		catalog := newSoakCatalog(messages, messages, 0, nil)
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range messages {
			candidate := soakCatalogCandidate{
				ID: soakCatalogProbeID(i), RoomID: "room-1", Author: "user-1",
				Content: body, CreatedAt: time.Unix(1000, 0).UTC(),
			}
			if err := catalog.TrackPublished(&candidate); err != nil {
				t.Fatalf("track: %v", err)
			}
			catalog.Accept("room-1", candidate.ID)
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(catalog)
		return float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
	}

	small := measure(strings.Repeat("a", 64))
	large := measure(strings.Repeat("a", 4096))

	assert.Less(t, large, small*1.5,
		"a catalogue of 4KB messages cost %.1fMB against %.1fMB for 64B ones; "+
			"the body is still being retained", large, small)
}

func soakCatalogProbeID(index int) string {
	const digits = "0123456789"
	id := []byte("msg-000000")
	for position := len(id) - 1; position >= 4 && index > 0; position-- {
		id[position] = digits[index%10]
		index /= 10
	}
	return string(id)
}
