package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// Keep one root integration test across the catalog and verifier packages. The
// catalog-owned state and memory tests live beside the extracted package.
func TestSoakCatalog_ObservePinnedVerifiesCleanAgainstTheServiceReply(t *testing.T) {
	catalog := newSoakCatalog(16, 64, 0, nil)
	body := "pinned announcement body"
	require.True(t, catalog.ObservePinned(&soakWireMessage{
		MessageID: "msg-1", RoomID: "room-1", Msg: body,
		Sender:    cassandra.Participant{Account: "user-1"},
		CreatedAt: time.Unix(1000, 0).UTC(),
	}))
	expected, known := catalog.Get("room-1", "msg-1")
	require.True(t, known)

	result := soakVerifyResult{}
	compareSoakVerifiedMessage(&result, &expected, &soakVerifyMessage{
		MessageID: "msg-1", RoomID: "room-1", Msg: body,
		Sender: modelParticipant("user-1"),
	})

	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Empty(t, result.Field)
}
