package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

func testRoomKey(t *testing.T) *roomkeystore.VersionedKeyPair {
	t.Helper()
	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return &roomkeystore.VersionedKeyPair{
		Version: 3,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: buf,
		},
	}
}

func decryptForTest(env *roomcrypto.EncryptedMessage, roomPrivateKey []byte) (string, error) {
	// The room private key is used directly as the AES-256-GCM key (no HKDF step).
	block, err := aes.NewCipher(roomPrivateKey)
	if err != nil {
		return "", fmt.Errorf("aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, env.Nonce, env.Ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("gcm open: %w", err)
	}
	return string(plaintext), nil
}

func decryptClientMessage(t *testing.T, data []byte, key *roomkeystore.VersionedKeyPair) (model.RoomEvent, *model.ClientMessage) {
	t.Helper()
	var evt model.RoomEvent
	require.NoError(t, json.Unmarshal(data, &evt))
	require.Nil(t, evt.Message, "Message must be nil when EncryptedMessage is set")
	require.NotEmpty(t, evt.EncryptedMessage, "EncryptedMessage must be populated")
	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(evt.EncryptedMessage, &env))
	require.Equal(t, key.Version, env.Version, "EncryptedMessage.Version must match the key version")
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	var msg model.ClientMessage
	require.NoError(t, json.Unmarshal([]byte(plaintext), &msg))
	return evt, &msg
}

// fixedRoutes pins a handler to one route mode, for tests that exercise routing
// itself rather than the lane logic. Production code always passes a
// subject.LaneRouter, so the constructor takes a resolver and a bare mode
// cannot be wired in by accident.
func fixedRoutes(mode subject.RoomRouteMode) subject.RouteResolver {
	return subject.NewLaneRouter(mode, subject.LaneHome, nil, 0)
}
