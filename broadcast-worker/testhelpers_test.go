package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roomkeystore"
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

// lastMsg builds the room-doc update a plain (preview-less) insert produces, so
// the many existing expectations stay one line. Preview is nil: these tests run
// with previews off, which is also the ATREST_ENABLED=false production shape.
func lastMsg(roomID, msgID string, at time.Time, mentionAll bool) roomLastMessage {
	return roomLastMessage{RoomID: roomID, MsgID: msgID, At: at, MentionAll: mentionAll}
}
