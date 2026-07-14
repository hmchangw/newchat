package main

// Validation for the broadcast-worker sonic (default config) trial.
//
// sonic's default config is NOT byte-identical to encoding/json: HTML
// metacharacters (< > &) are left unescaped. These tests assert that the
// output is nonetheless SEMANTICALLY identical (any compliant JSON parser
// decodes it to the same value) and that cross-codec round-trips are lossless,
// while documenting the exact byte divergence so the wire change is explicit.
//
// Run: go test -run 'TestSonic' -v ./broadcast-worker/

import (
	"encoding/base64"
	stdjson "encoding/json"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// htmlContentRoomEvent embeds content with HTML metacharacters — the exact case
// where sonic default and encoding/json diverge on the wire.
func htmlContentRoomEvent() model.RoomEvent {
	ts := time.Date(2026, 6, 22, 10, 30, 0, 0, time.UTC)
	msg := model.Message{
		ID:          "01H8XGJ9ABCDEF01234567",
		RoomID:      "rm_01H8XGJ9ABCDEF",
		UserID:      "usr_01H8XGJ9ABCDEF",
		UserAccount: "alice@example.com",
		Content:     `if a < b && c > d then "x" else <tag>`, // HTML/JSON-sensitive
		CreatedAt:   ts,
		Type:        "message",
	}
	return model.RoomEvent{
		Type:      model.RoomEventNewMessage,
		RoomID:    msg.RoomID,
		Timestamp: ts.UnixMilli(),
		RoomName:  "engineering",
		RoomType:  model.RoomTypeChannel,
		SiteID:    "site-a",
		UserCount: 7,
		LastMsgAt: ts,
		LastMsgID: msg.ID,
		Message:   &model.ClientMessage{Message: msg, Sender: &model.Participant{Account: msg.UserAccount}},
	}
}

func htmlContentMessageEvent() model.MessageEvent {
	ev := htmlContentRoomEvent()
	return model.MessageEvent{
		Event:     model.EventCreated,
		Message:   ev.Message.Message,
		SiteID:    "site-a",
		Timestamp: ev.Timestamp,
	}
}

// TestSonic_ClientMessageAttachmentShadow pins that the wrapper's decoded
// Attachments shadows the embedded raw Message.Attachments under sonic: with both
// fields populated, the "attachments" key must serialize the decoded objects (not
// the base64 of the raw blob), and sonic must agree with stdlib.
func TestSonic_ClientMessageAttachmentShadow(t *testing.T) {
	blob, err := stdjson.Marshal(model.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)
	cm := model.ClientMessage{
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Attachments: [][]byte{blob}, // raw embedded — must be shadowed
		},
		Attachments: []model.Attachment{{ID: "f1", Title: "a.png", Type: "file"}}, // decoded wrapper
	}
	std, err := stdjson.Marshal(cm)
	require.NoError(t, err)
	son, err := sonic.Marshal(cm)
	require.NoError(t, err)

	assert.JSONEq(t, string(std), string(son))
	assert.Contains(t, string(son), `"id":"f1"`)
	assert.NotContains(t, string(son), base64.StdEncoding.EncodeToString(blob))
}

// TestSonicSemanticEquivalence: sonic default output must parse to the same
// value as stdlib output, even though the bytes differ.
func TestSonic_SemanticEquivalence(t *testing.T) {
	fixtures := map[string]any{
		"roomEvent":    htmlContentRoomEvent(),
		"messageEvent": htmlContentMessageEvent(),
	}
	for name, v := range fixtures {
		t.Run(name, func(t *testing.T) {
			std, err := stdjson.Marshal(v)
			require.NoError(t, err)
			son, err := sonic.Marshal(v)
			require.NoError(t, err)

			// Semantically identical to any compliant parser...
			assert.JSONEq(t, string(std), string(son), "sonic output must be semantically equal to stdlib")

			// ...but NOT byte-identical: stdlib escapes < > & to their \uXXXX
			// forms, sonic default leaves them literal. Documented wire change.
			assert.NotEqual(t, string(std), string(son), "expected a byte divergence (HTML escaping)")
			assert.NotContains(t, string(std), "<", "stdlib escapes < (no literal < in output)")
			assert.Contains(t, string(son), "<", "sonic emits a literal <")
		})
	}
}

// TestSonicCrossCodecRoundTrip: a consumer on either codec decodes the other's
// bytes to an equal value — i.e. clients (stdlib/JSON.parse) read sonic output
// losslessly, and broadcast-worker (sonic) reads stdlib-produced inbound events.
func TestSonic_CrossCodecRoundTrip(t *testing.T) {
	t.Run("roomEvent", func(t *testing.T) {
		orig := htmlContentRoomEvent()
		son, err := sonic.Marshal(orig)
		require.NoError(t, err)
		var viaStd model.RoomEvent
		require.NoError(t, stdjson.Unmarshal(son, &viaStd)) // client reads sonic bytes
		assert.Equal(t, orig, viaStd)
	})
	t.Run("messageEvent", func(t *testing.T) {
		orig := htmlContentMessageEvent()
		std, err := stdjson.Marshal(orig)
		require.NoError(t, err)
		var viaSonic model.MessageEvent
		require.NoError(t, sonic.Unmarshal(std, &viaSonic)) // worker reads stdlib bytes
		assert.Equal(t, orig, viaSonic)
	})
}

// TestSonicEncryptedRawMessageEmbedding guards the encrypt path: EncryptedMessage
// is a json.RawMessage ([]byte). sonic must splice it in as raw JSON, not
// base64-encode it like an ordinary byte slice. Covers a real wire risk of the
// trial (per review feedback).
func TestSonic_EncryptedRawMessageEmbedding(t *testing.T) {
	ev := htmlContentRoomEvent()
	ev.Message = nil
	ev.EncryptedMessage = stdjson.RawMessage(`{"ciphertext":"abcdef","version":7}`)

	std, err := stdjson.Marshal(ev)
	require.NoError(t, err)
	son, err := sonic.Marshal(ev)
	require.NoError(t, err)

	assert.JSONEq(t, string(std), string(son))
	// RawMessage must be embedded as an object, never a base64 string.
	assert.Contains(t, string(son), `"encryptedMessage":{`, "RawMessage spliced as raw JSON")
	assert.NotContains(t, string(son), `"encryptedMessage":"`, "RawMessage not base64-encoded")

	var viaStd model.RoomEvent
	require.NoError(t, stdjson.Unmarshal(son, &viaStd))
	assert.JSONEq(t, string(ev.EncryptedMessage), string(viaStd.EncryptedMessage))
}
