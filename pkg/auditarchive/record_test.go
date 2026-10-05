package auditarchive

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord_Hash(t *testing.T) {
	r := Record{Site: "site-a", Stream: "MESSAGES-CANONICAL-site-a", Seq: 42, Subject: "chat.msg.canonical.site-a.created", EventAt: 1700000000000, Payload: json.RawMessage(`{ "event": "created" , "x":1}`)}
	h1, err := r.Hash()
	require.NoError(t, err)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, h1)

	t.Run("whitespace in payload does not change the hash", func(t *testing.T) {
		r2 := r
		r2.Payload = json.RawMessage(`{"event":"created","x":1}`)
		h2, err := r2.Hash()
		require.NoError(t, err)
		assert.Equal(t, h1, h2)
	})
	t.Run("a different seq changes the hash", func(t *testing.T) {
		r3 := r
		r3.Seq = 43
		h3, err := r3.Hash()
		require.NoError(t, err)
		assert.NotEqual(t, h1, h3)
	})
	t.Run("invalid payload is an error", func(t *testing.T) {
		r4 := r
		r4.Payload = json.RawMessage(`{not json`)
		_, err := r4.Hash()
		assert.Error(t, err)
	})
	t.Run("HashBytes matches Hash", func(t *testing.T) {
		b, err := r.Marshal()
		require.NoError(t, err)
		assert.Equal(t, h1, HashBytes(b))
	})
}
