package auditarchive

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord_Marshal(t *testing.T) {
	r := Record{Site: "site-a", Stream: "MESSAGES-CANONICAL-site-a", Seq: 42, Subject: "chat.msg.canonical.site-a.created", EventAt: 1700000000000, Payload: json.RawMessage(`{ "event": "created" , "x":1}`)}
	b1, err := r.Marshal()
	require.NoError(t, err)

	t.Run("whitespace in payload does not change the canonical form", func(t *testing.T) {
		r2 := r
		r2.Payload = json.RawMessage(`{"event":"created","x":1}`)
		b2, err := r2.Marshal()
		require.NoError(t, err)
		assert.Equal(t, b1, b2)
	})
	t.Run("a different seq changes the canonical form", func(t *testing.T) {
		r3 := r
		r3.Seq = 43
		b3, err := r3.Marshal()
		require.NoError(t, err)
		assert.NotEqual(t, b1, b3)
	})
	t.Run("invalid payload is an error", func(t *testing.T) {
		r4 := r
		r4.Payload = json.RawMessage(`{not json`)
		_, err := r4.Marshal()
		assert.Error(t, err)
	})
}
