package bucketcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// TestEncodeDecode_PreservesEveryServedField pins the gob blob to a faithful
// round-trip of the columns the read path serves.
//
// gob flattens pointers and then omits fields whose value is the zero value, so
// a non-nil pointer to a zero value decodes back as nil. Message.TCount is
// *int: a thread parent whose replies were all deleted holds tcount = 0, which
// survives a live Cassandra read as a pointer to 0 but comes back from the cache
// as nil. `json:"tcount,omitempty"` on a pointer emits "tcount":0 for the former
// and drops the field entirely for the latter, so the same message serializes
// differently depending on whether the read was cached — a wire divergence in a
// field documented in docs/client-api.md.
func TestEncodeDecode_PreservesEveryServedField(t *testing.T) {
	at := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	zero, two := 0, 2

	tests := []struct {
		name string
		msg  models.Message
	}{
		{
			// The regression: every reply in the thread was deleted, so the
			// delete path recomputed the parent's tcount to 0.
			name: "thread parent with tcount zero",
			msg: models.Message{
				MessageID: "m1", RoomID: "r1", CreatedAt: at,
				TCount: &zero, ThreadRoomID: "t1",
			},
		},
		{
			name: "thread parent with non-zero tcount",
			msg: models.Message{
				MessageID: "m2", RoomID: "r1", CreatedAt: at,
				TCount: &two, ThreadRoomID: "t1", ThreadLastMsgAt: &at,
			},
		},
		{
			name: "message with no thread",
			msg: models.Message{
				MessageID: "m3", RoomID: "r1", CreatedAt: at, Msg: "plain",
			},
		},
		{
			// Reactions is the struct-keyed, marshal-only map gob was chosen for.
			name: "message with reactions",
			msg: models.Message{
				MessageID: "m4", RoomID: "r1", CreatedAt: at,
				Reactions: cassandra.Reactions{
					{Emoji: "👍", UserAccount: "alice"}: {UserID: "u1", Account: "alice", ReactedAt: at},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := []models.Message{tt.msg}

			blob, err := encode(in)
			require.NoError(t, err)
			require.NotEmpty(t, blob)
			require.Equal(t, tagBucket, blob[0])

			out, err := decode(blob[1:])
			require.NoError(t, err)

			assert.Equal(t, in, out,
				"a cached read must serve the same field values a live read does")
		})
	}
}
