package main

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

// envelope marshals an errcode error to its wire shape ({error, code, reason}) the way
// natsrouter replies it, so the test exercises the exact bytes history-service sends.
func envelope(t *testing.T, e *errcode.Error) []byte {
	t.Helper()
	data, err := json.Marshal(e)
	require.NoError(t, err)
	return data
}

func TestClassifyHistoryReply(t *testing.T) {
	t.Parallel()

	okAck, err := json.Marshal(model.MigrationAck{OK: true})
	require.NoError(t, err)
	notOKAck, err := json.Marshal(model.MigrationAck{OK: false})
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
		// wantErr: nil = Ack; poison = Term; plain = Nak (retry).
		wantNil      bool
		wantPoison   bool
		wantTermCode errcode.Code
	}{
		{
			name:    "success ack acks",
			data:    okAck,
			wantNil: true,
		},
		{
			name:       "not-found is retryable (Nak) — the not-yet-persisted convergence case",
			data:       envelope(t, errcode.NotFound("message not yet persisted, retry")),
			wantPoison: false,
		},
		{
			name:       "internal is retryable (Nak)",
			data:       envelope(t, errcode.Internal("boom")),
			wantPoison: false,
		},
		{
			name:       "unavailable is retryable (Nak)",
			data:       envelope(t, errcode.Unavailable("down")),
			wantPoison: false,
		},
		{
			name:       "too-many-requests is retryable (Nak)",
			data:       envelope(t, errcode.TooManyRequests("slow down")),
			wantPoison: false,
		},
		{
			name:         "bad-request is permanent (Term) + term code",
			data:         envelope(t, errcode.BadRequest("malformed")),
			wantPoison:   true,
			wantTermCode: errcode.CodeBadRequest,
		},
		{
			name:         "forbidden is permanent (Term) + term code",
			data:         envelope(t, errcode.Forbidden("no")),
			wantPoison:   true,
			wantTermCode: errcode.CodeForbidden,
		},
		{
			name:         "conflict is permanent (Term) + term code",
			data:         envelope(t, errcode.Conflict("dupe")),
			wantPoison:   true,
			wantTermCode: errcode.CodeConflict,
		},
		{
			name:         "unauthenticated is permanent (Term) + term code",
			data:         envelope(t, errcode.Unauthenticated("nope")),
			wantPoison:   true,
			wantTermCode: errcode.CodeUnauthenticated,
		},
		{
			name:       "unknown/foreign code is retryable (Nak) — never Term on uncertainty",
			data:       []byte(`{"error":"weird","code":"banana"}`),
			wantPoison: false,
		},
		{
			name:       "ack not ok is retryable (Nak)",
			data:       notOKAck,
			wantPoison: false,
		},
		{
			name:       "undecodable reply is retryable (Nak), never Term-dropped",
			data:       []byte(`{not valid json`),
			wantPoison: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			termCode, err := classifyHistoryReply("chat.migration.internal.s1.msg.edit", tc.data)
			if tc.wantNil {
				assert.NoError(t, err)
				assert.Empty(t, termCode)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantPoison, errors.Is(err, migration.ErrPoison),
				"migration.ErrPoison membership drives Term vs Nak")
			assert.Equal(t, tc.wantTermCode, termCode)
		})
	}
}
