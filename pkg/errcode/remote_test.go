package errcode

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromReply(t *testing.T) {
	tests := []struct {
		name string
		data []byte

		wantErr       bool
		wantTyped     bool // the caller may errors.As it back to *Error
		wantCode      Code
		wantContains  []string
		wantReasonSet bool
	}{
		{
			name: "a successful payload is not an error",
			data: []byte(`{"rooms":[]}`),
		},
		{
			name: "an empty payload is not an error",
			data: []byte(`{}`),
		},
		{
			name: "malformed JSON is not an error envelope",
			data: []byte("{not json"),
		},
		{
			name: "an envelope with no message is not an error envelope",
			data: []byte(`{"code":"not_found"}`),
		},
		{
			name:      "a canonical code comes back typed",
			data:      mustMarshal(t, NotFound("room not found")),
			wantErr:   true,
			wantTyped: true,
			wantCode:  CodeNotFound,
		},
		{
			name:          "a canonical code keeps its reason",
			data:          mustMarshal(t, Forbidden("nope", WithReason(Reason("room_not_owner")))),
			wantErr:       true,
			wantTyped:     true,
			wantCode:      CodeForbidden,
			wantReasonSet: true,
		},
		{
			// The whole point. Parse recognises the envelope; Valid() does not
			// recognise the code. Treating that as "not an error" let the reply
			// fall through to a decoder that ignores unknown fields, producing a
			// zero-value success. Relaying it typed is the other failure: it
			// puts a code outside the closed set into an API that requires one,
			// and errcode.New panics on exactly that.
			name:         "an unknown code is an error, but not a typed one",
			data:         []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			wantErr:      true,
			wantTyped:    false,
			wantContains: []string{"upstream_only_code", "upstream boom"},
		},
		{
			name:         "an empty code is unknown too",
			data:         []byte(`{"code":"","error":"boom"}`),
			wantErr:      true,
			wantTyped:    false,
			wantContains: []string{"boom"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := FromReply(tt.data)

			if !tt.wantErr {
				require.NoError(t, err, "a non-envelope payload must not produce an error")
				return
			}
			require.Error(t, err)

			var typed *Error
			if tt.wantTyped {
				require.ErrorAs(t, err, &typed, "a canonical code must survive as *errcode.Error")
				assert.Equal(t, tt.wantCode, typed.Code)
				if tt.wantReasonSet {
					assert.NotEmpty(t, typed.Reason, "the remote reason must survive")
				}
				return
			}

			assert.False(t, errors.As(err, &typed),
				"an unknown code must not be relayed as *errcode.Error — errcode.New panics on a "+
					"non-canonical code, and the boundary writers assume the closed set")
			for _, want := range tt.wantContains {
				assert.Contains(t, err.Error(), want,
					"the message must carry enough to diagnose a version skew")
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	out, err := json.Marshal(v)
	require.NoError(t, err)
	return out
}
