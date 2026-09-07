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
		{
			// The third way this goes wrong, and the one Parse alone cannot see:
			// the envelope does not decode at all. Metadata is map[string]string,
			// so one numeric value fails the whole unmarshal, Parse says "not an
			// envelope", and the reply falls through to the success decoder — the
			// same zero-value-success fail-open the unknown-code branch exists to
			// stop. Whether a field decodes is a question about this build's
			// struct, not about whether the call failed.
			name:         "an envelope whose fields do not decode is still an error",
			data:         []byte(`{"code":"unavailable","error":"upstream down","metadata":{"retryAfter":5}}`),
			wantErr:      true,
			wantTyped:    false,
			wantContains: []string{"upstream down"},
		},
		{
			name:         "a non-string code does not make the envelope a success",
			data:         []byte(`{"code":123,"error":"numeric code"}`),
			wantErr:      true,
			wantTyped:    false,
			wantContains: []string{"numeric code"},
		},
		{
			name:         "a non-string reason does not make the envelope a success",
			data:         []byte(`{"code":"unavailable","error":"boom","reason":42}`),
			wantErr:      true,
			wantTyped:    false,
			wantContains: []string{"boom"},
		},
		{
			// The boundary, stated so it is a decision rather than an oversight:
			// the envelope contract is a non-empty string "error". A payload whose
			// "error" is not a string carries no message to relay and matches no
			// producer in this repo, so it stays a non-envelope. Widening past the
			// contract would start turning success payloads that happen to carry
			// an "error" key of another shape into failures.
			name: "a non-string error field is not an envelope",
			data: []byte(`{"error":["not","a","string"],"code":"unavailable"}`),
		},
		{
			name:      "metadata survives on the decodable path",
			data:      []byte(`{"code":"not_found","error":"room not found","metadata":{"roomId":"r1"}}`),
			wantErr:   true,
			wantTyped: true,
			wantCode:  CodeNotFound,
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
