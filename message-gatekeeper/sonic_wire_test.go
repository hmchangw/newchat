package main

import (
	stdjson "encoding/json"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// TestSonicDecodesClientRequest guards the untrusted-input path: clients marshal
// SendMessageRequest with their own (stdlib-class) encoder, and gatekeeper now
// decodes it with sonic. Assert sonic decodes a client request to the same value
// stdlib does, including HTML-metachar content.
func TestSonic_DecodesClientRequest(t *testing.T) {
	orig := model.SendMessageRequest{
		ID:                    "01H8XGJ9ABCDEF01234567",
		Content:               `if a < b && c > d then "x" else <tag>`,
		RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ThreadParentMessageID: "01H8XGJ0PARENT0000000",
		TShow:                 true,
	}
	wire, err := stdjson.Marshal(orig)
	require.NoError(t, err)

	var viaStd, viaSonic model.SendMessageRequest
	require.NoError(t, stdjson.Unmarshal(wire, &viaStd))
	require.NoError(t, sonic.Unmarshal(wire, &viaSonic))
	assert.Equal(t, viaStd, viaSonic, "sonic must decode client request identically to stdlib")
	assert.Equal(t, orig, viaSonic, "round-trip must preserve the request")
}
