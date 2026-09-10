package natsmetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsValidRPCMethod pins the shape rule that replaced the closed const
// vocabulary. There is no list to check a method against any more — the set the
// fleet uses is whatever its route tables declare — so the naming rule itself is
// the only thing left to enforce, and it has to be enforced here rather than in
// natsrouter so the record site can bound a value that never passed a table.
func TestIsValidRPCMethod(t *testing.T) {
	tests := []struct {
		name   string
		method RPCMethod
		want   bool
	}{
		{name: "a verb-first snake_case name is valid", method: "list_channel_messages", want: true},
		{name: "a single word is valid", method: "me", want: true},
		{name: "a digit inside a word is valid", method: "get_room_v2", want: true},
		{name: "the zero value is not", method: MethodNone, want: false},
		{name: "the fallback is not, so no route can claim it", method: MethodOther, want: false},
		{name: "CamelCase is not", method: "ListChannelMessages", want: false},
		{name: "a raw subject is not", method: "chat.user.alice.request.room.r1", want: false},
		{name: "a hyphen is not", method: "list-channel-messages", want: false},
		{name: "a leading underscore is not", method: "_list_messages", want: false},
		{name: "a trailing underscore is not", method: "list_messages_", want: false},
		{name: "a doubled underscore is not", method: "list__messages", want: false},
		{name: "a leading digit is not", method: "2_messages", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsValidRPCMethod(tt.method))
		})
	}
}

// TestNormalizeRPCMethod keeps an unusable label bounded at the record site
// rather than minting a series from it or dropping the sample.
func TestNormalizeRPCMethod(t *testing.T) {
	tests := []struct {
		name   string
		method RPCMethod
		want   RPCMethod
	}{
		{name: "a declared method passes through", method: "list_channel_messages", want: "list_channel_messages"},
		{name: "the zero value is bounded", method: MethodNone, want: MethodOther},
		{name: "the fallback stays itself", method: MethodOther, want: MethodOther},
		{name: "an unusable value is bounded", method: "ListChannelMessages", want: MethodOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeRPCMethod(tt.method))
		})
	}
}
