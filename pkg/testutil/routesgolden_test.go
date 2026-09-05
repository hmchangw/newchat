package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// The fallback must be refused before the golden file is touched, so a
// degraded route can never be written into one or hand-added to one.
func TestRejectFallbackMethod(t *testing.T) {
	tests := []struct {
		name      string
		routes    []natsmetrics.RPCRoute
		wantErr   bool
		wantNamed []string
	}{
		{
			name:   "only declared methods",
			routes: []natsmetrics.RPCRoute{{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.open"}},
		},
		{
			name:   "empty table",
			routes: nil,
		},
		{
			name:    "degraded route alone",
			routes:  []natsmetrics.RPCRoute{{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"}},
			wantErr: true,
		},
		{
			name: "degraded route beside a good one",
			routes: []natsmetrics.RPCRoute{
				{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.open"},
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"},
			},
			wantErr: true,
		},
		{
			name: "two degraded routes are both named",
			routes: []natsmetrics.RPCRoute{
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"},
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo2"},
			},
			wantErr:   true,
			wantNamed: []string{"chat.user.{account}.request.room.typo", "chat.user.{account}.request.room.typo2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectFallbackMethod(tt.routes)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), string(natsmetrics.MethodOther))
			named := tt.wantNamed
			if named == nil {
				named = []string{"chat.user.{account}.request.room.typo"}
			}
			for _, pattern := range named {
				assert.Contains(t, err.Error(), pattern,
					"the message must name every offending pattern so each route is findable")
			}
		})
	}
}

// Two patterns that differ only in placeholder spelling subscribe to one
// subject, so both handlers are live and NATS splits the traffic. Comparing
// Pattern passed this pair; comparing NATSSubject does not.
func TestRejectDuplicateSubject(t *testing.T) {
	const subject = "chat.user.*.request.settings.get"

	err := rejectDuplicateSubject([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodGetSettings, Pattern: "chat.user.{account}.request.settings.get", NATSSubject: subject},
		{Method: natsmetrics.MethodGetChatlist, Pattern: "chat.user.{user}.request.settings.get", NATSSubject: subject},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), subject)
	assert.Contains(t, err.Error(), "{account}")
	assert.Contains(t, err.Error(), "{user}",
		"both patterns must be named, or the reader cannot tell which registration to remove")

	require.NoError(t, rejectDuplicateSubject([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodGetSettings, Pattern: "chat.user.{account}.request.settings.get", NATSSubject: subject},
		{Method: natsmetrics.MethodGetChatlist, Pattern: "chat.user.{account}.request.chatlist.get", NATSSubject: "chat.user.*.request.chatlist.get"},
	}), "distinct subjects must pass")
}

// service_name + rpc_method must identify one handler. Two routes sharing a
// method merge into a series no dashboard can split, so the golden file
// showing it is not enough — a regeneration would absorb it.
func TestRejectDuplicateMethod(t *testing.T) {
	err := rejectDuplicateMethod([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.open", NATSSubject: "a"},
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.archive", NATSSubject: "b"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), string(natsmetrics.MethodOpenRoom))
	assert.Contains(t, err.Error(), "archive", "both routes must be named")

	require.NoError(t, rejectDuplicateMethod([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: "a", NATSSubject: "a"},
		{Method: natsmetrics.MethodGetRoomAppTabs, Pattern: "b", NATSSubject: "b"},
	}))
}
