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

// A pattern registered twice leaves both subscriptions live, so a request is
// answered by whichever handler NATS picks. Keyed collections hid this; the
// route list does not.
func TestRejectDuplicatePattern(t *testing.T) {
	const pattern = "chat.user.{account}.request.room.{roomID}.site-a.open"

	err := rejectDuplicatePattern([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: pattern},
		{Method: natsmetrics.MethodGetRoomAppTabs, Pattern: pattern},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), pattern)
	assert.Contains(t, err.Error(), string(natsmetrics.MethodOpenRoom))
	assert.Contains(t, err.Error(), string(natsmetrics.MethodGetRoomAppTabs),
		"both spellings must be named, or the reader cannot tell which registration to remove")

	require.NoError(t, rejectDuplicatePattern([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: pattern},
		{Method: natsmetrics.MethodGetRoomAppTabs, Pattern: pattern + ".tabs"},
	}), "distinct patterns sharing nothing must pass")
}
