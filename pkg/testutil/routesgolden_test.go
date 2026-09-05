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
		routes    map[string]natsmetrics.RPCMethod
		wantErr   bool
		wantNamed []string
	}{
		{
			name:   "only declared methods",
			routes: map[string]natsmetrics.RPCMethod{"chat.user.{account}.request.room.open": natsmetrics.MethodOpenRoom},
		},
		{
			name:   "empty table",
			routes: map[string]natsmetrics.RPCMethod{},
		},
		{
			name:    "degraded route alone",
			routes:  map[string]natsmetrics.RPCMethod{"chat.user.{account}.request.room.typo": natsmetrics.MethodOther},
			wantErr: true,
		},
		{
			name: "degraded route beside a good one",
			routes: map[string]natsmetrics.RPCMethod{
				"chat.user.{account}.request.room.open": natsmetrics.MethodOpenRoom,
				"chat.user.{account}.request.room.typo": natsmetrics.MethodOther,
			},
			wantErr: true,
		},
		{
			name: "two degraded routes are both named",
			routes: map[string]natsmetrics.RPCMethod{
				"chat.user.{account}.request.room.typo":  natsmetrics.MethodOther,
				"chat.user.{account}.request.room.typo2": natsmetrics.MethodOther,
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
