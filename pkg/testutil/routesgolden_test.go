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
		name    string
		routes  map[natsmetrics.RPCMethod]string
		wantErr bool
	}{
		{
			name:   "only declared methods",
			routes: map[natsmetrics.RPCMethod]string{natsmetrics.MethodOpenRoom: "chat.user.{account}.request.room.open"},
		},
		{
			name:   "empty table",
			routes: map[natsmetrics.RPCMethod]string{},
		},
		{
			name:    "degraded route alone",
			routes:  map[natsmetrics.RPCMethod]string{natsmetrics.MethodOther: "chat.user.{account}.request.room.typo"},
			wantErr: true,
		},
		{
			name: "degraded route beside a good one",
			routes: map[natsmetrics.RPCMethod]string{
				natsmetrics.MethodOpenRoom: "chat.user.{account}.request.room.open",
				natsmetrics.MethodOther:    "chat.user.{account}.request.room.typo",
			},
			wantErr: true,
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
			assert.Contains(t, err.Error(), "chat.user.{account}.request.room.typo",
				"the message must name the offending pattern so the route is findable")
		})
	}
}
