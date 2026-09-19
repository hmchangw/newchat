package failoverlane

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// Which standby streams a lane readies is the one thing BuddyLane decides on
// its own, so it is tested directly rather than through a bind.
func TestBuddyLane_streamsToEnsure(t *testing.T) {
	tests := []struct {
		name string
		lane BuddyLane
		want []string
	}{
		{
			name: "defaults to just the consumed stream",
			lane: BuddyLane{Stream: stream.MessagesCanonicalFailover("site-a")},
			want: []string{"MESSAGES-CANONICAL-FAILOVER-site-a"},
		},
		{
			// Both have to exist before the first failover message arrives, or
			// the work would be consumed and then have nowhere to go.
			name: "consumed stream comes first, then the ones it publishes to",
			lane: BuddyLane{
				Stream:      stream.MessagesFailover("site-a"),
				PublishesTo: []stream.Config{stream.MessagesCanonicalFailover("site-a")},
			},
			want: []string{"MESSAGES-FAILOVER-site-a", "MESSAGES-CANONICAL-FAILOVER-site-a"},
		},
		{
			// A service that does not own its standby stream must not create it
			// and must not assert a placement its owner is responsible for;
			// binding the consumer is its existence check.
			name: "a borrowed stream is left to its owner",
			lane: BuddyLane{
				Stream:       stream.PushNotificationFailover("site-a"),
				BorrowStream: true,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.lane.streamsToEnsure()
			require.Len(t, got, len(tt.want))
			for i, name := range tt.want {
				assert.Equal(t, name, got[i].Name)
			}
		})
	}
}
