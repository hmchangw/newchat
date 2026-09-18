package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// The failover lane runs because this site's own NATS is down, and the live
// OUTBOX buffer lives on exactly that cluster. A mention federated to the live
// outbox subject from the failover lane would go nowhere, so the remote
// mentionee's badge would be lost for the whole outage.
func TestHandler_FederateMentions_PublishesOnItsLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		lane subject.Lane
		want string
	}{
		{"home lane keeps the live outbox", subject.LaneHome, subject.Outbox("site-a", "site-0", "subscription_mention")},
		{"failover lane uses the standby outbox", subject.LaneFailover, subject.FailoverOutbox("site-a", "site-0", "subscription_mention")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &mentionOutboxRecorder{}
			h := NewHandler(nil, nil, nil, nil, nil, false, nil,
				withBroadcastMetrics(nil), withOutboxFederation("site-a", rec.publish, tc.lane))

			ctx := natsutil.WithRequestID(context.Background(), testMentionRequestID)
			h.federateMentions(ctx, "room-1", "msg-1", remoteParticipants(1), time.Now())

			require.Len(t, rec.records, 1)
			assert.Equal(t, tc.want, rec.records[0].subject)
		})
	}
}
