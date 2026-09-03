package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// The failover lane runs because this site's own NATS is down, and the live
// OUTBOX buffer sits on exactly that cluster. A thread-subscription event
// federated to the live outbox subject from the failover lane would go nowhere,
// so the remote owner's thread subscription would never be created.
func TestHandler_ThreadSubOutbox_PublishesOnItsLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		lane subject.Lane
		want string
	}{
		{"home lane keeps the live outbox", subject.LaneHome, subject.Outbox("site-a", "site-b", "thread_subscription_upserted")},
		{"failover lane uses the standby outbox", subject.LaneFailover, subject.FailoverOutbox("site-a", "site-b", "thread_subscription_upserted")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var subjects []string
			h := NewHandler(nil, nil, nil, "site-a", func(_ context.Context, subj string, _ []byte, _ string) error {
				subjects = append(subjects, subj)
				return nil
			}, withLane(tc.lane))

			sub := &model.ThreadSubscription{ThreadRoomID: "t-1", UserID: "u-1", RoomID: "r-1"}
			require.NoError(t, h.publishThreadSubInboxIfRemote(context.Background(), sub, "site-b", "m-1"))

			assert.Equal(t, []string{tc.want}, subjects)
		})
	}
}
