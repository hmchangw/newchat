package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// captureLanePublish records the subjects a handler federated to.
func captureLanePublish(subjects *[]string) PublishFunc {
	return func(_ context.Context, subj string, _ []byte, _ string) error {
		*subjects = append(*subjects, subj)
		return nil
	}
}

// The failover lane runs because this site's own NATS is down, and the live
// OUTBOX buffer sits on exactly that cluster. A thread-subscription event
// federated to the live outbox subject from the failover lane would go nowhere,
// so the remote owner's thread subscription would never be created.
func TestHandler_ThreadSubOutbox_FailoverLaneUsesFailoverOutbox(t *testing.T) {
	var subjects []string
	h := &Handler{siteID: "site-a", publish: captureLanePublish(&subjects), outboxFailover: true}

	sub := &model.ThreadSubscription{ThreadRoomID: "t-1", UserID: "u-1", RoomID: "r-1"}
	require.NoError(t, h.publishThreadSubInboxIfRemote(context.Background(), sub, "site-b", "m-1"))

	require.Len(t, subjects, 1)
	assert.Equal(t, subject.FailoverOutbox("site-a", "site-b", "thread_subscription_upserted"), subjects[0])
	assert.NotEqual(t, subject.Outbox("site-a", "site-b", "thread_subscription_upserted"), subjects[0],
		"the live OUTBOX is on the cluster that is down")
}

// The control: the home lane keeps the live outbox subject.
func TestHandler_ThreadSubOutbox_HomeLaneUsesLiveOutbox(t *testing.T) {
	var subjects []string
	h := &Handler{siteID: "site-a", publish: captureLanePublish(&subjects)}

	sub := &model.ThreadSubscription{ThreadRoomID: "t-1", UserID: "u-1", RoomID: "r-1"}
	require.NoError(t, h.publishThreadSubInboxIfRemote(context.Background(), sub, "site-b", "m-1"))

	require.Len(t, subjects, 1)
	assert.Equal(t, subject.Outbox("site-a", "site-b", "thread_subscription_upserted"), subjects[0])
}

// withOutboxLane carries the lane through to the handler, so main cannot build a
// failover handler that still buffers into the live outbox.
func TestWithOutboxLane_CarriesLane(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failover bool
	}{
		{"home lane", false},
		{"failover lane", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(nil, nil, nil, "site-a", captureLanePublish(new([]string)),
				withOutboxLane(tc.failover))
			assert.Equal(t, tc.failover, h.outboxFailover)
		})
	}
}
