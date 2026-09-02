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

// captureOutbox records the subjects a handler's outbox fan-out published to.
func captureOutbox(subjects *[]string) PublishFunc {
	return func(_ context.Context, subj string, _ []byte, _ string) error {
		*subjects = append(*subjects, subj)
		return nil
	}
}

// The failover lane runs because this site's own NATS is down, and the live
// OUTBOX buffer lives on exactly that cluster. A mention federated to the live
// outbox subject from the failover lane would go nowhere, so the remote
// mentionee's badge would be lost for the whole outage.
func TestHandler_FederateMentions_FailoverLaneUsesFailoverOutbox(t *testing.T) {
	var subjects []string
	h := &Handler{siteID: "site-a", publish: captureOutbox(&subjects), outboxFailover: true}

	ctx := natsutil.WithRequestID(context.Background(), testMentionRequestID)
	h.federateMentions(ctx, "room-1", "msg-1", remoteParticipants(1), time.Now())

	require.Len(t, subjects, 1)
	assert.Equal(t, subject.FailoverOutbox("site-a", "site-0", "subscription_mention"), subjects[0])
	assert.NotEqual(t, subject.Outbox("site-a", "site-0", "subscription_mention"), subjects[0],
		"the live OUTBOX is on the cluster that is down")
}

// The control: the home lane keeps the live outbox subject, so the failover
// wiring cannot regress steady-state federation.
func TestHandler_FederateMentions_HomeLaneUsesLiveOutbox(t *testing.T) {
	var subjects []string
	h := &Handler{siteID: "site-a", publish: captureOutbox(&subjects)}

	ctx := natsutil.WithRequestID(context.Background(), testMentionRequestID)
	h.federateMentions(ctx, "room-1", "msg-1", remoteParticipants(1), time.Now())

	require.Len(t, subjects, 1)
	assert.Equal(t, subject.Outbox("site-a", "site-0", "subscription_mention"), subjects[0])
}

// withOutboxFederation carries the lane through to the handler, so main cannot
// build a failover handler that still targets the live outbox.
func TestWithOutboxFederation_CarriesLane(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failover bool
	}{
		{"home lane", false},
		{"failover lane", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts handlerOptions
			withOutboxFederation("site-a", captureOutbox(new([]string)), tc.failover)(&opts)
			assert.Equal(t, tc.failover, opts.outboxFailover)
			assert.Equal(t, "site-a", opts.siteID)
		})
	}
}
