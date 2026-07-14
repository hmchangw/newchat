package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// outboxEvent marshals an OutboxEvent body carrying a pre-marshaled InboxEvent
// envelope for the given destination/event type, returning both so tests can
// assert the forward is envelope-verbatim.
func outboxEvent(t *testing.T, dest string, eventType model.InboxEventType, dedupID string) (data, envelope []byte) {
	t.Helper()
	env, err := json.Marshal(model.InboxEvent{Type: eventType, SiteID: "site-a", DestSiteID: dest, Payload: []byte(`{}`), Timestamp: 1})
	require.NoError(t, err)
	data, err = json.Marshal(model.OutboxEvent{RoomID: "r1", Envelope: env, DedupID: dedupID, Timestamp: 2})
	require.NoError(t, err)
	return data, env
}

func TestHandleEvent_ForwardsEnvelope(t *testing.T) {
	type pub struct {
		subj  string
		data  []byte
		msgID string
	}
	var pubs []pub
	h := NewHandler(func(_ context.Context, subj string, data []byte, msgID string) error {
		pubs = append(pubs, pub{subj, data, msgID})
		return nil
	})

	data, env := outboxEvent(t, "site-b", model.InboxSubscriptionMuteToggled, "req-1:site-b")

	subj := subject.Outbox("site-a", "site-b", model.InboxSubscriptionMuteToggled)
	require.NoError(t, h.HandleEvent(context.Background(), subj, data))
	require.Len(t, pubs, 1)
	// Destination + event type come from the subject, not the body.
	assert.Equal(t, "chat.inbox.site-b.external.subscription_mute_toggled", pubs[0].subj)
	assert.Equal(t, env, pubs[0].data)
	assert.Equal(t, "req-1:site-b", pubs[0].msgID)
}

func TestHandleEvent_PublishErrorReturnsForNak(t *testing.T) {
	h := NewHandler(func(_ context.Context, _ string, _ []byte, _ string) error {
		return errors.New("gateway down")
	})
	subj := subject.Outbox("site-a", "site-b", model.InboxSubscriptionRead)
	data, _ := outboxEvent(t, "site-b", model.InboxSubscriptionRead, "d")
	err := h.HandleEvent(context.Background(), subj, data)
	require.Error(t, err)
	_, permanent := errcode.IsPermanent(err)
	assert.False(t, permanent, "publish failure must be transient so jsretry Naks for redelivery")
}

func TestHandleEvent_MalformedBodyIsPermanent(t *testing.T) {
	h := NewHandler(func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	subj := subject.Outbox("site-a", "site-b", model.InboxSubscriptionRead)
	err := h.HandleEvent(context.Background(), subj, []byte("not json"))
	require.Error(t, err)
	_, permanent := errcode.IsPermanent(err)
	assert.True(t, permanent, "malformed body must Ack-poison, not Nak forever")
}

func TestHandleEvent_MalformedSubjectIsPermanent(t *testing.T) {
	h := NewHandler(func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	data, _ := outboxEvent(t, "site-b", model.InboxSubscriptionRead, "d")
	err := h.HandleEvent(context.Background(), "chat.outbox.site-a.site-b", data)
	require.Error(t, err)
	_, permanent := errcode.IsPermanent(err)
	assert.True(t, permanent, "malformed subject must Ack-poison, not Nak forever")
}

func TestHandleEvent_SkipsEventMissingDedupID(t *testing.T) {
	published := false
	h := NewHandler(func(_ context.Context, _ string, _ []byte, _ string) error {
		published = true
		return nil
	})
	// Empty DedupID would forward without a Nats-Msg-Id (no dedup), so the event
	// must be skipped rather than forwarded.
	subj := subject.Outbox("site-a", "site-b", model.InboxSubscriptionRead)
	data, _ := outboxEvent(t, "site-b", model.InboxSubscriptionRead, "")
	require.NoError(t, h.HandleEvent(context.Background(), subj, data))
	assert.False(t, published, "event with empty DedupID must be skipped, never forwarded without dedup")
}
