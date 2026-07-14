package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEventTypeSetsAreDisjoint(t *testing.T) {
	seen := make(map[model.InboxEventType]string)
	for _, et := range ConcurrentEventTypes {
		seen[et] = "concurrent"
	}
	for _, et := range OrderedEventTypes {
		require.NotContains(t, seen, et,
			"event type %q must appear in exactly one filter set — overlapping filters would double-forward", et)
		seen[et] = "ordered"
	}
}

func TestPublish_WrapsAndPublishesWithDedupMsgID(t *testing.T) {
	var gotSubj, gotMsgID string
	var gotData []byte
	publish := func(_ context.Context, subj string, data []byte, msgID string) error {
		gotSubj, gotData, gotMsgID = subj, data, msgID
		return nil
	}

	payload := []byte(`{"x":1}`)
	err := Publish(context.Background(), publish, "site-a", "r1", "site-b", model.InboxMemberAdded, payload, "dedup-1", 7)
	require.NoError(t, err)

	assert.Equal(t, "chat.outbox.site-a.site-b.member_added", gotSubj)
	assert.Equal(t, "dedup-1", gotMsgID, "the dedup ID must ride the OUTBOX publish as its Nats-Msg-Id")

	var evt model.OutboxEvent
	require.NoError(t, json.Unmarshal(gotData, &evt))
	assert.Equal(t, "r1", evt.RoomID)
	assert.Equal(t, "dedup-1", evt.DedupID)
	assert.Positive(t, evt.Timestamp)

	// Publish owns the envelope build, so every producer relays the same shape.
	var env model.InboxEvent
	require.NoError(t, json.Unmarshal(evt.Envelope, &env))
	assert.Equal(t, model.InboxMemberAdded, env.Type)
	assert.Equal(t, "site-a", env.SiteID)
	assert.Equal(t, "site-b", env.DestSiteID)
	assert.Equal(t, int64(7), env.Timestamp)
	assert.JSONEq(t, `{"x":1}`, string(env.Payload))
}

func TestPublish_NoopForEmptyOrLocalDestination(t *testing.T) {
	called := false
	publish := func(context.Context, string, []byte, string) error { called = true; return nil }
	require.NoError(t, Publish(context.Background(), publish, "site-a", "r1", "", model.InboxMemberAdded, []byte(`{}`), "d", 1))
	require.NoError(t, Publish(context.Background(), publish, "site-a", "r1", "site-a", model.InboxMemberAdded, []byte(`{}`), "d", 1))
	assert.False(t, called, "an empty or local destination has nothing to federate and must not publish")
}

func TestPublish_RejectsEventTypeOutsideThePartition(t *testing.T) {
	err := Publish(context.Background(), func(context.Context, string, []byte, string) error { return nil },
		"site-a", "r1", "site-b", model.InboxThreadSubscriptionUpserted, []byte(`{}`), "d", 1)
	require.Error(t, err,
		"an event type with no outbox-worker filter would sit in the stream unconsumed — must fail fast at the publish site")
	assert.Contains(t, err.Error(), "filter set")
}

func TestPublish_RejectsEmptyDedupID(t *testing.T) {
	publish := func(context.Context, string, []byte, string) error { return nil }
	err := Publish(context.Background(), publish, "site-a", "r1", "site-b", model.InboxMemberAdded, []byte(`{}`), "", 1)
	assert.Error(t, err, "without a dedup id the forward loses its Nats-Msg-Id and outbox-worker Ack-drops it")
}

func TestPublish_WrapsPublishError(t *testing.T) {
	publish := func(context.Context, string, []byte, string) error {
		return assert.AnError
	}
	err := Publish(context.Background(), publish, "site-a", "r1", "site-b", model.InboxSubscriptionRead, []byte(`{}`), "d", 1)
	require.ErrorIs(t, err, assert.AnError)
}
