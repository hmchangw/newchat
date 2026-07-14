package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestJetstreamPublisher_Publish(t *testing.T) {
	const site = "s1"

	t.Run("publishes event to correct subject with correct data", func(t *testing.T) {
		var captured *nats.Msg
		pub := &jetstreamPublisher{
			publish: func(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
				captured = msg
				return &jetstream.PubAck{}, nil
			},
		}

		// Distinct SiteID (home) vs DestSiteID (routing) so a source/dest mix-up in subject
		// routing would be caught — the subject must use DestSiteID.
		evt := model.InboxEvent{
			Type:       "room_sync",
			SiteID:     "home-site",
			DestSiteID: site,
			Payload:    []byte(`{"id":"r1"}`),
			Timestamp:  1700000000000,
		}

		err := pub.Publish(context.Background(), evt)
		require.NoError(t, err)
		require.NotNil(t, captured)

		wantSubject := subject.InboxExternal(evt.DestSiteID, "room_sync")
		assert.Equal(t, wantSubject, captured.Subject, "subject must route on DestSiteID, not SiteID")

		var got model.InboxEvent
		require.NoError(t, json.Unmarshal(captured.Data, &got))
		assert.Equal(t, evt.Type, got.Type)
		assert.Equal(t, evt.SiteID, got.SiteID)
		assert.Equal(t, evt.DestSiteID, got.DestSiteID)
		assert.Equal(t, evt.Timestamp, got.Timestamp)
		assert.JSONEq(t, string(evt.Payload), string(got.Payload))
	})

	t.Run("publish error propagates as wrapped error", func(t *testing.T) {
		publishErr := errors.New("nats down")
		pub := &jetstreamPublisher{
			publish: func(_ context.Context, _ *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
				return nil, publishErr
			},
		}

		evt := model.InboxEvent{
			Type:      "room_sync",
			SiteID:    site,
			Timestamp: 1700000000000,
		}

		err := pub.Publish(context.Background(), evt)
		require.Error(t, err)
		assert.ErrorContains(t, err, "publish inbox external")
		assert.ErrorIs(t, err, publishErr)
	})
}
