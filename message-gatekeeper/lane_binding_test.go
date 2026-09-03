package main

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingConn stands in for one NATS connection, so a test can tell which of
// the two a handler actually spoke on.
type recordingConn struct {
	published []string
	replied   []string
}

func (c *recordingConn) deps() laneDeps {
	return laneDeps{
		publish: func(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			c.published = append(c.published, msg.Subject)
			return &jetstream.PubAck{}, nil
		},
		reply: func(_ context.Context, msg *nats.Msg) error {
			c.replied = append(c.replied, msg.Subject)
			return nil
		},
	}
}

func laneTestConfig() *config {
	return &config{
		SiteID:             "site-a",
		LargeRoomThreshold: 500,
		MaxAttachments:     1,
		MaxAttachmentBytes: 8192,
		ChatBaseURL:        "http://chat",
	}
}

// A handler's publisher and replier are fixed at construction, so each lane
// needs its own. The failover lane exists precisely because the home cluster is
// unreachable: a handler that consumed on the buddy but published on home would
// validate the message and then drop it into a dead connection, and the client's
// reply would go the same way — silently, with the send appearing to hang.
func TestNewLaneHandler_SpeaksOnItsOwnConnection(t *testing.T) {
	home := &recordingConn{}
	buddy := &recordingConn{}
	cfg := laneTestConfig()

	homeHandler := newLaneHandler(home.deps(), nil, nil, cfg)
	buddyHandler := newLaneHandler(buddy.deps(), nil, nil, cfg)

	ctx := context.Background()
	_, err := homeHandler.publish(ctx, &nats.Msg{Subject: "canonical.home"})
	require.NoError(t, err)
	require.NoError(t, homeHandler.reply(ctx, &nats.Msg{Subject: "reply.home"}))

	_, err = buddyHandler.publish(ctx, &nats.Msg{Subject: "canonical.buddy"})
	require.NoError(t, err)
	require.NoError(t, buddyHandler.reply(ctx, &nats.Msg{Subject: "reply.buddy"}))

	// Each connection saw exactly its own lane's traffic and nothing else: the
	// failing shape this guards against is both lanes sharing one handler, which
	// puts all four subjects on the home connection.
	assert.Equal(t, []string{"canonical.home"}, home.published)
	assert.Equal(t, []string{"reply.home"}, home.replied)
	assert.Equal(t, []string{"canonical.buddy"}, buddy.published)
	assert.Equal(t, []string{"reply.buddy"}, buddy.replied)
}
