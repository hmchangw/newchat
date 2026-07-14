package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// jetstreamPublisher publishes InboxEvents into the local INBOX stream.
type jetstreamPublisher struct {
	publish func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Publish emits one InboxEvent onto the INBOX external lane, blocking on the pub-ack. The request
// id flows from ctx into the message headers (natsutil.NewMsg) so transformer→inbox-worker shares it.
//
//nolint:gocritic // model.InboxEvent passed by value: one per migrated record, off the hot path.
func (p *jetstreamPublisher) Publish(ctx context.Context, evt model.InboxEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal inbox event: %w", err)
	}
	m := natsutil.NewMsg(ctx, subject.InboxExternal(evt.DestSiteID, evt.Type), data)
	if _, err := p.publish(ctx, m); err != nil {
		return fmt.Errorf("publish inbox external: %w", err)
	}
	return nil
}
