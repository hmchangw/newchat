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

// publishFunc is the minimal JetStream publish surface (oteljetstream.JetStream.PublishMsg).
type publishFunc func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

// canonicalPublisher emits migrated inserts onto the canonical .created subject.
type canonicalPublisher struct {
	siteID  string
	publish publishFunc
	now     func() int64
}

// publishInsert publishes a migrated message as a MessageEvent{created}, blocking on the pub-ack.
// X-Migration: live suppresses live delivery; dedup id = message ID so replays collapse.
//
//nolint:gocritic // model.Message is passed by value to match the canonical event envelope; one per insert, off the hot path.
func (p *canonicalPublisher) publishInsert(ctx context.Context, msg model.Message) error {
	evt := model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: p.siteID, Timestamp: p.now()}
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal message event: %w", err)
	}
	m := natsutil.NewMsg(ctx, subject.MsgCanonicalCreated(p.siteID), data)
	natsutil.SetMigrationLive(m)
	if _, err := p.publish(ctx, m, jetstream.WithMsgID(msg.ID)); err != nil {
		return fmt.Errorf("publish canonical created: %w", err)
	}
	return nil
}
