package main

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// publisher is the narrow sync-publish surface mobileEmitter needs.
// Sync semantics let the handler nak on publish failure; {messageId}-b{N} dedup
// protects against duplicate emission of batches that already succeeded.
type publisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg) error
}

// Emitter dispatches one batched push event per ~RecipientBatchSize recipients.
type Emitter interface {
	Emit(ctx context.Context, evt model.PushNotificationEvent) error
}

type mobileEmitter struct {
	pub             publisher
	siteID          string
	maxPayloadBytes int
}

func newMobileEmitter(pub publisher, siteID string, maxPayloadBytes int) *mobileEmitter {
	return &mobileEmitter{pub: pub, siteID: siteID, maxPayloadBytes: maxPayloadBytes}
}

func (e *mobileEmitter) Emit(ctx context.Context, evt model.PushNotificationEvent) error { //nolint:gocritic // hugeParam: spec requires value semantics for Emitter interface
	data, err := sonic.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal push batch %s: %w", evt.ID, err)
	}
	if e.maxPayloadBytes > 0 && len(data) > e.maxPayloadBytes {
		return fmt.Errorf("push batch %s exceeds NATS max_payload: wire=%d, cap=%d", evt.ID, len(data), e.maxPayloadBytes)
	}
	msg := &nats.Msg{
		Subject: subject.PushNotification(e.siteID),
		Header:  nats.Header{},
		Data:    data,
	}
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set("Nats-Msg-Id", evt.ID) // dedup key — see contract doc § Dedup
	if err := e.pub.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish push batch %s: %w", evt.ID, err)
	}
	return nil
}

// jsPublisher adapts oteljetstream.JetStream to the publisher interface by discarding the PubAck.
type jsPublisher struct {
	js interface {
		PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	}
}

func (p *jsPublisher) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	_, err := p.js.PublishMsg(ctx, msg)
	return err
}
