package main

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// publishFunc publishes one room-creation batch to subj. Injected into the
// runner so unit tests capture batches without a real NATS connection.
type publishFunc func(ctx context.Context, subj string, data []byte) error

// newJetStreamPublisher returns a publishFunc that publishes via JetStream and
// blocks on the PubAck. js is the o11y-traced JetStream handle from
// natsutil.Connect(...).JetStream() (see main.go), matching the o11ynats.JetStream
// parameter used by every other production publisher wrapper in the repo (e.g.
// user-service/publisher, history-service/internal/publisher) — not the raw
// nats-io jetstream.JetStream.
//
// No Nats-Msg-Id / publish-side dedup: this is a CronJob that re-runs
// minutes-to-hours later, far outside any JetStream Duplicates window, so
// server-side dedup never fired across runs anyway. The guarantee against
// duplicate room creation is the downstream room-worker being idempotent on
// chat id.
func newJetStreamPublisher(js o11ynats.JetStream) publishFunc {
	return func(ctx context.Context, subj string, data []byte) error {
		msg := natsutil.NewMsg(ctx, subj, natsutil.EncodeZstd(data))
		// NewMsg returns a nil Header when ctx carries no request-id; guard
		// before flagging the payload as zstd.
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set(natsutil.HeaderNatsEncoding, natsutil.EncodingZstd)
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}
}
