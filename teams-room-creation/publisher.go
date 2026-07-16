package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// publishFunc publishes one room-creation batch to subj with a JetStream
// dedup id. Injected into the runner so unit tests capture batches without a
// real NATS connection.
type publishFunc func(ctx context.Context, subj string, data []byte, dedupID string) error

// dedupID is a deterministic Nats-Msg-Id for a batch: identical (site, chat-id
// set) always yields the same id regardless of chat order, so a re-published
// un-flipped batch is deduplicated server-side.
func dedupID(siteID string, chatIDs []string) string {
	sorted := append([]string(nil), chatIDs...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, ",")))
	return fmt.Sprintf("teamroom:%s:%s", siteID, hex.EncodeToString(sum[:]))
}

// newJetStreamPublisher returns a publishFunc that publishes via JetStream and
// blocks on the PubAck, honoring dedupID as Nats-Msg-Id. js is the o11y-traced
// JetStream handle from natsutil.Connect(...).JetStream() (see main.go),
// matching the o11ynats.JetStream parameter used by every other production
// publisher wrapper in the repo (e.g. user-service/publisher,
// history-service/internal/publisher) — not the raw nats-io jetstream.JetStream.
func newJetStreamPublisher(js o11ynats.JetStream) publishFunc {
	return func(ctx context.Context, subj string, data []byte, dedup string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(dedup)); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}
}
