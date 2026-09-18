package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates stream creation to dev/integration; leave Enabled false in production.
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the narrow JetStream surface bootstrapStreams uses, injected by tests.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams creates the input+output streams when enabled (dev/integration), otherwise verifies the
// input stream exists and the output stream's duplicate window covers the consumer's outage retry budget,
// so a misconfigured deploy fails at startup; identities are env-driven.
//
// Both streams arrive as stream.Config, never as loose name/subject strings: CreateOrUpdateStream NARROWS
// an existing stream, so passing a consumer's filter subject (this worker filters the .created leaf) would
// rebind MESSAGES-CANONICAL to that leaf and strip every other publisher's subjects. Taking the Config
// makes that mistake unrepresentable.
func bootstrapStreams(ctx context.Context, js streamManager, input, output stream.Config, enabled bool) error {
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     input.Name,
			Subjects: input.Subjects,
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", input.Name, err)
		}
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     output.Name,
			Subjects: output.Subjects,
			// S2 storage compression — transparent to publisher/consumer; ~2× ratio on JSON at near-zero CPU. Shrinks inter-replica wire bytes and on-disk bytes.
			Compression: jetstream.S2Compression,
			// The consumer's outage retry budget can redeliver a source message for about an hour; each
			// batch's Nats-Msg-Id only protects it while this window covers that span. Ops must match it.
			Duplicates: stream.OutageRetryWindow,
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", output.Name, err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, input.Name); err != nil {
		return fmt.Errorf("verify stream %s: %w", input.Name, err)
	}
	// A present output stream must carry the window above, or a redelivery after it republishes batches
	// the stream already accepted. Only ABSENCE is non-fatal (async publish surfaces that per-publish,
	// and nothing can be duplicated in a stream that does not exist); any other lookup error leaves the
	// window unverified, which is the failure this check exists to catch, so it must not pass for it.
	out, err := js.Stream(ctx, output.Name)
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		slog.WarnContext(ctx, "output stream absent at startup; its duplicate window is unverified",
			"stream", output.Name)
		return nil
	case err != nil:
		return fmt.Errorf("verify stream %s duplicate window: %w", output.Name, err)
	}
	info := out.CachedInfo()
	if info == nil {
		return fmt.Errorf("verify stream %s: no stream info", output.Name)
	}
	if d := info.Config.Duplicates; d < stream.OutageRetryWindow {
		return fmt.Errorf("stream %s duplicate window %s is shorter than the consumer's outage retry window %s; raise it before deploying",
			output.Name, d, stream.OutageRetryWindow)
	}
	return nil
}
