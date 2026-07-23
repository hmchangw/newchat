package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"
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

// bootstrapStreams creates the input+output streams when enabled (dev/integration), otherwise
// verifies the input stream exists so a misconfigured deploy fails at startup; identities are env-driven.
func bootstrapStreams(ctx context.Context, js streamManager, inputStream, inputSubject, outputStream, outputSubjectPrefix string, enabled bool) error {
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     inputStream,
			Subjects: []string{inputSubject},
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", inputStream, err)
		}
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     outputStream,
			Subjects: []string{outputSubjectPrefix + ".>"},
			// S2 storage compression — transparent to publisher/consumer; ~2× ratio on JSON at near-zero CPU. Shrinks inter-replica wire bytes and on-disk bytes.
			Compression: jetstream.S2Compression,
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", outputStream, err)
		}
		return nil
	}
	// Output stream absence is non-fatal: async publish surfaces errors per-publish.
	if _, err := js.Stream(ctx, inputStream); err != nil {
		return fmt.Errorf("verify stream %s: %w", inputStream, err)
	}
	return nil
}
