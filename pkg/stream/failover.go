package stream

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// FailoverStreamManager is the JetStream subset EnsureFailoverStream needs,
// narrowed so a unit test can fake it without a NATS server.
//
// It does not mirror o11ynats.JetStream directly: that returns a Stream whose
// Info is a second call, and Go has no covariant returns, so a narrower
// interface cannot be satisfied structurally. FailoverJS adapts the real thing.
type FailoverStreamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error
	StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)
}

// FailoverJS adapts an o11y JetStream context to FailoverStreamManager, so each
// service binding a standby lane writes one line rather than its own adapter.
func FailoverJS(js o11ynats.JetStream) FailoverStreamManager { return o11yFailoverJS{js: js} }

type o11yFailoverJS struct{ js o11ynats.JetStream }

// The by-value StreamConfig mirrors jetstream.JetStream's own signature; taking
// it by pointer here would make the interface diverge from the API it wraps.
//
//nolint:gocritic // signature mirrors the wrapped jetstream API
func (a o11yFailoverJS) CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	_, err := a.js.CreateOrUpdateStream(ctx, cfg)
	return err
}

func (a o11yFailoverJS) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	s, err := a.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.Info(ctx)
}

// EnsureFailoverStream readies a standby stream on a buddy connection.
//
// In dev (bootstrapEnabled) it creates the stream and does NOT assert placement:
// a single-server NATS reports no cluster at all, and there is no buddy to be
// wrong about.
//
// In production it verifies the stream exists AND is hosted by expectedCluster.
// The second half is the one that matters: names are unique supercluster-wide,
// so a lookup succeeds no matter which cluster hosts the asset — a standby
// stream sitting on the very cluster it exists to outlive would pass an
// existence check and fail only during the outage it was built for.
//
// Shared by every service that binds a standby lane, so no service can ship a
// variant that skips the placement check.
func EnsureFailoverStream(ctx context.Context, js FailoverStreamManager, cfg Config,
	bootstrapEnabled bool, expectedCluster string,
) error {
	if bootstrapEnabled {
		if err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     cfg.Name,
			Subjects: cfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create failover stream %s: %w", cfg.Name, err)
		}
		return nil
	}

	info, err := js.StreamInfo(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("verify failover stream %s: %w", cfg.Name, err)
	}
	if err := CheckPlacement(info, expectedCluster); err != nil {
		return fmt.Errorf("failover stream %s placement: %w", cfg.Name, err)
	}
	return nil
}
