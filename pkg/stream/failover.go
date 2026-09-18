package stream

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
)

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
func EnsureFailoverStream(ctx context.Context, js o11ynats.JetStream, cfg Config,
	bootstrapEnabled bool, expectedCluster string,
) error {
	return ensureFailoverStream(ctx, o11yStreams{js: js}, cfg, bootstrapEnabled, expectedCluster)
}

// failoverStreams is the JetStream surface ensureFailoverStream needs, narrowed
// so a unit test can drive it without a NATS server. It cannot be satisfied by
// o11ynats.JetStream structurally — that returns a Stream rather than the info
// directly, and Go has no covariant returns — hence o11yStreams.
type failoverStreams interface {
	create(ctx context.Context, cfg *jetstream.StreamConfig) error
	info(ctx context.Context, name string) (*jetstream.StreamInfo, error)
}

type o11yStreams struct{ js o11ynats.JetStream }

func (a o11yStreams) create(ctx context.Context, cfg *jetstream.StreamConfig) error {
	_, err := a.js.CreateOrUpdateStream(ctx, *cfg)
	return err
}

func (a o11yStreams) info(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	s, err := a.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	// Stream() has just fetched this; asking again would be a second
	// cross-cluster round trip on the boot path for the same answer.
	if info := s.CachedInfo(); info != nil {
		return info, nil
	}
	return s.Info(ctx)
}

func ensureFailoverStream(ctx context.Context, js failoverStreams, cfg Config,
	bootstrapEnabled bool, expectedCluster string,
) error {
	if bootstrapEnabled {
		if err := js.create(ctx, &jetstream.StreamConfig{
			Name:     cfg.Name,
			Subjects: cfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create failover stream %s: %w", cfg.Name, err)
		}
		return nil
	}

	info, err := js.info(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("verify failover stream %s: %w", cfg.Name, err)
	}
	if err := CheckPlacement(info, expectedCluster); err != nil {
		return fmt.Errorf("failover stream %s placement: %w", cfg.Name, err)
	}
	return nil
}
