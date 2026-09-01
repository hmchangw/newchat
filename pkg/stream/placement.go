package stream

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// CheckPlacement verifies a stream is hosted by the expected NATS cluster.
//
// A standby failover stream placed on the very cluster it exists to outlive is
// silently fatal: js.Stream() resolves any stream from any cluster (names are
// unique supercluster-wide), so an existence check passes and the
// misconfiguration only surfaces during the outage it was meant to survive.
// Comparing the hosting cluster is the only check that catches it — and it also
// catches a ring disagreement, where ops provisioned against a different buddy
// than the service is configured with.
//
// Callers pass an already-fetched StreamInfo so this stays a pure function.
// Only the production path should call it: a single-server dev NATS reports no
// cluster at all, which is an error here by design.
func CheckPlacement(info *jetstream.StreamInfo, expectedCluster string) error {
	if info == nil || info.Cluster == nil {
		return fmt.Errorf("stream placement unknown: no cluster info (want cluster %q)", expectedCluster)
	}
	if info.Cluster.Name != expectedCluster {
		return fmt.Errorf("stream %q hosted by cluster %q, want %q",
			info.Config.Name, info.Cluster.Name, expectedCluster)
	}
	return nil
}
