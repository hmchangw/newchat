package jsiter

import (
	"context"
	"fmt"

	"github.com/hmchangw/chat/pkg/health"
)

// Check builds the "this consumer is up" readiness probe.
func Check(name string, up func() bool) health.Check {
	return health.Check{
		Name: "jetstream-consumer:" + name,
		Probe: func(context.Context) error {
			if !up() {
				return fmt.Errorf("%s consumer is down", name)
			}
			return nil
		},
	}
}
