package atrest

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// BreakerConfig fences the Mongo DEK fetch, and is declared here for the same
// reason TTLConfig is: more than one service reads this collection, so the env
// names and the defaults belong to the thing they configure rather than to each
// consumer. Two services had declared the identical pair, and had already
// diverged — one validated the range, the other did not, so a negative value
// meant different things depending on which binary read it.
//
// Kept separate from a service's MONGO_BREAKER_* budget on purpose: the DEK
// fetch and the service's other Mongo reads are independent failure signals,
// and sharing one budget would let either reset the other.
//
// Add it as a named field, call Validate() during config load, and build the
// breaker with New.
type BreakerConfig struct {
	// Fails is the consecutive-failure budget before the breaker opens. 0
	// disables fencing entirely: calls always pass through.
	Fails int `env:"ATREST_DEK_BREAKER_FAILS" envDefault:"5"`
	// Cooldown is how long an open breaker fences calls before admitting one
	// half-open probe.
	Cooldown time.Duration `env:"ATREST_DEK_BREAKER_COOLDOWN" envDefault:"10s"`
}

// Validate rejects negative values. Zero is legal for both and means "no
// fencing", which a deployment may want; negative means nothing.
func (b BreakerConfig) Validate() error {
	if b.Fails < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_FAILS must be >= 0, got %d", b.Fails)
	}
	if b.Cooldown < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_COOLDOWN must be >= 0, got %s", b.Cooldown)
	}
	return nil
}

// New builds the DEK breaker, reporting under name on the shared state gauge.
//
// No failure predicate: a DEK miss is not a healthy absence the way a missing
// room is — a room whose key cannot be found has messages that cannot be
// opened — so every error counts, as both services already had it.
func (b BreakerConfig) New(ctx context.Context, name string) *circuitbreaker.Breaker {
	return circuitbreaker.New(b.Fails, b.Cooldown, circuitbreaker.Tracked(ctx, name))
}
