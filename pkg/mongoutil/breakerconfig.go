package mongoutil

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// BreakerConfig is an env-tagged MongoDB circuit-breaker configuration, the
// companion to PoolConfig: the pool bounds how long one call waits, this bounds
// how many calls pay that wait before the service stops trying.
//
// Add it as a named field, call Validate() during config load, and build
// breakers with New. A service that prefixes its knobs puts an envPrefix on the
// field — the tags carry the full MONGO_ name, so an envPrefix of HISTORY_
// reads HISTORY_MONGO_BREAKER_FAILS, exactly as before the field existed.
//
// Six services had declared the same two knobs, under four different env
// prefixes, and two spellings of the same field name.
type BreakerConfig struct {
	// Fails is the consecutive-failure budget before the breaker opens. 0
	// disables fencing entirely: calls always pass through.
	Fails int `env:"MONGO_BREAKER_FAILS" envDefault:"5"`
	// Cooldown is how long an open breaker fences calls before admitting one
	// half-open probe.
	Cooldown time.Duration `env:"MONGO_BREAKER_COOLDOWN" envDefault:"10s"`
}

// Validate rejects negative values. Zero is legal for both and means "no
// fencing", which a deployment may want; negative means nothing.
//
// envPrefix is the field's own envPrefix ("" when unprefixed), so the message
// names the variable the operator actually set rather than the shared suffix.
// A wrapped error would lose that: the reader has to reassemble the name.
func (b BreakerConfig) Validate(envPrefix string) error {
	if b.Fails < 0 {
		return fmt.Errorf("%sMONGO_BREAKER_FAILS must be >= 0, got %d", envPrefix, b.Fails)
	}
	if b.Cooldown < 0 {
		return fmt.Errorf("%sMONGO_BREAKER_COOLDOWN must be >= 0, got %s", envPrefix, b.Cooldown)
	}
	return nil
}

// New builds a breaker from this config, reporting under name on the shared
// state gauge. Extra options are applied after, so a caller can supply a
// failure predicate.
//
// It deliberately imposes no predicate of its own: whether a not-found counts is
// a per-call-site decision, and defaulting it here would silently change two
// services that count one today. Pass BreakerFailure explicitly.
func (b BreakerConfig) New(ctx context.Context, name string, opts ...circuitbreaker.Option) *circuitbreaker.Breaker {
	return circuitbreaker.New(b.Fails, b.Cooldown,
		append([]circuitbreaker.Option{circuitbreaker.Tracked(ctx, name)}, opts...)...)
}

// BreakerFailure is the failure predicate for a Mongo-backed store: every error
// counts except mongo.ErrNoDocuments and the caller's own "healthy absence"
// sentinels. A missing document is an answer from a working database, not
// evidence it is unwell.
//
// It exists so the sentinel list is the only thing a service writes. Four
// services had each declared their own FailureExcept(mongo.ErrNoDocuments, …),
// which meant the load-bearing part — Canceled exempt, DeadlineExceeded NOT
// exempt, the asymmetry that lets the breaker see an unreachable MongoDB at all
// — was re-derived per service rather than shared.
func BreakerFailure(extra ...error) func(error) bool {
	return circuitbreaker.FailureExcept(append([]error{mongo.ErrNoDocuments}, extra...)...)
}
