package subauthcache

import "time"

// TTLConfig is this tier's retention knob, declared here rather than in each
// consuming service so the services sharing this cache key cannot disagree
// about it. They read and write the same entries: a service configured shorter
// re-validates on a different clock than its peers and expires entries they
// still expect to be serveable.
//
// Add it as a named field and pass TTL where the tier is built. Zero disables
// the tier.
//
// Mounted under an envPrefix by each consumer, so the two services reading this
// key keep their existing GATEKEEPER_ / HISTORY_ variable names.
type TTLConfig struct {
	TTL time.Duration `env:"SUB_L2_TTL" envDefault:"90m"`
}
