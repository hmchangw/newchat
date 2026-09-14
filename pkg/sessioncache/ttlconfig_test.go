package sessioncache

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// ttlHolder mounts TTLConfig the way a service config does.
type ttlHolder struct {
	Cfg TTLConfig
}

// The env name and the default are this tier's contract with every service
// sharing its cache key. Both moved here from the service configs; changing
// either desynchronises services that must agree, so both are pinned.
func TestTTLConfig_EnvName(t *testing.T) {
	t.Setenv("SESSION_CACHE_TTL", "45m")

	cfg, err := env.ParseAs[ttlHolder]()

	require.NoError(t, err)
	assert.Equal(t, 45*time.Minute, cfg.Cfg.TTL)
}

func TestTTLConfig_Default(t *testing.T) {
	testutil.UnsetEnv(t, "SESSION_CACHE_TTL")

	cfg, err := env.ParseAs[ttlHolder]()

	require.NoError(t, err)
	assert.Equal(t, 90*time.Minute, cfg.Cfg.TTL, "the L2 tiers are sized together at 90m")
}
