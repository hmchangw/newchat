package atrest

import (
	"context"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/testutil"
)

// breakerHolder mounts BreakerConfig the way a service config does.
type breakerHolder struct {
	Cfg BreakerConfig
}

// The env names and defaults are the contract every service fencing this
// collection shares. They moved here from two service configs that had already
// diverged on validation; changing either here changes both, which is the point.
func TestBreakerConfig_EnvNames(t *testing.T) {
	t.Setenv("ATREST_DEK_BREAKER_FAILS", "9")
	t.Setenv("ATREST_DEK_BREAKER_COOLDOWN", "45s")

	cfg, err := env.ParseAs[breakerHolder]()

	require.NoError(t, err)
	assert.Equal(t, 9, cfg.Cfg.Fails)
	assert.Equal(t, 45*time.Second, cfg.Cfg.Cooldown)
}

func TestBreakerConfig_Defaults(t *testing.T) {
	testutil.UnsetEnv(t, "ATREST_DEK_BREAKER_FAILS")
	testutil.UnsetEnv(t, "ATREST_DEK_BREAKER_COOLDOWN")

	cfg, err := env.ParseAs[breakerHolder]()

	require.NoError(t, err)
	assert.Equal(t, 5, cfg.Cfg.Fails)
	assert.Equal(t, 10*time.Second, cfg.Cfg.Cooldown)
}

// Zero is legal on both — a deployment may want no fencing — and negative is
// meaningless. message-worker validated neither before this moved here.
func TestBreakerConfig_Validate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		cfg     BreakerConfig
		wantErr string
	}{
		{"zero is legal", BreakerConfig{}, ""},
		{"positive is legal", BreakerConfig{Fails: 3, Cooldown: time.Second}, ""},
		{"negative fails", BreakerConfig{Fails: -1}, "ATREST_DEK_BREAKER_FAILS"},
		{"negative cooldown", BreakerConfig{Cooldown: -time.Second}, "ATREST_DEK_BREAKER_COOLDOWN"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestBreakerConfig_New(t *testing.T) {
	b := BreakerConfig{Fails: 1, Cooldown: time.Minute}.New(context.Background(), "atrestdek")

	require.NotNil(t, b)
	assert.Equal(t, circuitbreaker.StateClosed, b.State())
}
