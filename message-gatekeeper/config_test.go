package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// setRequiredEnv seeds the env vars marked required so ParseAs can succeed and
// the test can assert on the field it actually cares about.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
}

// The breaker knobs moved onto the shared mongoutil.BreakerConfig, mounted under
// this service's own envPrefix. The operator-facing names must be byte-identical
// to what they were before the move — this pins that, since a silent rename
// would leave a tuned deployment running the default.
func TestConfig_BreakerEnvNamesUnchanged(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GATEKEEPER_MONGO_BREAKER_FAILS", "9")
	t.Setenv("GATEKEEPER_MONGO_BREAKER_COOLDOWN", "45s")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, 9, cfg.Breaker.Fails)
	require.Equal(t, 45*time.Second, cfg.Breaker.Cooldown)
}

func TestConfig_BreakerDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, 5, cfg.Breaker.Fails)
	require.Equal(t, 10*time.Second, cfg.Breaker.Cooldown)
}

// The L2 retentions moved onto each tier package's TTLConfig so the services
// sharing a cache key cannot disagree about them. The composed env names must
// be byte-identical to what they were before the move.
func TestConfig_L2TTLEnvNamesUnchanged(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ROOM_META_L2_TTL", "11m")
	t.Setenv("GATEKEEPER_SUB_L2_TTL", "22m")
	t.Setenv("USER_L2_TTL", "33m")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, 11*time.Minute, cfg.RoomMetaL2.TTL)
	require.Equal(t, 22*time.Minute, cfg.SubL2.TTL)
	require.Equal(t, 33*time.Minute, cfg.UserL2.TTL)
}

func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_READ_PREFERENCE", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.ReadPreference,
		"gatekeeper authorises sends from Mongo; a primary-only read takes messaging down with the primary")

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

func TestConfig_ReadPreferenceRejectsGarbage(t *testing.T) {
	_, err := mongoutil.ParseReadPreference("quorum")
	require.Error(t, err)
}

func TestConfig_ThreadParentRecheckDelayDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, defaultThreadParentRecheckDelay, cfg.ThreadParentRecheckDelay,
		"a reply racing its parent's write must get one re-check before the send is rejected")
}

func TestConfig_ThreadParentRecheckDelayIsTunable(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY", "0s")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Zero(t, cfg.ThreadParentRecheckDelay, "operators can switch the re-check off")
}
