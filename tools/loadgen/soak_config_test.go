package main

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakConfig_Defaults(t *testing.T) {
	cfg := mustDefaultSoakConfig(t)

	assert.Equal(t, "", cfg.RunID)
	assert.Equal(t, 72*time.Hour, cfg.RunDuration)
	assert.Equal(t, 30*time.Second, cfg.Warmup)
	assert.Equal(t, 100.0, cfg.SendRate)
	assert.Equal(t, 700.0, cfg.ReadRate)
	assert.Equal(t, 0.10, cfg.ThreadShare)
	assert.Equal(t, 5.0, cfg.MutationRate)
	assert.Equal(t, 0.001, cfg.SoftDeleteRatio)
	assert.Equal(t, 100.0, cfg.ReactionRate)
	assert.Equal(t, 30, cfg.ReactionsPerHotMessage)
	assert.Equal(t, "hot_only", cfg.ReactionMessageScope)
	assert.Equal(t, 0.20, cfg.ReactionRemoveShare)
	assert.Equal(t, 1.0, cfg.PinnedListRate)
	assert.Equal(t, 1.0, cfg.VerifyRate)
	assert.Equal(t, 20000, cfg.MaxUsers)
	assert.Equal(t, 2000, cfg.ActiveUsers)
	assert.Equal(t, 10000, cfg.RoomCount)
	assert.Equal(t, 0.30, cfg.ChannelRatio)
	assert.Equal(t, 100, cfg.ChannelMembers)
	assert.Equal(t, "site", cfg.RateScope)
	assert.Equal(t, 0.0, cfg.MessagesPerActiveUserPerDay)
	assert.Equal(t, 1024, cfg.PayloadMedianBytes)
	assert.Equal(t, 2048, cfg.PayloadP95Bytes)
	assert.Equal(t, 10240, cfg.PayloadMaxBytes)
	assert.Equal(t, 10*time.Second, cfg.PersistGrace)
	assert.Equal(t, 3, cfg.MutationRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.RetryMinBackoff)
	assert.Equal(t, 5*time.Second, cfg.RetryMaxBackoff)
	assert.Equal(t, 128, cfg.RecentPerRoom)
	assert.Equal(t, 200000, cfg.RecentTotal)
	assert.Equal(t, "none", cfg.CassandraCleanup)
	assert.Empty(t, cfg.ConfirmKeyspace)
}

func TestSoakConfig_EnvironmentOverrides(t *testing.T) {
	cfg, err := env.ParseAsWithOptions[config](env.Options{
		Environment: map[string]string{
			"NATS_URL":                              "nats://example.invalid",
			"MONGO_URI":                             "mongodb://example.invalid",
			"SOAK_RUN_ID":                           "run-20260724",
			"SOAK_RUN_DURATION":                     "4h",
			"SOAK_SEND_RATE":                        "125.5",
			"SOAK_REACTION_MESSAGE_SCOPE":           "all_messages",
			"SOAK_RATE_SCOPE":                       "global",
			"SOAK_CASSANDRA_CLEANUP":                "truncate",
			"SOAK_CONFIRM_KEYSPACE":                 "chat_soak",
			"SOAK_RETRY_MIN_BACKOFF":                "250ms",
			"SOAK_MESSAGES_PER_ACTIVE_USER_PER_DAY": "42.5",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "run-20260724", cfg.Soak.RunID)
	assert.Equal(t, 4*time.Hour, cfg.Soak.RunDuration)
	assert.Equal(t, 125.5, cfg.Soak.SendRate)
	assert.Equal(t, "all_messages", cfg.Soak.ReactionMessageScope)
	assert.Equal(t, "global", cfg.Soak.RateScope)
	assert.Equal(t, "truncate", cfg.Soak.CassandraCleanup)
	assert.Equal(t, "chat_soak", cfg.Soak.ConfirmKeyspace)
	assert.Equal(t, 250*time.Millisecond, cfg.Soak.RetryMinBackoff)
	assert.Equal(t, 42.5, cfg.Soak.MessagesPerActiveUserPerDay)
}

func TestConfig_ExistingCommandsDoNotRequireSoakRunID(t *testing.T) {
	cfg, err := env.ParseAsWithOptions[config](env.Options{
		Environment: map[string]string{
			"NATS_URL":  "nats://example.invalid",
			"MONGO_URI": "mongodb://example.invalid",
		},
	})
	require.NoError(t, err)
	assert.Empty(t, cfg.Soak.RunID)
}

func TestValidateSoakConfig_RequiresRunID(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RunID = " "

	err := validateSoakConfig(&cfg, "chat")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_RUN_ID")
}

func TestValidateSoakConfig_RejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		change func(*soakConfig)
		want   string
	}{
		{"negative send rate", func(c *soakConfig) { c.SendRate = -1 }, "SOAK_SEND_RATE"},
		{"negative read rate", func(c *soakConfig) { c.ReadRate = -1 }, "SOAK_READ_RATE"},
		{"negative mutation rate", func(c *soakConfig) { c.MutationRate = -1 }, "SOAK_MUTATION_RATE"},
		{"negative reaction rate", func(c *soakConfig) { c.ReactionRate = -1 }, "SOAK_REACTION_RATE"},
		{"negative pinned list rate", func(c *soakConfig) { c.PinnedListRate = -1 }, "SOAK_PINNED_LIST_RATE"},
		{"negative verify rate", func(c *soakConfig) { c.VerifyRate = -1 }, "SOAK_VERIFY_RATE"},
		{"thread share above one", func(c *soakConfig) { c.ThreadShare = 1.01 }, "SOAK_THREAD_SHARE"},
		{"soft delete ratio below zero", func(c *soakConfig) { c.SoftDeleteRatio = -0.01 }, "SOAK_SOFT_DELETE_RATIO"},
		{"reaction remove share above one", func(c *soakConfig) { c.ReactionRemoveShare = 1.01 }, "SOAK_REACTION_REMOVE_SHARE"},
		{"channel ratio above one", func(c *soakConfig) { c.ChannelRatio = 1.01 }, "SOAK_CHANNEL_RATIO"},
		{"zero run duration", func(c *soakConfig) { c.RunDuration = 0 }, "SOAK_RUN_DURATION"},
		{"negative warmup", func(c *soakConfig) { c.Warmup = -time.Second }, "SOAK_WARMUP"},
		{"warmup equals duration", func(c *soakConfig) { c.Warmup = c.RunDuration }, "SOAK_WARMUP"},
		{"negative persist grace", func(c *soakConfig) { c.PersistGrace = -time.Second }, "SOAK_PERSIST_GRACE"},
		{"negative mutation retries", func(c *soakConfig) { c.MutationRetries = -1 }, "SOAK_MUTATION_RETRIES"},
		{"zero retry minimum", func(c *soakConfig) { c.RetryMinBackoff = 0 }, "SOAK_RETRY_MIN_BACKOFF"},
		{"retry maximum below minimum", func(c *soakConfig) { c.RetryMaxBackoff = c.RetryMinBackoff / 2 }, "SOAK_RETRY_MAX_BACKOFF"},
		{"zero per-room catalog", func(c *soakConfig) { c.RecentPerRoom = 0 }, "SOAK_RECENT_PER_ROOM"},
		{"global catalog below per-room catalog", func(c *soakConfig) { c.RecentTotal = c.RecentPerRoom - 1 }, "SOAK_RECENT_TOTAL"},
		{"max users exceeds borrowed-user cap", func(c *soakConfig) { c.MaxUsers = 20001 }, "SOAK_MAX_USERS"},
		{"active users exceed max users", func(c *soakConfig) { c.ActiveUsers = c.MaxUsers + 1 }, "SOAK_ACTIVE_USERS"},
		{"zero room count", func(c *soakConfig) { c.RoomCount = 0 }, "SOAK_ROOM_COUNT"},
		{"channel has fewer than two members", func(c *soakConfig) { c.ChannelMembers = 1 }, "SOAK_CHANNEL_MEMBERS"},
		{"channel members exceed users", func(c *soakConfig) { c.ChannelMembers = c.MaxUsers + 1 }, "SOAK_CHANNEL_MEMBERS"},
		{"zero reactions per hot message", func(c *soakConfig) { c.ReactionsPerHotMessage = 0 }, "SOAK_REACTIONS_PER_HOT_MESSAGE"},
		{"reactions per hot message exceed active users", func(c *soakConfig) { c.ReactionsPerHotMessage = c.ActiveUsers + 1 }, "SOAK_REACTIONS_PER_HOT_MESSAGE"},
		{"negative messages per active user", func(c *soakConfig) { c.MessagesPerActiveUserPerDay = -1 }, "SOAK_MESSAGES_PER_ACTIVE_USER_PER_DAY"},
		{"payload median is zero", func(c *soakConfig) { c.PayloadMedianBytes = 0 }, "SOAK_PAYLOAD_MEDIAN_BYTES"},
		{"payload p95 below median", func(c *soakConfig) { c.PayloadP95Bytes = c.PayloadMedianBytes - 1 }, "SOAK_PAYLOAD_P95_BYTES"},
		{"payload max below p95", func(c *soakConfig) { c.PayloadMaxBytes = c.PayloadP95Bytes - 1 }, "SOAK_PAYLOAD_MAX_BYTES"},
		{"unknown reaction scope", func(c *soakConfig) { c.ReactionMessageScope = "popular-ish" }, "SOAK_REACTION_MESSAGE_SCOPE"},
		{"unknown rate scope", func(c *soakConfig) { c.RateScope = "cluster-ish" }, "SOAK_RATE_SCOPE"},
		{"unknown cleanup mode", func(c *soakConfig) { c.CassandraCleanup = "drop" }, "SOAK_CASSANDRA_CLEANUP"},
		{"truncate without confirmation", func(c *soakConfig) {
			c.CassandraCleanup = "truncate"
			c.ConfirmKeyspace = ""
		}, "SOAK_CONFIRM_KEYSPACE"},
		{"truncate with mismatched confirmation", func(c *soakConfig) {
			c.CassandraCleanup = "truncate"
			c.ConfirmKeyspace = "wrong"
		}, "SOAK_CONFIRM_KEYSPACE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			tt.change(&cfg)

			err := validateSoakConfig(&cfg, "chat")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestValidateSoakConfig_AcceptsGuardedTruncate(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.CassandraCleanup = "truncate"
	cfg.ConfirmKeyspace = "chat_soak"

	require.NoError(t, validateSoakConfig(&cfg, "chat_soak"))
}

func TestLogSoakAssumptions_PrintsProvisionalInputs(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.ReactionMessageScope = "all_messages"
	cfg.RateScope = "global"
	cfg.MessagesPerActiveUserPerDay = 42.5

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	logSoakAssumptions(&cfg)

	assert.Contains(t, output.String(), "provisional")
	assert.Contains(t, output.String(), `"i8ReactionMessageScope":"all_messages"`)
	assert.Contains(t, output.String(), `"i10RateScope":"global"`)
	assert.Contains(t, output.String(), `"i12MessagesPerActiveUserPerDay":42.5`)
	assert.Contains(t, output.String(), `"i12Derived":false`)

	output.Reset()
	cfg.MessagesPerActiveUserPerDay = 0
	cfg.SendRate = 100
	cfg.ActiveUsers = 2000
	logSoakAssumptions(&cfg)

	assert.Contains(t, output.String(), `"i12MessagesPerActiveUserPerDay":4320`)
	assert.Contains(t, output.String(), `"i12Derived":true`)
}

func mustDefaultSoakConfig(t *testing.T) soakConfig {
	t.Helper()
	cfg, err := env.ParseAsWithOptions[soakConfig](env.Options{
		Prefix:      "SOAK_",
		Environment: map[string]string{},
	})
	require.NoError(t, err)
	return cfg
}

func validSoakConfig(t *testing.T) soakConfig {
	t.Helper()
	cfg := mustDefaultSoakConfig(t)
	cfg.RunID = "run-a-test"
	return cfg
}
