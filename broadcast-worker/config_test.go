package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

// TestConfig_ParsesStreamEnv verifies INPUT_STREAM / INPUT_SUBJECT_FILTER /
// CONSUMER_NAME parse into the typed config so the same binary can be
// deployed a second time with bot-canonical env values.
func TestConfig_ParsesStreamEnv(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.>")
	t.Setenv("CONSUMER_NAME", "broadcast-worker")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "MESSAGES_CANONICAL_site-a", cfg.InputStream)
	require.Equal(t, "chat.msg.canonical.site-a.>", cfg.InputSubjectFilter)
	require.Equal(t, "broadcast-worker", cfg.ConsumerName)
}

// TestConfig_ConsumerNameDefault verifies a user-side deployment can omit
// CONSUMER_NAME and still get today's hardcoded "broadcast-worker" durable.
func TestConfig_ConsumerNameDefault(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.>")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "broadcast-worker", cfg.ConsumerName)
}

// TestConfig_MissingInputStream_Errors verifies main fails fast at startup
// when INPUT_STREAM is unset rather than silently binding to an empty stream.
func TestConfig_MissingInputStream_Errors(t *testing.T) {
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.>")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}

// TestConfig_MissingInputSubjectFilter_Errors verifies main fails fast at
// startup when INPUT_SUBJECT_FILTER is unset rather than binding an
// unfiltered consumer.
func TestConfig_MissingInputSubjectFilter_Errors(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}
