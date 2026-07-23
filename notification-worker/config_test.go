package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

// TestConfig_ParsesStreamEnv verifies INPUT_STREAM / INPUT_SUBJECT_FILTER /
// CONSUMER_NAME / OUTPUT_STREAM / OUTPUT_SUBJECT_PREFIX parse into the typed
// config so the same binary can be deployed a second time with bot-canonical
// / bot-push env values (Task 8).
func TestConfig_ParsesStreamEnv(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.created")
	t.Setenv("CONSUMER_NAME", "notification-worker")
	t.Setenv("OUTPUT_STREAM", "PUSH_NOTIFICATION_site-a")
	t.Setenv("OUTPUT_SUBJECT_PREFIX", "chat.server.notification.push.site-a")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "MESSAGES_CANONICAL_site-a", cfg.InputStream)
	require.Equal(t, "chat.msg.canonical.site-a.created", cfg.InputSubjectFilter)
	require.Equal(t, "notification-worker", cfg.ConsumerName)
	require.Equal(t, "PUSH_NOTIFICATION_site-a", cfg.OutputStream)
	require.Equal(t, "chat.server.notification.push.site-a", cfg.OutputSubjectPrefix)
}

// TestConfig_ConsumerNameDefault verifies a user-side deployment can omit
// CONSUMER_NAME and still get today's hardcoded "notification-worker" durable.
func TestConfig_ConsumerNameDefault(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.created")
	t.Setenv("OUTPUT_STREAM", "PUSH_NOTIFICATION_site-a")
	t.Setenv("OUTPUT_SUBJECT_PREFIX", "chat.server.notification.push.site-a")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "notification-worker", cfg.ConsumerName)
}

// TestConfig_MissingInputStream_Errors verifies main fails fast at startup
// when INPUT_STREAM is unset rather than silently binding to an empty stream.
func TestConfig_MissingInputStream_Errors(t *testing.T) {
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.created")
	t.Setenv("OUTPUT_STREAM", "PUSH_NOTIFICATION_site-a")
	t.Setenv("OUTPUT_SUBJECT_PREFIX", "chat.server.notification.push.site-a")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}

// TestConfig_MissingInputSubjectFilter_Errors verifies main fails fast at
// startup when INPUT_SUBJECT_FILTER is unset rather than binding an
// unfiltered consumer.
func TestConfig_MissingInputSubjectFilter_Errors(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("OUTPUT_STREAM", "PUSH_NOTIFICATION_site-a")
	t.Setenv("OUTPUT_SUBJECT_PREFIX", "chat.server.notification.push.site-a")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}

// TestConfig_MissingOutputStream_Errors verifies main fails fast at startup
// when OUTPUT_STREAM is unset rather than bootstrapping against an empty
// stream name.
func TestConfig_MissingOutputStream_Errors(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.created")
	t.Setenv("OUTPUT_SUBJECT_PREFIX", "chat.server.notification.push.site-a")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}

// TestConfig_MissingOutputSubjectPrefix_Errors verifies main fails fast at
// startup when OUTPUT_SUBJECT_PREFIX is unset rather than publishing push
// events to an empty subject.
func TestConfig_MissingOutputSubjectPrefix_Errors(t *testing.T) {
	t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
	t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.created")
	t.Setenv("OUTPUT_STREAM", "PUSH_NOTIFICATION_site-a")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}
