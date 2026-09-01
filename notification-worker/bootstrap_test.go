package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeStreamManager struct {
	created  []jetstream.StreamConfig
	verified []string
	existing map[string]bool // streams that "exist" for the disabled path
	failOn   string          // stream name to fail on; empty = never fail
	failErr  error           // error to return when failing
}

func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	if f.failOn != "" && cfg.Name == f.failOn {
		return nil, f.failErr
	}
	f.created = append(f.created, cfg)
	return nil, nil
}

func (f *fakeStreamManager) Stream(_ context.Context, name string) (o11ynats.Stream, error) {
	f.verified = append(f.verified, name)
	if f.existing[name] {
		return nil, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

func (f *fakeStreamManager) createdNames() []string {
	var out []string
	for i := range f.created {
		out = append(out, f.created[i].Name)
	}
	return out
}

// testWiring is the real production wiring, so these tests pin the values
// main.go actually passes rather than hand-written stand-ins.
func testWiring() stream.Wiring { return stream.Resolve(stream.PipelineUser, "test") }

// TestBootstrapStreams_CreatesFullStreamSubjects is the regression pin for the
// narrowing bug: MESSAGES-CANONICAL must be created with its full binding
// (chat.msg.canonical.{site}.>), never the .created filter leaf this worker
// happens to consume. CreateOrUpdateStream narrows an existing stream, so
// passing a leaf here silently strips .edited/.deleted/.reacted/.pinned from a
// stream message-gatekeeper owns — last service to boot wins.
func TestBootstrapStreams_CreatesFullStreamSubjects(t *testing.T) {
	w := testWiring()
	fake := &fakeStreamManager{existing: map[string]bool{}}

	require.NoError(t, bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, true))

	require.Len(t, fake.created, 2)

	input := fake.created[0]
	assert.Equal(t, "MESSAGES-CANONICAL-test", input.Name)
	assert.Equal(t, []string{"chat.msg.canonical.test.>"}, input.Subjects,
		"must bind the whole canonical subject tree, not the .created leaf")
	assert.NotContains(t, input.Subjects, w.CanonicalCreated,
		"the consumer's filter subject must never be used as the stream binding")

	output := fake.created[1]
	assert.Equal(t, "PUSH-NOTIFICATION-test", output.Name)
	assert.Equal(t, w.PushStream.Subjects, output.Subjects)
}

// TestBootstrapStreams_SetsOnlySchema: per CLAUDE.md the helper sets ONLY Name +
// Subjects. Retention, storage, compression and federation belong to ops/IaC; a
// dev boot must not silently reconfigure a stream another owner provisions.
func TestBootstrapStreams_SetsOnlySchema(t *testing.T) {
	w := testWiring()
	fake := &fakeStreamManager{existing: map[string]bool{}}

	require.NoError(t, bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, true))

	for _, cfg := range fake.created {
		assert.Equal(t, jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}, cfg,
			"stream %s: bootstrap must set Name+Subjects and nothing else", cfg.Name)
	}
}

// TestBootstrapStreams_BotPipelineBindsBotSubjects: the same rule holds for the
// bot pipeline, which binds its own canonical/push pair.
func TestBootstrapStreams_BotPipelineBindsBotSubjects(t *testing.T) {
	w := stream.Resolve(stream.PipelineBot, "test")
	fake := &fakeStreamManager{existing: map[string]bool{}}

	require.NoError(t, bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, true))

	require.Len(t, fake.created, 2)
	assert.Equal(t, "BOT-MESSAGES-CANONICAL-test", fake.created[0].Name)
	assert.Equal(t, w.CanonicalStream.Subjects, fake.created[0].Subjects)
	assert.Equal(t, "BOT-PUSH-NOTIFICATION-test", fake.created[1].Name)
	assert.Equal(t, w.PushStream.Subjects, fake.created[1].Subjects)
}

// TestBootstrapStreams_DisabledVerifiesBothStreams: publishing is synchronous
// (jsPublisher.PublishMsg returns the PublishMsg error), so a missing push
// stream is not "surfaced per publish and tolerated" — it naks every message
// until MaxDeliver drops it, i.e. silent total notification loss. Fail fast at
// startup instead, matching message-gatekeeper.
func TestBootstrapStreams_DisabledVerifiesBothStreams(t *testing.T) {
	w := testWiring()
	fake := &fakeStreamManager{existing: map[string]bool{
		"MESSAGES-CANONICAL-test": true,
		"PUSH-NOTIFICATION-test":  true,
	}}

	require.NoError(t, bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, false))

	assert.Empty(t, fake.created, "production must never create streams")
	assert.Equal(t, []string{"MESSAGES-CANONICAL-test", "PUSH-NOTIFICATION-test"}, fake.verified)
}

func TestBootstrapStreams_DisabledFailsOnMissingOutputStream(t *testing.T) {
	w := testWiring()
	fake := &fakeStreamManager{existing: map[string]bool{"MESSAGES-CANONICAL-test": true}}

	err := bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify stream PUSH-NOTIFICATION-test")
	assert.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

func TestBootstrapStreams(t *testing.T) {
	w := testWiring()
	tests := []struct {
		name        string
		enabled     bool
		existing    map[string]bool
		failOn      string
		failErr     error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:     "disabled - verifies existing streams",
			enabled:  false,
			existing: map[string]bool{"MESSAGES-CANONICAL-test": true, "PUSH-NOTIFICATION-test": true},
		},
		{
			name:       "disabled - fails when input stream missing",
			enabled:    false,
			existing:   map[string]bool{"PUSH-NOTIFICATION-test": true},
			wantErrSub: "verify stream MESSAGES-CANONICAL-test",
		},
		{
			name:        "enabled - creates input + output streams",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"MESSAGES-CANONICAL-test", "PUSH-NOTIFICATION-test"},
		},
		{
			name:       "enabled - wraps input stream creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "MESSAGES-CANONICAL-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create stream MESSAGES-CANONICAL-test",
		},
		{
			name:       "enabled - wraps output stream creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "PUSH-NOTIFICATION-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create stream PUSH-NOTIFICATION-test",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing}
			err := bootstrapStreams(context.Background(), fake, w.CanonicalStream, w.PushStream, tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				if tc.enabled {
					assert.ErrorIs(t, err, tc.failErr)
				} else {
					assert.ErrorIs(t, err, jetstream.ErrStreamNotFound)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreated, fake.createdNames())
		})
	}
}
