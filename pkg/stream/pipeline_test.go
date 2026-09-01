package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestPipeline_UnmarshalText(t *testing.T) {
	cases := []struct {
		in      string
		want    stream.Pipeline
		wantErr bool
	}{
		{"user", stream.PipelineUser, false},
		{"bot", stream.PipelineBot, false},
		{"", "", true},
		{"admin", "", true},
		{"USER", "", true},
		{"User", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			var got stream.Pipeline
			err := got.UnmarshalText([]byte(tc.in))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPipeline_ConsumerName(t *testing.T) {
	assert.Equal(t, "broadcast-worker", stream.PipelineUser.ConsumerName("broadcast-worker"))
	assert.Equal(t, "bot-broadcast-worker", stream.PipelineBot.ConsumerName("broadcast-worker"))
}

func TestResolve_User(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")
	assert.Equal(t, "MESSAGES-CANONICAL-site-a", w.CanonicalStream.Name)
	assert.Equal(t, "chat.msg.canonical.site-a.created", w.CanonicalCreated)
	assert.Equal(t, "chat.msg.canonical.site-a.>", w.CanonicalWildcard)
	assert.Equal(t, "PUSH-NOTIFICATION-site-a", w.PushStream.Name)
	assert.Equal(t, "chat.server.notification.push.site-a.send", w.PushSendSubject)
	assert.Equal(t, "chat.server.notification.push.site-a.>", w.PushInputWildcard)
}

func TestResolve_Bot(t *testing.T) {
	w := stream.Resolve(stream.PipelineBot, "site-a")
	assert.Equal(t, "BOT-MESSAGES-CANONICAL-site-a", w.CanonicalStream.Name)
	assert.Equal(t, "chat.bot.canonical.site-a.created", w.CanonicalCreated)
	assert.Equal(t, "chat.bot.canonical.site-a.>", w.CanonicalWildcard)
	assert.Equal(t, "BOT-PUSH-NOTIFICATION-site-a", w.PushStream.Name)
	assert.Equal(t, "chat.bot.notification.push.site-a.send", w.PushSendSubject)
	assert.Equal(t, "chat.bot.notification.push.site-a.>", w.PushInputWildcard)
}

func TestResolve_UserPipelineHasFailover(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	require.True(t, w.HasFailover())
	assert.Equal(t, "MESSAGES-CANONICAL-FAILOVER-site-a", w.CanonicalFailoverStream.Name)
	assert.Equal(t, "chat.failover.msg.canonical.site-a.created", w.CanonicalFailoverCreated)
	assert.Equal(t, "chat.failover.msg.canonical.site-a.>", w.CanonicalFailoverWildcard)
	assert.Equal(t, "PUSH-NOTIFICATION-FAILOVER-site-a", w.PushFailoverStream.Name)
	assert.Equal(t, "chat.failover.push.site-a.send", w.PushFailoverSendSubject)
	assert.Equal(t, "chat.failover.push.site-a.>", w.PushFailoverInputWildcard)
}

// Bots are not displaced users; the bot pipeline has no failover lane, and
// HasFailover is how a service skips it without a mode check at each call site.
func TestResolve_BotPipelineHasNoFailover(t *testing.T) {
	w := stream.Resolve(stream.PipelineBot, "site-a")

	assert.False(t, w.HasFailover())
	assert.Empty(t, w.CanonicalFailoverStream.Name)
	assert.Empty(t, w.PushFailoverStream.Name)
}

// Three services hand-rolled this suffix; the shared derivation is what keeps
// them from drifting to different names on the same buddy cluster.
func TestPipeline_FailoverConsumerName(t *testing.T) {
	tests := []struct {
		name string
		p    stream.Pipeline
		base string
		want string
	}{
		{"user pipeline keeps the base", stream.PipelineUser, "broadcast-worker", "broadcast-worker-failover"},
		{"bot pipeline keeps its prefix", stream.PipelineBot, "broadcast-worker", "bot-broadcast-worker-failover"},
		{"user notification worker", stream.PipelineUser, "notification-worker", "notification-worker-failover"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.p.FailoverConsumerName(tt.base))
		})
	}
}

// The failover durable must never collide with the home one, on either
// pipeline — a shared durable would have the two lanes clobber each other's
// cursor on a single-server dev NATS.
func TestPipeline_FailoverConsumerNameDiffersFromHome(t *testing.T) {
	for _, p := range []stream.Pipeline{stream.PipelineUser, stream.PipelineBot} {
		assert.NotEqual(t, p.ConsumerName("svc"), p.FailoverConsumerName("svc"))
	}
}
