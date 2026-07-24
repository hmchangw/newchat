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
	assert.Equal(t, "MESSAGES_CANONICAL_site-a", w.CanonicalStream.Name)
	assert.Equal(t, "chat.msg.canonical.site-a.created", w.CanonicalCreated)
	assert.Equal(t, "chat.msg.canonical.site-a.>", w.CanonicalWildcard)
	assert.Equal(t, "PUSH_NOTIFICATION_site-a", w.PushStream.Name)
	assert.Equal(t, "chat.server.notification.push.site-a.send", w.PushSendSubject)
	assert.Equal(t, "chat.server.notification.push.site-a.>", w.PushInputWildcard)
}

func TestResolve_Bot(t *testing.T) {
	w := stream.Resolve(stream.PipelineBot, "site-a")
	assert.Equal(t, "BOT_MESSAGES_CANONICAL_site-a", w.CanonicalStream.Name)
	assert.Equal(t, "chat.bot.canonical.site-a.created", w.CanonicalCreated)
	assert.Equal(t, "chat.bot.canonical.site-a.>", w.CanonicalWildcard)
	assert.Equal(t, "BOT_PUSH_NOTIFICATION_site-a", w.PushStream.Name)
	assert.Equal(t, "chat.bot.notification.push.site-a.send", w.PushSendSubject)
	assert.Equal(t, "chat.bot.notification.push.site-a.>", w.PushInputWildcard)
}
