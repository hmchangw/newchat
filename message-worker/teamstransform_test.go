package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// stubResolver echoes the teams user id as the account, so transform tests need no store.
type stubResolver struct{}

func (stubResolver) resolve(_ context.Context, teamsUserID, displayName string) (resolvedSender, error) {
	return resolvedSender{Account: teamsUserID, UserID: teamsUserID, DisplayName: displayName}, nil
}

func transformJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestDefaultTransformer_Shapes(t *testing.T) {
	tr := NewDefaultTransformer(stubResolver{})

	t.Run("user message", func(t *testing.T) {
		raw := transformJSON(t, teamsmigrate.Message{
			ID: "m1", RoomID: "r1", MessageType: "message",
			From: teamsmigrate.User{ID: "u1", DisplayName: "Al"},
			Body: teamsmigrate.Body{ContentType: "text", Content: "hello"},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		assert.Equal(t, "hello", msg.Content)
		assert.Equal(t, "", msg.Type) // normal user message
		assert.Equal(t, "u1", msg.UserAccount)
		assert.Equal(t, "r1", msg.RoomID)
	})

	t.Run("system message", func(t *testing.T) {
		raw := transformJSON(t, teamsmigrate.Message{
			ID: "m2", MessageType: "systemEventMessage",
			From: teamsmigrate.User{ID: "u1"}, Body: teamsmigrate.Body{ContentType: "text", Content: "x joined"},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		assert.Equal(t, "teams_system", msg.Type)
	})

	t.Run("reply(quote) shape", func(t *testing.T) {
		raw := transformJSON(t, teamsmigrate.Message{
			ID: "m3", RoomID: "r1", From: teamsmigrate.User{ID: "u1"},
			Body: teamsmigrate.Body{ContentType: "text", Content: "re"},
			ReplyToMessage: &teamsmigrate.Message{
				ID: "p1", From: teamsmigrate.User{ID: "u2", DisplayName: "Bo"}, // no roomId → scoped by the outer reply's room
				Body: teamsmigrate.Body{ContentType: "text", Content: "parent"},
			},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		require.NotNil(t, msg.QuotedParentMessage)
		assert.Equal(t, teamsmigrate.DeterministicMessageID("p1"), msg.QuotedParentMessage.MessageID)
		assert.Equal(t, "r1", msg.QuotedParentMessage.RoomID)
		assert.Equal(t, "parent", msg.QuotedParentMessage.Msg)
		assert.Equal(t, "u2", msg.QuotedParentMessage.Sender.Account)
	})

	t.Run("mentions resolved", func(t *testing.T) {
		raw := transformJSON(t, teamsmigrate.Message{
			ID: "m4", From: teamsmigrate.User{ID: "u1"}, Body: teamsmigrate.Body{Content: "hi @b"},
			Mentions: []teamsmigrate.Mention{{UserID: "u2", DisplayName: "Bo"}},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		require.Len(t, msg.Mentions, 1)
		assert.Equal(t, "u2", msg.Mentions[0].Account)
	})

	t.Run("forward is not marked (stubbed)", func(t *testing.T) {
		raw := transformJSON(t, teamsmigrate.Message{
			ID: "m5", From: teamsmigrate.User{ID: "u1"}, Forwarded: true,
			Body: teamsmigrate.Body{Content: "fwd"},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		assert.Equal(t, "fwd", msg.Content) // migrates as a plain message; no forward field exists
	})

	t.Run("malformed json errors", func(t *testing.T) {
		_, err := tr.Transform(context.Background(), json.RawMessage(`{`))
		require.Error(t, err)
	})
}

func TestReactionShortcode(t *testing.T) {
	cases := map[string]string{
		"like": ":thumbsup:", "heart": ":heart:", "laugh": ":laughing:",
		"LIKE": ":thumbsup:", "": "", "custom": ":custom:",
	}
	for in, want := range cases {
		assert.Equal(t, want, reactionShortcode(in), in)
	}
}
