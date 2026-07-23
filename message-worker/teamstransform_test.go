package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		raw := transformJSON(t, teamsMessage{
			ID: "m1", RoomID: "r1", MessageType: "message",
			From: teamsUser{ID: "u1", DisplayName: "Al"},
			Body: teamsBody{ContentType: "text", Content: "hello"},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		assert.Equal(t, "hello", msg.Content)
		assert.Equal(t, "", msg.Type) // normal user message
		assert.Equal(t, "u1", msg.UserAccount)
		assert.Equal(t, "r1", msg.RoomID)
	})

	t.Run("system message", func(t *testing.T) {
		raw := transformJSON(t, teamsMessage{
			ID: "m2", MessageType: "systemEventMessage",
			From: teamsUser{ID: "u1"}, Body: teamsBody{ContentType: "text", Content: "x joined"},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		assert.Equal(t, "teams_system", msg.Type)
	})

	t.Run("reply(quote) shape", func(t *testing.T) {
		raw := transformJSON(t, teamsMessage{
			ID: "m3", RoomID: "r1", From: teamsUser{ID: "u1"},
			Body: teamsBody{ContentType: "text", Content: "re"},
			ReplyToMessage: &teamsMessage{
				ID: "p1", From: teamsUser{ID: "u2", DisplayName: "Bo"}, // no roomId → scoped by the outer reply's room
				Body: teamsBody{ContentType: "text", Content: "parent"},
			},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		require.NotNil(t, msg.QuotedParentMessage)
		assert.Equal(t, deterministicMessageID("r1", "p1"), msg.QuotedParentMessage.MessageID)
		assert.Equal(t, "r1", msg.QuotedParentMessage.RoomID)
		assert.Equal(t, "parent", msg.QuotedParentMessage.Msg)
		assert.Equal(t, "u2", msg.QuotedParentMessage.Sender.Account)
	})

	t.Run("mentions resolved", func(t *testing.T) {
		raw := transformJSON(t, teamsMessage{
			ID: "m4", From: teamsUser{ID: "u1"}, Body: teamsBody{Content: "hi @b"},
			Mentions: []teamsMention{{UserID: "u2", DisplayName: "Bo"}},
		})
		msg, err := tr.Transform(context.Background(), raw)
		require.NoError(t, err)
		require.Len(t, msg.Mentions, 1)
		assert.Equal(t, "u2", msg.Mentions[0].Account)
	})

	t.Run("forward is not marked (stubbed)", func(t *testing.T) {
		raw := transformJSON(t, teamsMessage{
			ID: "m5", From: teamsUser{ID: "u1"}, Forwarded: true,
			Body: teamsBody{Content: "fwd"},
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

func TestHTMLToMarkdown(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"bold":               {"<b>hi</b>", "**hi**"},
		"strong":             {"<strong>hi</strong>", "**hi**"},
		"italic":             {"<i>hi</i>", "*hi*"},
		"link":               {`<a href="http://x">t</a>`, "[t](http://x)"},
		"break":              {"a<br>b", "a\nb"},
		"unsupported to raw": {"<div><span>plain</span></div>", "plain"},
		"entities":           {"a &amp; b &lt;c&gt;", "a & b <c>"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, htmlToMarkdown(tc.in))
		})
	}
}

func TestBodyToContent_TextPassthrough(t *testing.T) {
	assert.Equal(t, "*raw* text", bodyToContent(teamsBody{ContentType: "text", Content: "*raw* text"}))
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

func TestDeterministicMessageID_Stable(t *testing.T) {
	a := deterministicMessageID("chatA", "tm-1")
	assert.Equal(t, a, deterministicMessageID("chatA", "tm-1"), "same (chat, id) → same message id")
	assert.NotEqual(t, a, deterministicMessageID("chatB", "tm-1"), "same teams id in a different chat → different id (no cross-chat collision)")
	assert.NotEqual(t, a, deterministicMessageID("chatA", "tm-2"))
	assert.True(t, isValidBase62MessageID(a), "valid message-id format: %q", a)
}

// isValidBase62MessageID mirrors idgen's 20-char base62 message-id shape.
func isValidBase62MessageID(s string) bool {
	if len(s) != 20 {
		return false
	}
	for _, c := range s {
		isBase62 := (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
		if !isBase62 {
			return false
		}
	}
	return true
}
