package cassandra

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReactionKey_JSONRoundTrip(t *testing.T) {
	k := ReactionKey{Emoji: "👍", UserAccount: "alice"}
	roundTrip(t, k)
}

func TestReactorInfo_JSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	ri := ReactorInfo{
		UserID:    "u1",
		EngName:   "Alice",
		ChnName:   "爱丽丝",
		Account:   "alice",
		ReactedAt: now,
	}
	roundTrip(t, ri)
}

func TestReactions_MarshalJSON(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	t.Run("nil", func(t *testing.T) {
		// Direct marshal exercises the nil branch that omitempty otherwise skips.
		data, err := json.Marshal(Reactions(nil))
		require.NoError(t, err)
		assert.Equal(t, "null", string(data))
	})

	t.Run("nil_omitted_via_omitempty", func(t *testing.T) {
		msg := Message{
			RoomID:    "r1",
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			MessageID: "m1",
			Sender:    Participant{ID: "u1", Account: "alice"},
			Msg:       "hi",
		}
		data, err := json.Marshal(msg)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["reactions"]
		assert.False(t, present, "nil Reactions should be omitted via omitempty")
	})

	t.Run("empty", func(t *testing.T) {
		data, err := json.Marshal(Reactions{})
		require.NoError(t, err)
		assert.Equal(t, "{}", string(data))
	})

	t.Run("single", func(t *testing.T) {
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{
				UserID: "u1", EngName: "Alice", ChnName: "爱丽丝", Account: "alice", ReactedAt: now,
			},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		require.Contains(t, grouped, "👍")
		require.Len(t, grouped["👍"], 1)
		assert.Equal(t, "alice", grouped["👍"][0]["account"])
		assert.Equal(t, "Alice 爱丽丝", grouped["👍"][0]["displayName"])
	})

	t.Run("grouped_by_emoji_fifo_by_reactedAt", func(t *testing.T) {
		// Per-emoji order is FIFO — oldest reaction first, newest last.
		// Matches the legacy MongoDB array-push insertion order.
		t0 := now
		t1 := now.Add(1 * time.Minute)
		t2 := now.Add(2 * time.Minute)
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "carol"}: ReactorInfo{ // newest 👍
				UserID: "u3", EngName: "Carol", ChnName: "卡罗尔", Account: "carol", ReactedAt: t2,
			},
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{ // oldest 👍
				UserID: "u1", EngName: "Alice", ChnName: "爱丽丝", Account: "alice", ReactedAt: t0,
			},
			ReactionKey{Emoji: "👍", UserAccount: "dave"}: ReactorInfo{ // middle 👍
				UserID: "u4", EngName: "Dave", Account: "dave", ReactedAt: t1,
			},
			ReactionKey{Emoji: "❤️", UserAccount: "bob"}: ReactorInfo{
				UserID: "u2", EngName: "Bob", ChnName: "鲍勃", Account: "bob", ReactedAt: t1,
			},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		require.Contains(t, grouped, "❤️")
		require.Len(t, grouped["👍"], 3)
		// Oldest first, newest last.
		assert.Equal(t, "alice", grouped["👍"][0]["account"])
		assert.Equal(t, "dave", grouped["👍"][1]["account"])
		assert.Equal(t, "carol", grouped["👍"][2]["account"])
		require.Len(t, grouped["❤️"], 1)
		assert.Equal(t, "bob", grouped["❤️"][0]["account"])
		assert.Equal(t, "Bob 鲍勃", grouped["❤️"][0]["displayName"])
	})

	t.Run("same_timestamp_breaks_by_account_asc", func(t *testing.T) {
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "carol"}: ReactorInfo{Account: "carol", ReactedAt: now},
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{Account: "alice", ReactedAt: now},
			ReactionKey{Emoji: "👍", UserAccount: "bob"}:   ReactorInfo{Account: "bob", ReactedAt: now},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		require.Len(t, grouped["👍"], 3)
		assert.Equal(t, "alice", grouped["👍"][0]["account"])
		assert.Equal(t, "bob", grouped["👍"][1]["account"])
		assert.Equal(t, "carol", grouped["👍"][2]["account"])
	})

	t.Run("displayName_fallback_to_account", func(t *testing.T) {
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "anon"}: ReactorInfo{Account: "anon", ReactedAt: now},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		assert.Equal(t, "anon", grouped["👍"][0]["displayName"])
	})

	t.Run("one_user_multiple_different_emoji", func(t *testing.T) {
		// Spec §1: a user may react with multiple different emoji on the same
		// message. The same account must appear under each emoji bucket they
		// reacted with — different ReactionKey, distinct map entries.
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "alice"}:  ReactorInfo{EngName: "Alice", Account: "alice", ReactedAt: now},
			ReactionKey{Emoji: "❤️", UserAccount: "alice"}: ReactorInfo{EngName: "Alice", Account: "alice", ReactedAt: now},
			ReactionKey{Emoji: "🎉", UserAccount: "alice"}:  ReactorInfo{EngName: "Alice", Account: "alice", ReactedAt: now},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		assert.Len(t, grouped, 3, "expected three emoji buckets, one per distinct reaction")
		for _, emoji := range []string{"👍", "❤️", "🎉"} {
			require.Len(t, grouped[emoji], 1, "alice should appear exactly once under %q", emoji)
			assert.Equal(t, "alice", grouped[emoji][0]["account"])
		}
	})

	t.Run("no_duplicate_account_within_emoji_bucket", func(t *testing.T) {
		// Spec §1 self-uniqueness: the (emoji, userAccount) map key guarantees
		// one user cannot appear twice under the same emoji. Verified at the
		// type level (Go disallows duplicate map keys) and re-asserted here so
		// any future regression that bypasses the map shape is caught.
		r := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{Account: "alice", ReactedAt: now},
			ReactionKey{Emoji: "👍", UserAccount: "bob"}:   ReactorInfo{Account: "bob", ReactedAt: now},
			ReactionKey{Emoji: "👍", UserAccount: "carol"}: ReactorInfo{Account: "carol", ReactedAt: now},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var grouped map[string][]map[string]string
		require.NoError(t, json.Unmarshal(data, &grouped))
		require.Len(t, grouped["👍"], 3)
		seen := make(map[string]bool, 3)
		for _, u := range grouped["👍"] {
			require.False(t, seen[u["account"]], "duplicate account %q in 👍 bucket", u["account"])
			seen[u["account"]] = true
		}
	})
}

func TestReactions_UnmarshalJSON(t *testing.T) {
	t.Run("null_yields_nil_map", func(t *testing.T) {
		var r Reactions
		require.NoError(t, json.Unmarshal([]byte("null"), &r))
		assert.Nil(t, r)
	})

	t.Run("empty_object_yields_non_nil_empty_map", func(t *testing.T) {
		// Non-nil so it re-marshals back to "{}" rather than "null".
		var r Reactions
		require.NoError(t, json.Unmarshal([]byte("{}"), &r))
		require.NotNil(t, r)
		assert.Len(t, r, 0)
	})

	t.Run("single_reactor_round_trips_to_client_shape", func(t *testing.T) {
		const wire = `{"👍":[{"account":"alice","displayName":"Alice 爱丽丝"}]}`
		var r Reactions
		require.NoError(t, json.Unmarshal([]byte(wire), &r))
		got, ok := r[ReactionKey{Emoji: "👍", UserAccount: "alice"}]
		require.True(t, ok, "expected (👍, alice) key")
		assert.Equal(t, "alice", got.Account)
		// Behaviour asserted via the wire, not the internal carrier field.
		out, err := json.Marshal(r)
		require.NoError(t, err)
		assert.JSONEq(t, wire, string(out))
	})

	t.Run("multiple_emoji_multiple_reactors", func(t *testing.T) {
		wire := `{"👍":[{"account":"alice","displayName":"Alice"},{"account":"bob","displayName":"Bob"}],"❤️":[{"account":"carol","displayName":"Carol"}]}`
		var r Reactions
		require.NoError(t, json.Unmarshal([]byte(wire), &r))
		assert.Len(t, r, 3)
		assert.Contains(t, r, ReactionKey{Emoji: "👍", UserAccount: "alice"})
		assert.Contains(t, r, ReactionKey{Emoji: "👍", UserAccount: "bob"})
		assert.Contains(t, r, ReactionKey{Emoji: "❤️", UserAccount: "carol"})
	})

	t.Run("wire_stable_across_marshal_unmarshal_marshal", func(t *testing.T) {
		// A Cassandra-read value (EngName/ChnName + real ReactedAt) re-serializes
		// byte-for-byte after a decode, preserving per-emoji FIFO reactor order.
		now := time.Now().UTC().Truncate(time.Millisecond)
		src := Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "carol"}: ReactorInfo{EngName: "Carol", Account: "carol", ReactedAt: now.Add(2 * time.Minute)},
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{EngName: "Alice", Account: "alice", ReactedAt: now},
			ReactionKey{Emoji: "👍", UserAccount: "dave"}:  ReactorInfo{EngName: "Dave", Account: "dave", ReactedAt: now.Add(1 * time.Minute)},
			ReactionKey{Emoji: "❤️", UserAccount: "bob"}:  ReactorInfo{EngName: "Bob", ChnName: "鲍勃", Account: "bob", ReactedAt: now},
		}
		first, err := json.Marshal(src)
		require.NoError(t, err)
		var decoded Reactions
		require.NoError(t, json.Unmarshal(first, &decoded))
		second, err := json.Marshal(decoded)
		require.NoError(t, err)
		assert.Equal(t, string(first), string(second))
	})

	t.Run("invalid_json", func(t *testing.T) {
		var r Reactions
		assert.Error(t, json.Unmarshal([]byte(`{"👍":"not-an-array"}`), &r))
	})
}

// TestMessage_DecodeWithReactions reproduces the reported thread-list decode
// failure: a cassandra.Message carrying reactions must round-trip through JSON.
func TestMessage_DecodeWithReactions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	src := Message{
		RoomID:    "r1",
		CreatedAt: now,
		MessageID: "m1",
		Sender:    Participant{ID: "u1", Account: "alice"},
		Msg:       "hi",
		Reactions: Reactions{
			ReactionKey{Emoji: "👍", UserAccount: "alice"}: ReactorInfo{EngName: "Alice", Account: "alice", ReactedAt: now},
		},
	}
	data, err := json.Marshal(src)
	require.NoError(t, err)

	var dst Message
	require.NoError(t, json.Unmarshal(data, &dst), "Message with reactions must decode")
	require.NotNil(t, dst.Reactions)
	assert.Contains(t, dst.Reactions, ReactionKey{Emoji: "👍", UserAccount: "alice"})
}
