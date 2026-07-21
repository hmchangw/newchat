# cassandra.Message Reactions JSON Decode Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `cassandra.Reactions` a symmetric `UnmarshalJSON` so any consumer can JSON-decode a `cassandra.Message` that carries reactions, fixing the thread-list decode failure without changing the client wire shape.

**Architecture:** `cassandra.Reactions` (a struct-keyed map) has a custom `MarshalJSON` emitting the client shape `map<emoji,[{account,displayName}]>` but no `UnmarshalJSON`, so decoding a Message with reactions fails. Add an inverse `UnmarshalJSON` plus a transient `DisplayName` carrier on `ReactorInfo`; `MarshalJSON` prefers the carrier when set (so a decoded value re-marshals to the identical client bytes), else composes as today. The decode is lossy only in fields never transmitted to clients (`userId`, `chnName`, real `reactedAt`); per-emoji FIFO order is preserved with a synthetic index-based `reactedAt`.

**Tech Stack:** Go 1.25, `encoding/json`, `github.com/hmchangw/chat/pkg/displayfmt`, testify (`assert`/`require`).

## Global Constraints

- Always use `make` targets — never raw `go` commands. Unit tests: `make test SERVICE=<pkg-or-service>` (runs `go test -race ./<path>/...`).
- TDD is mandatory: write the failing test first, confirm it fails, implement the minimum, confirm it passes, commit.
- Minimum 80% coverage for the package; cover error and null/empty branches.
- `make lint` (golangci-lint) and `make sast` (gosec/govulncheck/semgrep, fail on medium+) are blocking gates — run before finishing.
- No new third-party dependencies.
- Do NOT change the client wire shape or `docs/client-api.md` — `MarshalJSON` output must stay byte-identical for messages read from Cassandra (where `DisplayName` is empty).
- Commit trailers on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01E9yYH8NNDWLX81MBY4bZmd
  ```
  Ensure `git config user.email noreply@anthropic.com` and `git config user.name Claude` first (verified commits).
- Branch: `claude/thread-list-decode-error-56lvw2` (already checked out).

## File Structure

- `pkg/model/cassandra/reactions.go` — the `Reactions`/`ReactorInfo` types and their JSON codec. Add the transient field, the `MarshalJSON` tweak, and the new `UnmarshalJSON`. (Task 1)
- `pkg/model/cassandra/reactions_test.go` — type-level codec tests. Add `TestReactions_UnmarshalJSON` and `TestMessage_DecodeWithReactions`. (Task 1)
- `CLAUDE.md` — update the codec note that currently says the `Reactions` decoder is unusable. (Task 1)
- `pkg/model/threadlist_test.go` — strengthen the existing `ThreadListItem` round-trip test to carry reactions (closes the blind spot at the embedding layer). (Task 2)
- Verification only (no edits): `message-gatekeeper`, `broadcast-worker` sonic wire-compat tests. (Task 3)

---

### Task 1: Symmetric JSON codec for `cassandra.Reactions`

**Files:**
- Modify: `pkg/model/cassandra/reactions.go`
- Test: `pkg/model/cassandra/reactions_test.go`
- Modify (docs): `CLAUDE.md:233`

**Interfaces:**
- Consumes: existing `ReactionKey{Emoji, UserAccount string}`, `ReactorInfo{UserID, EngName, ChnName, Account string; ReactedAt time.Time}`, `Reactions map[ReactionKey]ReactorInfo`, `reactionUser{Account, DisplayName string}` (`json:"account"`/`json:"displayName"`), and `displayfmt.CombineWithFallback(first, second, fallback string) string`.
- Produces: `ReactorInfo.DisplayName string` (transient, `json:"-" cql:"-"`) and `func (r *Reactions) UnmarshalJSON(data []byte) error`. After this task a `cassandra.Message` with a non-empty `reactions` object decodes under `encoding/json` (and sonic), and a decode→encode round trip is byte-identical.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/model/cassandra/reactions_test.go` (the file already imports `encoding/json`, `testing`, `time`, `assert`, `require`):

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/model/cassandra`
Expected: FAIL. The object-decoding subtests and `TestMessage_DecodeWithReactions` error with `json: cannot unmarshal object into Go value of type cassandra.Reactions` (and `...Go struct field Message.reactions of type cassandra.Reactions`). `null_yields_nil_map` and `invalid_json` may already pass; the suite as a whole fails.

- [ ] **Step 3: Add the transient carrier field to `ReactorInfo`**

In `pkg/model/cassandra/reactions.go`, replace the `ReactorInfo` struct:

```go
// ReactorInfo is the map-value UDT for Message.Reactions.
type ReactorInfo struct {
	UserID    string    `json:"userId"    cql:"user_id"`
	EngName   string    `json:"engName"   cql:"eng_name"`
	ChnName   string    `json:"chnName"   cql:"chn_name"`
	Account   string    `json:"account"   cql:"account"`
	ReactedAt time.Time `json:"reactedAt" cql:"reacted_at"`
	// DisplayName is a transient carrier populated only when a Reactions value was
	// decoded from the client wire form (see UnmarshalJSON). MarshalJSON prefers it
	// over composing from EngName/ChnName. json:"-" keeps it off the wire directly;
	// cql:"-" keeps gocql from binding/scanning it into the Cassandra UDT.
	DisplayName string `json:"-" cql:"-"`
}
```

- [ ] **Step 4: Make `MarshalJSON` prefer the carried display name**

In `pkg/model/cassandra/reactions.go`, inside `MarshalJSON`, replace the loop body that stages reactors:

```go
	for k, v := range r {
		staged[k.Emoji] = append(staged[k.Emoji], reactorWithTime{
			user: reactionUser{
				Account:     k.UserAccount,
				DisplayName: displayfmt.CombineWithFallback(v.EngName, v.ChnName, k.UserAccount),
			},
			reactedAt: v.ReactedAt,
		})
	}
```

with:

```go
	for k, v := range r {
		dn := v.DisplayName
		if dn == "" {
			dn = displayfmt.CombineWithFallback(v.EngName, v.ChnName, k.UserAccount)
		}
		staged[k.Emoji] = append(staged[k.Emoji], reactorWithTime{
			user: reactionUser{
				Account:     k.UserAccount,
				DisplayName: dn,
			},
			reactedAt: v.ReactedAt,
		})
	}
```

- [ ] **Step 5: Add `UnmarshalJSON` and the `fmt` import**

In `pkg/model/cassandra/reactions.go`, add `"fmt"` to the import block:

```go
import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/displayfmt"
)
```

Append this method to the end of the file:

```go
// UnmarshalJSON is the inverse of MarshalJSON: it reads the wire shape
// map<emoji, [{account, displayName}]> back into the struct-keyed map. Without it
// the default decoder rejects the JSON object because the map key is a struct
// (ReactionKey), not a string/int/TextUnmarshaler — which is what broke decoding
// any cassandra.Message that carries reactions.
//
// The wire form is lossy — userId, chnName and reactedAt are not transmitted — so
// a decoded ReactorInfo carries only Account plus the already-composed display
// name (in the transient DisplayName field, which MarshalJSON re-emits verbatim).
// A synthetic per-emoji ReactedAt preserves each emoji's on-wire FIFO reactor order
// across marshal→unmarshal→marshal. "null" → nil map; "{}" → non-nil empty map.
func (r *Reactions) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*r = nil
		return nil
	}
	var grouped map[string][]reactionUser
	if err := json.Unmarshal(data, &grouped); err != nil {
		return fmt.Errorf("decode reactions: %w", err)
	}
	out := make(Reactions, len(grouped))
	for emoji, users := range grouped {
		for i, u := range users {
			out[ReactionKey{Emoji: emoji, UserAccount: u.Account}] = ReactorInfo{
				Account:     u.Account,
				DisplayName: u.DisplayName,
				// Index-based so MarshalJSON's (reactedAt, account) sort replays the
				// original array order; distinct per position within each emoji bucket.
				ReactedAt: time.UnixMilli(int64(i)),
			}
		}
	}
	*r = out
	return nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/model/cassandra`
Expected: PASS (`ok  github.com/hmchangw/chat/pkg/model/cassandra`). All existing tests (including `TestReactorInfo_JSONRoundTrip`, which does not set `DisplayName`, and `TestReactions_MarshalJSON`) still pass.

- [ ] **Step 7: Update the CLAUDE.md codec note**

In `CLAUDE.md` (line 233, the "All NATS payloads are JSON…" bullet), replace the final sentence:

```
One exception: `message-gatekeeper/fetcher_history.go` decodes a narrow projection rather than the full `cassandra.Message`, because that type embeds the marshal-only struct-keyed `Reactions` map whose decoder sonic rejects.
```

with:

```
`cassandra.Reactions` is a struct-keyed map with a client-facing `MarshalJSON` (`map<emoji,[{account,displayName}]>`) and a symmetric `UnmarshalJSON` that reverses it, so a full `cassandra.Message` with reactions now decodes under both `encoding/json` and sonic; the decode is lossy only in fields never sent to clients (`userId`/`chnName`/real `reactedAt`) and preserves per-emoji FIFO order. Cross-service consumers that only need a snapshot still decode a narrow projection (e.g. `message-gatekeeper/fetcher_history.go`, `room-service/reader_history.go`, `notification-worker`/`broadcast-worker` parent fetchers) to minimize decoded fields and assert id/room authority from the request rather than the reply.
```

- [ ] **Step 8: Commit**

```bash
git config user.email noreply@anthropic.com && git config user.name Claude
git add pkg/model/cassandra/reactions.go pkg/model/cassandra/reactions_test.go CLAUDE.md
git commit -m "$(cat <<'EOF'
Add symmetric UnmarshalJSON to cassandra.Reactions

cassandra.Reactions is a struct-keyed map with a custom MarshalJSON but no
UnmarshalJSON, so decoding a cassandra.Message with reactions failed with
"cannot unmarshal object into ... cassandra.Reactions". Add the inverse
UnmarshalJSON plus a transient DisplayName carrier on ReactorInfo so decoded
values re-marshal to the identical client wire shape; MarshalJSON output is
byte-identical for Cassandra-read messages (DisplayName empty). Per-emoji FIFO
order is preserved via a synthetic index-based reactedAt.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01E9yYH8NNDWLX81MBY4bZmd
EOF
)"
```

---

### Task 2: Guard the embedding path (`ThreadListItem`)

**Files:**
- Test: `pkg/model/threadlist_test.go`

**Interfaces:**
- Consumes: `model.ThreadListItem` (fields `ParentMessage`/`LastMessage *cassandra.Message`), `cassandra.Message`, `cassandra.Reactions`, `cassandra.ReactionKey`, `cassandra.ReactorInfo` (all already imported by the test file as `model` and `cassandra`).
- Produces: nothing consumed by later tasks; this is the regression guard for the reported production path (`user-service/historyclient` decodes `ThreadSubscriptionListResponse` → `ThreadListItem` → `*cassandra.Message`). The existing test previously used a reaction-free message, which is why the bug shipped.

- [ ] **Step 1: Strengthen the round-trip test to carry reactions**

In `pkg/model/threadlist_test.go`, replace `TestThreadListItemJSON_WithMessages` in full:

```go
// The hydrated parent/last message bodies survive a JSON round trip — including
// reactions, whose struct-keyed map once failed to decode (see cassandra.Reactions).
func TestThreadListItemJSON_WithMessages(t *testing.T) {
	parent := &cassandra.Message{
		MessageID: "msg-parent", RoomID: "room-1", Msg: "anyone?",
		Reactions: cassandra.Reactions{
			cassandra.ReactionKey{Emoji: "👍", UserAccount: "alice"}: cassandra.ReactorInfo{EngName: "Alice", Account: "alice"},
		},
	}
	last := &cassandra.Message{MessageID: "msg-last", RoomID: "room-1", Msg: "on it"}
	src := model.ThreadListItem{
		SiteID: "site-a", RoomID: "room-1", ThreadRoomID: "thr-1",
		ParentMessageID: "msg-parent", LastMsgAt: 1746518400000,
		ParentMessage: parent, LastMessage: last,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.ThreadListItem
	require.NoError(t, json.Unmarshal(data, &dst))
	require.NotNil(t, dst.ParentMessage)
	require.NotNil(t, dst.LastMessage)
	assert.Equal(t, "msg-parent", dst.ParentMessage.MessageID)
	assert.Equal(t, "on it", dst.LastMessage.Msg)
	assert.Contains(t, dst.ParentMessage.Reactions, cassandra.ReactionKey{Emoji: "👍", UserAccount: "alice"})
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS for both `github.com/hmchangw/chat/pkg/model` and `.../pkg/model/cassandra`. (This test would fail without Task 1 — it now locks in decode at the embedding layer.)

- [ ] **Step 3: Commit**

```bash
git add pkg/model/threadlist_test.go
git commit -m "$(cat <<'EOF'
Cover reactions in ThreadListItem JSON round trip

The existing round-trip test used a reaction-free message, the blind spot that
let the thread-list decode failure ship. Add a reaction to the parent message
and assert it survives, guarding the user-service historyclient decode path.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01E9yYH8NNDWLX81MBY4bZmd
EOF
)"
```

---

### Task 3: Verify gates and cross-service wire compatibility

**Files:** none modified (verification only). If `make lint`/`make fmt` reports formatting changes, apply and fold them into the relevant task's commit or a small follow-up `style:` commit.

**Interfaces:** none.

- [ ] **Step 1: Confirm the client wire shape is unchanged (sonic wire-compat)**

Run: `make test SERVICE=message-gatekeeper`
Then: `make test SERVICE=broadcast-worker`
Expected: PASS for both. These packages carry sonic wire-compat tests asserting `MarshalJSON` byte output; they must stay green because `DisplayName` is empty on the Cassandra-read path (byte-identical output).

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: `0 issues.` Fix any reported issue (e.g. `gocritic hugeParam` — do not add heavy-struct value-receiver helpers) and re-run until clean.

- [ ] **Step 3: SAST**

Run: `make sast`
Expected: gosec/govulncheck/semgrep all pass (fail on medium+). If `semgrep` is not installed locally, note it and rely on CI; run at least `make sast-gosec` and `make sast-vuln` locally and confirm clean.

- [ ] **Step 4: Full model package check with race detector**

Run: `make test SERVICE=pkg/model`
Expected: PASS with `-race`.

- [ ] **Step 5: Push the branch**

```bash
git push -u origin claude/thread-list-decode-error-56lvw2
```
Expected: branch updated on origin. Retry with exponential backoff (2s, 4s, 8s, 16s) only on network errors.

---

## Self-Review

**1. Spec coverage:**
- Spec §3.1 (transient field + MarshalJSON prefer + UnmarshalJSON with null/empty/order/error) → Task 1 Steps 3–5, tested in Steps 1–2/6. ✓
- Spec §3.2 (type + model tests: null/empty/single/multi/wire-stability/order/invalid, Message regression, ThreadListItem reactions) → Task 1 Step 1, Task 2 Step 1. ✓
- Spec §3.3 (CLAUDE.md updated; client-api.md unchanged) → Task 1 Step 7; no client-api task by design. ✓
- Spec §5 risks (byte-identical MarshalJSON, cql:"-" UDT safety, sonic decodable) → Task 3 Step 1 guards wire compat; `cql:"-"`/`json:"-"` in Task 1 Step 3; `TestReactorInfo_JSONRoundTrip` (unchanged) guards the UDT struct. ✓
- Spec §6 verification (make test/lint/sast, message-gatekeeper + broadcast-worker) → Task 3. ✓
- Spec §4 non-goals (no client-shape change, projections untouched, no historyclient NATS test, no presenter split) → respected; no tasks touch those. ✓

**2. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Every code step shows full code; every run step shows the exact command and expected result. ✓

**3. Type consistency:** `ReactorInfo.DisplayName` (`string`), `reactionUser{Account, DisplayName}`, `ReactionKey{Emoji, UserAccount}`, `Reactions`, `UnmarshalJSON(*Reactions)`, and `displayfmt.CombineWithFallback` are used identically across Task 1 and Task 2. The `MarshalJSON` edit matches the current source exactly (verified against `reactions.go`). ✓
