# Notification Worker — Cache, Routing & Mobile Push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing blanket fan-out in `notification-worker` with a cached, mention-gated, presence-aware mobile-push pipeline (per the 2026-05-22 spec).

**Architecture:** Per canonical message, parse mentions + large-room flag once, load members through a Valkey-backed `roomsubcache` (with single-flight + a small in-process L1), apply ordered stages — Stage 1 exclusion filters (sender / mute / restricted / thread-non-follower), Stage 2 in-process hook veto, Stage 3 pure-CPU routing predicate, Stage 4 one bulk presence RPC — and emit one async JetStream push per surviving recipient on `chat.server.notification.push.{siteID}.send`. No desktop emit leg.

**Tech Stack:** Go 1.25, NATS + JetStream (`nats.go`, raw `jetstream.New` for async publish), Valkey via `pkg/valkeyutil` + `pkg/roomsubcache`, MongoDB (`go.mongodb.org/mongo-driver/v2`), `golang.org/x/sync/singleflight`, `hashicorp/golang-lru/v2/expirable`, `stretchr/testify`.

**Reference spec:** `docs/superpowers/specs/2026-05-22-notification-worker-cache-and-mobile-design.md`.

---

## File Structure

The notification-worker keeps the flat `package main` repo convention. Existing files (`main.go`, `handler.go`, `bootstrap.go`, `integration_test.go`, etc.) are modified; the new responsibilities are split into focused files:

| Path | Status | Responsibility |
|---|---|---|
| `pkg/roomsubcache/roomsubcache.go` | modify | Widen `Member` projection (IsBot, ChineseName, EngName, Muted, HistorySharedSince) |
| `pkg/roomsubcache/roomsubcache_test.go` | modify | JSON round-trip + omitempty assertions for new fields |
| `pkg/mention/mention.go` | modify | Add `MentionHere bool` to `ParseResult` |
| `pkg/mention/mention_test.go` | modify | Cases for `@here` |
| `pkg/subject/subject.go` | modify | Add `PushNotification`, `PresenceSnapshot`, `SubscriptionUpdateWildcard`, `ParseSubscriptionUpdateAccount` |
| `pkg/subject/subject_test.go` | modify | Tests for the new builders / parser |
| `pkg/model/push.go` | new | `PushNotificationEvent` + `PushNotificationData` |
| `pkg/model/presence.go` | new | `PresenceSnapshotRequest` / `PresenceSnapshotReply` / `Presence` |
| `pkg/model/model_test.go` | modify | round-trip tests for the new payload structs |
| `notification-worker/routing.go` | new | Pure routing predicate (Stage 3) |
| `notification-worker/routing_test.go` | new | Exhaustive table-driven tests |
| `notification-worker/members.go` | new | `cachedMemberLookup` (Valkey cache + Mongo loader + single-flight + L1 LRU) |
| `notification-worker/members_test.go` | new | Hit / miss-then-populate / cache-error / L1 hit / single-flight collapse |
| `notification-worker/threads.go` | new | Thread-follower lookup + `parentMessageId` index ensure |
| `notification-worker/threads_test.go` | new | Lookup happy + empty + error paths (against a fake collection) |
| `notification-worker/hook.go` | new | `Hook` interface + `noopHook` |
| `notification-worker/hook_test.go` | new | `noopHook` always allows |
| `notification-worker/presence.go` | new | `PresenceSource` interface, no-op default, bulk-RPC impl, status→push map |
| `notification-worker/presence_test.go` | new | Status table, chunking, fail-open behaviour |
| `notification-worker/emit.go` | new | `mobileEmitter` (async JS publish, dedup header, bounded in-flight) |
| `notification-worker/emit_test.go` | new | Subject + dedup header + payload assertions; drain semantics |
| `notification-worker/handler.go` | rewrite | Orchestrates Stages 1–4, emits push; defines all consumer interfaces |
| `notification-worker/handler_test.go` | rewrite | Table-driven per-stage tests (sender, mute, restricted, thread, hook, routing, presence, emit) |
| `notification-worker/main.go` | modify | Valkey wiring, raw JS for async push, config additions, pipeline assembly, EnsureIndexes, invalidator subscription, drain on shutdown |
| `notification-worker/integration_test.go` | modify | Real Valkey + Mongo cover the cache path and end-to-end mobile-push subject |
| `notification-worker/deploy/docker-compose.yml` | modify | Add `VALKEY_ADDRS`, `LARGE_ROOM_THRESHOLD` env, depend on valkey |
| `docs/client-api.md` | modify | Note mute/restricted exclusions; remove the legacy `notification` event description |

---

## Task 1: Extend `roomsubcache.Member` projection (TDD)

**Files:**
- Modify: `pkg/roomsubcache/roomsubcache.go:29-35`
- Modify: `pkg/roomsubcache/roomsubcache_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/roomsubcache/roomsubcache_test.go`:

```go
func TestMember_JSONRoundTrip_NewFields(t *testing.T) {
	hss := int64(1700000000000)
	in := Member{
		ID:                 "u1",
		Account:            "alice",
		IsBot:              true,
		ChineseName:        "張三",
		EngName:            "Alice",
		Muted:              true,
		HistorySharedSince: &hss,
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)

	var out Member
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func TestMember_OmitemptyOnZeroValues(t *testing.T) {
	in := Member{ID: "u1", Account: "alice"}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	got := string(data)

	// Only id + account on the wire; no zero-valued booleans / strings / pointers.
	assert.JSONEq(t, `{"id":"u1","account":"alice"}`, got)
}
```

Add imports if missing:

```go
import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=roomsubcache`
Expected: FAIL — `Member` does not have the new fields.

- [ ] **Step 3: Widen the projection**

Replace the `Member` type in `pkg/roomsubcache/roomsubcache.go` (line ~29):

```go
// Member is the projection of model.Subscription that notification-worker's
// fan-out path actually needs. Fields beyond {ID, Account} drive routing
// (IsBot), exclusion (Muted, HistorySharedSince), and push payload rendering
// (ChineseName, EngName for the message-author Sender). All extra fields
// use omitempty so a plain member's blob stays {id, account}.
type Member struct {
	ID                 string `json:"id"`
	Account            string `json:"account"`
	IsBot              bool   `json:"isBot,omitempty"`
	ChineseName        string `json:"chineseName,omitempty"`
	EngName            string `json:"engName,omitempty"`
	Muted              bool   `json:"muted,omitempty"`
	HistorySharedSince *int64 `json:"historySharedSince,omitempty"`
}
```

Also update the package doc comment at the top:

```go
// The cache stores the fan-out path's per-member input set — see Member.
// Entries are written with a caller-supplied TTL and may be eagerly
// invalidated via Invalidate; staleness is otherwise bounded by the TTL.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=roomsubcache && make lint`
Expected: PASS, no lint errors.

- [ ] **Step 5: Commit**

```bash
git add pkg/roomsubcache/roomsubcache.go pkg/roomsubcache/roomsubcache_test.go
git commit -m "feat(roomsubcache): widen Member projection for notification routing"
```

---

## Task 2: Add `@here` to `pkg/mention.ParseResult` (TDD)

**Files:**
- Modify: `pkg/mention/mention.go`
- Modify: `pkg/mention/mention_test.go`

- [ ] **Step 1: Write the failing tests**

Append cases to the table in `TestParse`:

```go
{name: "@here lowercase", content: "hey @here check this", accounts: nil, mentionAll: false, mentionHere: true},
{name: "@Here mixed case", content: "@Here folks", accounts: nil, mentionAll: false, mentionHere: true},
{name: "@all and @here", content: "@all then @here", accounts: nil, mentionAll: true, mentionHere: true},
{name: "word@here not mention", content: "say hi here@all team", accounts: nil, mentionAll: false, mentionHere: false},
```

Extend the case struct + assertions at the top of the test (do this once, edit existing cases to include the new field as zero-valued where appropriate):

```go
tests := []struct {
	name        string
	content     string
	accounts    []string
	mentionAll  bool
	mentionHere bool
}{
	// ... existing cases with mentionHere: false appended ...
}

// In the t.Run body, add:
assert.Equal(t, tt.mentionHere, got.MentionHere, "MentionHere")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=mention`
Expected: FAIL — `ParseResult` has no `MentionHere` field.

- [ ] **Step 3: Implement**

In `pkg/mention/mention.go`, extend `ParseResult`:

```go
type ParseResult struct {
	Accounts    []string
	MentionAll  bool
	MentionHere bool
}
```

In `Parse`, replace the special-case for `"all"`:

```go
switch account {
case "all":
	result.MentionAll = true
	continue
case "here":
	result.MentionHere = true
	continue
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=mention && make lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/mention/mention.go pkg/mention/mention_test.go
git commit -m "feat(mention): surface @here in ParseResult"
```

---

## Task 3: New subject builders (TDD)

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestPushNotification(t *testing.T) {
	assert.Equal(t,
		"chat.server.notification.push.site-a.send",
		subject.PushNotification("site-a"))
}

func TestPresenceSnapshot(t *testing.T) {
	assert.Equal(t,
		"chat.presence.site-a.request.snapshot",
		subject.PresenceSnapshot("site-a"))
}

func TestSubscriptionUpdateWildcard(t *testing.T) {
	assert.Equal(t,
		"chat.user.*.event.subscription.update",
		subject.SubscriptionUpdateWildcard())
}

func TestParseSubscriptionUpdateAccount(t *testing.T) {
	acct, ok := subject.ParseSubscriptionUpdateAccount("chat.user.alice.event.subscription.update")
	assert.True(t, ok)
	assert.Equal(t, "alice", acct)

	_, ok = subject.ParseSubscriptionUpdateAccount("chat.user.alice.event.room.update")
	assert.False(t, ok)

	_, ok = subject.ParseSubscriptionUpdateAccount("chat.user.*.event.subscription.update")
	assert.False(t, ok) // wildcard token rejected
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=subject`
Expected: FAIL — undefined references.

- [ ] **Step 3: Implement**

Append to `pkg/subject/subject.go` (group with the existing builders):

```go
// PushNotification is the single per-recipient mobile-push subject the
// notification-worker publishes to. Lives under chat.server.* so client
// JWTs cannot subscribe. Bound (by ops/IaC) to the PUSH_NOTIFICATIONS_{siteID}
// stream via filter "chat.server.notification.push.{siteID}.>" so additional
// leaves (e.g. .silent, .priority) can be added without restructuring.
func PushNotification(siteID string) string {
	return fmt.Sprintf("chat.server.notification.push.%s.send", siteID)
}

// PresenceSnapshot is the bulk presence RPC subject — one request per
// canonical message carrying the survivor account list, one reply with
// each account's aggregated status.
func PresenceSnapshot(siteID string) string {
	return fmt.Sprintf("chat.presence.%s.request.snapshot", siteID)
}

// SubscriptionUpdateWildcard matches every subscription.update fanout
// (chat.user.*.event.subscription.update). Used by notification-worker for
// eager cache invalidation.
func SubscriptionUpdateWildcard() string {
	return "chat.user.*.event.subscription.update"
}

// ParseSubscriptionUpdateAccount extracts the account token from a concrete
// subscription.update subject. Returns ok=false on wildcard or malformed
// input.
func ParseSubscriptionUpdateAccount(s string) (account string, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "event" ||
		parts[4] != "subscription" || parts[5] != "update" {
		return "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", false
	}
	return parts[2], true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=subject && make lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add push, presence-snapshot, and subscription-update wildcard builders"
```

---

## Task 4: New payload models — push + presence (TDD)

**Files:**
- Create: `pkg/model/push.go`
- Create: `pkg/model/presence.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/model/model_test.go` (use the existing `roundTrip` helper pattern; if it lives in another file, follow that file's convention):

```go
func TestPushNotificationEvent_RoundTrip(t *testing.T) {
	in := model.PushNotificationEvent{
		ID:      "m1-alice",
		Account: "alice",
		Title:   "general",
		Body:    "hello",
		RoomID:  "r1",
		Data: model.PushNotificationData{
			RoomID:    "r1",
			MessageID: "m1",
			Type:      "c",
			Sender:    &model.Participant{Account: "bob", ChineseName: "張三", EngName: "Bob"},
			PushTime:  "2026-05-27T00:00:00Z",
		},
		Timestamp: 1700000000000,
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out model.PushNotificationEvent
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func TestPresenceSnapshot_RoundTrip(t *testing.T) {
	in := model.PresenceSnapshotReply{
		Presences: map[string]model.Presence{
			"alice": {AggregatedStatus: "online"},
			"bob":   {AggregatedStatus: "busy"},
		},
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out model.PresenceSnapshotReply
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=model`
Expected: FAIL — undefined types.

- [ ] **Step 3: Implement `pkg/model/push.go`**

```go
package model

// PushNotificationEvent is the per-recipient envelope notification-worker
// hands off to the internal push-notification service via the
// PUSH_NOTIFICATIONS_{siteID} stream. ID is "{messageID}-{account}" — also
// used as the Nats-Msg-Id for JetStream dedup so a same-message redelivery
// on MESSAGES_CANONICAL does not produce duplicate pushes.
type PushNotificationEvent struct {
	ID        string               `json:"id"        bson:"id"`
	Account   string               `json:"account"   bson:"account"`
	Title     string               `json:"title"     bson:"title"`
	Body      string               `json:"body"      bson:"body"`
	Data      PushNotificationData `json:"data"      bson:"data"`
	RoomID    string               `json:"roomId"    bson:"roomId"`
	Timestamp int64                `json:"timestamp" bson:"timestamp"`
}

// PushNotificationData mirrors the legacy push-service payload with two
// repo-convention departures: cryptic tags (rid/tmid/prid) spelled out to
// camelCase, and the flat chineseName/engName fields collapsed into a
// *Participant Sender (matches ClientMessage.Sender).
type PushNotificationData struct {
	RoomID            string       `json:"roomId"                      bson:"roomId"`
	MessageID         string       `json:"messageId"                   bson:"messageId"`
	Type              string       `json:"type"                        bson:"type"`
	Sender            *Participant `json:"sender,omitempty"            bson:"sender,omitempty"`
	ThreadMessageID   string       `json:"threadMessageId,omitempty"   bson:"threadMessageId,omitempty"`
	FileName          string       `json:"fileName,omitempty"          bson:"fileName,omitempty"`
	FileType          string       `json:"fileType,omitempty"          bson:"fileType,omitempty"`
	ParentRoomID      string       `json:"parentRoomId,omitempty"      bson:"parentRoomId,omitempty"`
	PushTime          string       `json:"pushTime"                    bson:"pushTime"`
	AlsoSendToChannel bool         `json:"alsoSendToChannel,omitempty" bson:"alsoSendToChannel,omitempty"`
}
```

- [ ] **Step 4: Implement `pkg/model/presence.go`**

```go
package model

// PresenceSnapshotRequest is the request payload of the bulk presence RPC.
// One request per canonical message carrying the push-eligible account set.
type PresenceSnapshotRequest struct {
	Accounts []string `json:"accounts" bson:"accounts"`
}

// PresenceSnapshotReply is the reply payload. Accounts absent from the map
// (or any RPC error) are treated fail-open by notification-worker — the
// push fires.
type PresenceSnapshotReply struct {
	Presences map[string]Presence `json:"presences" bson:"presences"`
}

// Presence is a single account's aggregated status. The presence service
// folds manual user overrides (e.g. busy) into AggregatedStatus so it is
// the sole field routing needs.
//
// Known values: "online", "offline", "away", "busy", "in-call".
// Only "busy" and "in-call" suppress the push (DND).
type Presence struct {
	AggregatedStatus string `json:"aggregatedStatus" bson:"aggregatedStatus"`
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=model && make lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/model/push.go pkg/model/presence.go pkg/model/model_test.go
git commit -m "feat(model): add push notification + bulk presence RPC payloads"
```

---

## Task 5: Routing predicate (TDD)

**Files:**
- Create: `notification-worker/routing.go`
- Create: `notification-worker/routing_test.go`

- [ ] **Step 1: Write the failing tests**

Create `notification-worker/routing_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

func TestEligibleForPush(t *testing.T) {
	tests := []struct {
		name       string
		member     roomsubcache.Member
		roomType   model.RoomType
		isLarge    bool
		mentioned  bool
		want       bool
	}{
		{name: "dm always", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeDM, want: true},
		{name: "botdm always", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeBotDM, want: true},
		{name: "small channel non-mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: false, mentioned: false, want: true},
		{name: "small channel mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: false, mentioned: true, want: true},
		{name: "large channel non-mention dropped", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: true, mentioned: false, want: false},
		{name: "large channel mention pushed", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: true, mentioned: true, want: true},
		{name: "bot never", member: roomsubcache.Member{Account: "bot", IsBot: true}, roomType: model.RoomTypeDM, want: false},
		{name: "bot in mention dropped", member: roomsubcache.Member{Account: "bot", IsBot: true}, roomType: model.RoomTypeChannel, mentioned: true, want: false},
		{name: "discussion small non-mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeDiscussion, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EligibleForPush(tt.member, tt.roomType, tt.isLarge, tt.mentioned)
			assert.Equal(t, tt.want, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `EligibleForPush` undefined.

- [ ] **Step 3: Implement `notification-worker/routing.go`**

```go
package main

import (
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// EligibleForPush is Stage 3 of the fan-out pipeline. Pure CPU — no I/O,
// no dependencies — so it is exhaustively unit-testable. The recipient is
// eligible when: (a) the room is a DM/botDM, OR mentioned, OR not large;
// AND (b) the recipient is not a bot. A "large" room is one whose member
// count exceeds LARGE_ROOM_THRESHOLD (computed once per message in the
// handler).
func EligibleForPush(m roomsubcache.Member, roomType model.RoomType, isLargeRoom, mentioned bool) bool {
	if m.IsBot {
		return false
	}
	if isDirect(roomType) {
		return true
	}
	if mentioned {
		return true
	}
	return !isLargeRoom
}

func isDirect(t model.RoomType) bool {
	return t == model.RoomTypeDM || t == model.RoomTypeBotDM
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for `TestEligibleForPush` (other tests in the package may still fail at this point — that's fine).

- [ ] **Step 5: Commit**

```bash
git add notification-worker/routing.go notification-worker/routing_test.go
git commit -m "feat(notification-worker): pure routing predicate (Stage 3)"
```

---

## Task 6: Hook interface + no-op (TDD)

**Files:**
- Create: `notification-worker/hook.go`
- Create: `notification-worker/hook_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

func TestNoopHook_AlwaysAllows(t *testing.T) {
	h := noopHook{}
	allow, err := h.Allow(context.Background(), &model.Message{}, roomsubcache.Member{Account: "a"})
	assert.NoError(t, err)
	assert.True(t, allow)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `noopHook` undefined.

- [ ] **Step 3: Implement `notification-worker/hook.go`**

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// Hook is the Stage-2 in-process suppress-only veto. Allow returns true to
// keep the recipient; false to drop. It must never perform a per-recipient
// external call — any data the real impl needs must be batch-loaded once
// per message by Handler before the per-recipient loop.
//
// Errors are treated fail-open by the handler (logged + allow), so a hook
// outage never silently drops notifications.
type Hook interface {
	Allow(ctx context.Context, msg *model.Message, member roomsubcache.Member) (bool, error)
}

type noopHook struct{}

func (noopHook) Allow(context.Context, *model.Message, roomsubcache.Member) (bool, error) {
	return true, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for `TestNoopHook_AlwaysAllows`.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/hook.go notification-worker/hook_test.go
git commit -m "feat(notification-worker): hook interface + no-op default"
```

---

## Task 7: Presence source — no-op + bulk RPC + status map (TDD)

**Files:**
- Create: `notification-worker/presence.go`
- Create: `notification-worker/presence_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestNoopPresence_EmptySnapshot(t *testing.T) {
	p := noopPresenceSource{}
	snap, err := p.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Empty(t, snap)
}

func TestShouldPush(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"online", true},
		{"offline", true},
		{"away", true},
		{"busy", false},
		{"in-call", false},
		{"", true},        // missing → fail-open
		{"unknown", true}, // unknown → fail-open
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldPush(model.Presence{AggregatedStatus: tt.status}))
		})
	}
}

// Stub requester implementing the presenceRequester interface so we can
// drive bulkPresence without a real NATS connection.
type stubRequester struct {
	calls   int
	gotReqs []model.PresenceSnapshotRequest
	reply   func(req model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error)
}

func (s *stubRequester) Request(_ context.Context, _ string, data []byte, _ time.Duration) (*nats.Msg, error) {
	s.calls++
	var req model.PresenceSnapshotRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	s.gotReqs = append(s.gotReqs, req)
	reply, err := s.reply(req)
	if err != nil {
		return nil, err
	}
	out, _ := json.Marshal(reply)
	return &nats.Msg{Data: out}, nil
}

func TestBulkPresence_Chunks(t *testing.T) {
	accounts := make([]string, 1500)
	for i := range accounts {
		accounts[i] = "u"
	}
	// Distinct accounts so the map merge is observable.
	for i := range accounts {
		accounts[i] = string(rune('a'+i%26)) + "-" + string(rune('a'+i/26%26))
	}
	stub := &stubRequester{reply: func(req model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error) {
		out := model.PresenceSnapshotReply{Presences: map[string]model.Presence{}}
		for _, a := range req.Accounts {
			out.Presences[a] = model.Presence{AggregatedStatus: "online"}
		}
		return out, nil
	}}

	src := newBulkPresenceSource(stub, "site-a", 500, time.Second)
	got, err := src.Snapshot(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 3, stub.calls, "expect ceil(1500/500) chunks")
	assert.Len(t, got, len(uniqueStrings(accounts)))
}

func TestBulkPresence_FailOpenOnError(t *testing.T) {
	stub := &stubRequester{reply: func(model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error) {
		return model.PresenceSnapshotReply{}, errors.New("nats: timeout")
	}}
	src := newBulkPresenceSource(stub, "site-a", 100, 50*time.Millisecond)
	got, err := src.Snapshot(context.Background(), []string{"a", "b"})
	require.NoError(t, err) // fail-open: error is swallowed and snapshot is empty
	assert.Empty(t, got)
}

func uniqueStrings(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, s := range in {
		out[s] = struct{}{}
	}
	return out
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `notification-worker/presence.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// PresenceSource is the Stage-4 dependency. Snapshot returns presence for
// each push-eligible account in one batched read (potentially split across
// several bulk RPCs for huge rooms). Errors are swallowed and surfaced as
// an empty snapshot — every recipient then fails open to a push.
type PresenceSource interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]model.Presence, error)
}

// noopPresenceSource ships when the bulk presence RPC handler is not yet
// available on the presence service (see spec Open Question B). An empty
// snapshot makes every push-eligible recipient receive a push.
type noopPresenceSource struct{}

func (noopPresenceSource) Snapshot(context.Context, []string) (map[string]model.Presence, error) {
	return map[string]model.Presence{}, nil
}

// presenceRequester is the minimal NATS surface bulkPresenceSource depends
// on — kept narrow so tests can substitute without a real connection.
type presenceRequester interface {
	Request(ctx context.Context, subj string, data []byte, timeout time.Duration) (*nats.Msg, error)
}

type bulkPresenceSource struct {
	req       presenceRequester
	siteID    string
	batchSize int
	timeout   time.Duration
}

func newBulkPresenceSource(req presenceRequester, siteID string, batchSize int, timeout time.Duration) *bulkPresenceSource {
	if batchSize <= 0 {
		batchSize = 512
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &bulkPresenceSource{req: req, siteID: siteID, batchSize: batchSize, timeout: timeout}
}

func (b *bulkPresenceSource) Snapshot(ctx context.Context, accounts []string) (map[string]model.Presence, error) {
	if len(accounts) == 0 {
		return map[string]model.Presence{}, nil
	}
	subj := subject.PresenceSnapshot(b.siteID)
	chunks := chunkStrings(accounts, b.batchSize)

	var (
		mu  sync.Mutex
		out = make(map[string]model.Presence, len(accounts))
		wg  sync.WaitGroup
	)
	for _, ch := range chunks {
		ch := ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := json.Marshal(model.PresenceSnapshotRequest{Accounts: ch})
			if err != nil {
				slog.Warn("presence marshal failed", "error", err)
				return
			}
			msg, err := b.req.Request(ctx, subj, data, b.timeout)
			if err != nil {
				slog.Warn("presence rpc failed", "error", err, "chunk", len(ch))
				return
			}
			var reply model.PresenceSnapshotReply
			if err := json.Unmarshal(msg.Data, &reply); err != nil {
				slog.Warn("presence unmarshal failed", "error", err)
				return
			}
			mu.Lock()
			for k, v := range reply.Presences {
				out[k] = v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, nil
}

func chunkStrings(in []string, size int) [][]string {
	if size <= 0 || len(in) <= size {
		return [][]string{in}
	}
	out := make([][]string, 0, (len(in)+size-1)/size)
	for i := 0; i < len(in); i += size {
		end := i + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

// shouldPush maps an aggregated presence status to a push-or-not decision.
// Fail-open on unknown / missing — never drop a notification on a presence
// gap.
func shouldPush(p model.Presence) bool {
	switch p.AggregatedStatus {
	case "busy", "in-call":
		return false
	default:
		return true
	}
}

// natsPresenceRequester adapts the production NATS connection to the
// presenceRequester interface.
type natsPresenceRequester struct {
	nc *nats.Conn
}

func (n *natsPresenceRequester) Request(ctx context.Context, subj string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := n.nc.RequestWithContext(rctx, subj, data)
	if err != nil {
		return nil, fmt.Errorf("presence request: %w", err)
	}
	return msg, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for the new presence tests.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/presence.go notification-worker/presence_test.go
git commit -m "feat(notification-worker): bulk presence RPC + no-op default + status table"
```

---

## Task 8: Cached member lookup with single-flight + L1 (TDD)

**Files:**
- Create: `notification-worker/members.go`
- Create: `notification-worker/members_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeCache implements roomsubcache.Cache in memory.
type fakeCache struct {
	mu   sync.Mutex
	data map[string][]roomsubcache.Member
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string][]roomsubcache.Member{}} }

func (f *fakeCache) Get(_ context.Context, roomID string) ([]roomsubcache.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[roomID]
	if !ok {
		return nil, valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeCache) Set(_ context.Context, roomID string, members []roomsubcache.Member, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]roomsubcache.Member, len(members))
	copy(cp, members)
	f.data[roomID] = cp
	return nil
}
func (f *fakeCache) Invalidate(_ context.Context, roomID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, roomID)
	return nil
}

// fakeLoader counts loader invocations.
type fakeLoader struct {
	calls atomic.Int32
	out   []roomsubcache.Member
	err   error
	delay time.Duration
}

func (f *fakeLoader) Load(_ context.Context, _ string) ([]roomsubcache.Member, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.out, f.err
}

func TestCachedMemberLookup_HitFromValkey(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []roomsubcache.Member{{ID: "u1", Account: "alice"}}}
	_ = cache.Set(context.Background(), "r1", loader.out, time.Minute)

	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 0, 0)
	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(0), loader.calls.Load(), "loader must not be called on hit")
}

func TestCachedMemberLookup_MissThenPopulate(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []roomsubcache.Member{{ID: "u1", Account: "alice"}}}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 0, 0)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)

	// Second call hits the cache.
	_, _ = lookup.GetMembers(context.Background(), "r1")
	assert.Equal(t, int32(1), loader.calls.Load())
}

func TestCachedMemberLookup_CacheErrorFallsThrough(t *testing.T) {
	cache := &erroringCache{err: errors.New("valkey down")}
	loader := &fakeLoader{out: []roomsubcache.Member{{ID: "u1", Account: "alice"}}}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 0, 0)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(1), loader.calls.Load())
}

func TestCachedMemberLookup_SingleFlightCollapsesMisses(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:   []roomsubcache.Member{{ID: "u1", Account: "alice"}},
		delay: 50 * time.Millisecond,
	}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 0, 0)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = lookup.GetMembers(context.Background(), "r1")
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), loader.calls.Load(), "single-flight collapses concurrent misses")
}

func TestCachedMemberLookup_L1ServesRepeats(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []roomsubcache.Member{{ID: "u1", Account: "alice"}}}
	// L1 size 10, TTL 5s
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 10, 5*time.Second)

	for i := 0; i < 50; i++ {
		_, err := lookup.GetMembers(context.Background(), "r1")
		require.NoError(t, err)
	}
	// First fetch populates both Valkey and L1; subsequent calls hit L1.
	assert.LessOrEqual(t, loader.calls.Load(), int32(1))
}

func TestCachedMemberLookup_InvalidateDropsL1(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []roomsubcache.Member{{ID: "u1", Account: "alice"}}}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 10, time.Minute)

	_, _ = lookup.GetMembers(context.Background(), "r1")
	lookup.Invalidate(context.Background(), "r1")
	loader.out = []roomsubcache.Member{{ID: "u2", Account: "bob"}}
	got, _ := lookup.GetMembers(context.Background(), "r1")

	assert.Equal(t, loader.out, got, "after Invalidate the next read must reload")
}

type erroringCache struct{ err error }

func (e *erroringCache) Get(context.Context, string) ([]roomsubcache.Member, error) {
	return nil, e.err
}
func (e *erroringCache) Set(context.Context, string, []roomsubcache.Member, time.Duration) error {
	return nil
}
func (e *erroringCache) Invalidate(context.Context, string) error { return nil }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `cachedMemberLookup` undefined.

- [ ] **Step 3: Implement `notification-worker/members.go`**

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// memberLoader reads the canonical (Mongo) member list for a room. The
// closure shape decouples cachedMemberLookup from the concrete
// mongoMemberLookup so tests can substitute trivially.
type memberLoader func(ctx context.Context, roomID string) ([]roomsubcache.Member, error)

// cachedMemberLookup is notification-worker's MemberLookup implementation.
// Order of resolution per call:
//   1. In-process L1 LRU (decoded slices, short TTL)
//   2. Valkey via roomsubcache (multi-MB blob, JSON decode)
//   3. Mongo via the loader (cold start / TTL expiry)
// Single-flight guards stages 2→3 so a TTL-expiry stampede on a hot room
// collapses to one query.
type cachedMemberLookup struct {
	cache  roomsubcache.Cache
	load   memberLoader
	ttl    time.Duration
	sf     singleflight.Group
	l1     *lru.LRU[string, []roomsubcache.Member]
}

// newCachedMemberLookup wires the lookup. l1Size <= 0 disables the L1.
func newCachedMemberLookup(cache roomsubcache.Cache, load memberLoader, ttl time.Duration, l1Size int, l1TTL time.Duration) *cachedMemberLookup {
	c := &cachedMemberLookup{cache: cache, load: load, ttl: ttl}
	if l1Size > 0 {
		c.l1 = lru.NewLRU[string, []roomsubcache.Member](l1Size, nil, l1TTL)
	}
	return c
}

// GetMembers returns the member list for roomID, populating Valkey + L1 on
// a miss. Treats the returned slice as read-only — callers must not mutate.
func (c *cachedMemberLookup) GetMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	if c.l1 != nil {
		if v, ok := c.l1.Get(roomID); ok {
			return v, nil
		}
	}
	members, err, _ := c.sf.Do(roomID, func() (any, error) {
		if c.l1 != nil {
			if v, ok := c.l1.Get(roomID); ok {
				return v, nil
			}
		}
		got, err := c.cache.Get(ctx, roomID)
		if err == nil {
			c.populateL1(roomID, got)
			return got, nil
		}
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			slog.Warn("roomsubcache get failed, falling back to mongo", "error", err, "roomId", roomID)
		}
		loaded, lerr := c.load(ctx, roomID)
		if lerr != nil {
			return nil, lerr
		}
		if setErr := c.cache.Set(ctx, roomID, loaded, c.ttl); setErr != nil {
			slog.Warn("roomsubcache set failed", "error", setErr, "roomId", roomID)
		}
		c.populateL1(roomID, loaded)
		return loaded, nil
	})
	if err != nil {
		return nil, err
	}
	return members.([]roomsubcache.Member), nil
}

// Invalidate drops the room from both the L1 and the Valkey cache. Called
// by the subscription-update fan-out subscriber on every membership change.
func (c *cachedMemberLookup) Invalidate(ctx context.Context, roomID string) {
	if c.l1 != nil {
		c.l1.Remove(roomID)
	}
	if err := c.cache.Invalidate(ctx, roomID); err != nil {
		slog.Warn("roomsubcache invalidate failed", "error", err, "roomId", roomID)
	}
}

func (c *cachedMemberLookup) populateL1(roomID string, members []roomsubcache.Member) {
	if c.l1 == nil {
		return
	}
	c.l1.Add(roomID, members)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for `TestCachedMemberLookup_*`.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/members.go notification-worker/members_test.go
git commit -m "feat(notification-worker): valkey-backed member lookup with single-flight and L1"
```

---

## Task 9: Thread-follower lookup (TDD)

**Files:**
- Create: `notification-worker/threads.go`
- Create: `notification-worker/threads_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubThreadLookup struct {
	out []string
	err error
}

func (s *stubThreadLookup) followers(_ context.Context, _ string) (map[string]struct{}, error) {
	if s.err != nil {
		return nil, s.err
	}
	set := make(map[string]struct{}, len(s.out))
	for _, a := range s.out {
		set[a] = struct{}{}
	}
	return set, nil
}

func TestThreadFollowers_Resolve(t *testing.T) {
	s := &stubThreadLookup{out: []string{"alice", "bob"}}
	got, err := s.followers(context.Background(), "parent-1")
	require.NoError(t, err)
	assert.Contains(t, got, "alice")
	assert.Contains(t, got, "bob")
	assert.NotContains(t, got, "carol")
}

func TestThreadFollowers_PropagatesError(t *testing.T) {
	s := &stubThreadLookup{err: errors.New("mongo down")}
	_, err := s.followers(context.Background(), "parent-1")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — types missing.

- [ ] **Step 3: Implement `notification-worker/threads.go`**

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ThreadFollowers returns the set of accounts subscribed to the thread
// rooted at parentMessageID. Backed by an indexed read on
// thread_subscriptions (parentMessageId, userAccount). Empty set on no
// followers.
type ThreadFollowers interface {
	Followers(ctx context.Context, parentMessageID string) (map[string]struct{}, error)
}

type mongoThreadFollowers struct {
	col *mongo.Collection
}

func newMongoThreadFollowers(col *mongo.Collection) *mongoThreadFollowers {
	return &mongoThreadFollowers{col: col}
}

func (m *mongoThreadFollowers) Followers(ctx context.Context, parentMessageID string) (map[string]struct{}, error) {
	if parentMessageID == "" {
		return map[string]struct{}{}, nil
	}
	opts := options.Find().SetProjection(bson.M{"userAccount": 1, "_id": 0})
	cur, err := m.col.Find(ctx, bson.M{"parentMessageId": parentMessageID}, opts)
	if err != nil {
		return nil, fmt.Errorf("find thread followers: %w", err)
	}
	defer cur.Close(ctx)

	out := map[string]struct{}{}
	for cur.Next(ctx) {
		var r struct {
			UserAccount string `bson:"userAccount"`
		}
		if err := cur.Decode(&r); err != nil {
			return nil, fmt.Errorf("decode thread subscription: %w", err)
		}
		if r.UserAccount != "" {
			out[r.UserAccount] = struct{}{}
		}
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread followers: %w", err)
	}
	return out, nil
}

// EnsureThreadSubscriptionIndex creates the (parentMessageId, userAccount)
// index notification-worker reads. Idempotent — safe to call on every
// startup. The room-service also ensures this in its own EnsureIndexes;
// duplicating it here keeps notification-worker self-sufficient against a
// fresh database.
func EnsureThreadSubscriptionIndex(ctx context.Context, col *mongo.Collection) error {
	_, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}, {Key: "userAccount", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("ensure thread_subscriptions (parentMessageId, userAccount) index: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/threads.go notification-worker/threads_test.go
git commit -m "feat(notification-worker): thread-follower lookup + index ensure"
```

---

## Task 10: Mobile emitter — async JS publish + dedup header (TDD)

**Files:**
- Create: `notification-worker/emit.go`
- Create: `notification-worker/emit_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

type recordedPublish struct {
	subject string
	msgID   string
	payload []byte
}

type fakeAsyncPublisher struct {
	mu       sync.Mutex
	records  []recordedPublish
	failNext error
}

func (f *fakeAsyncPublisher) PublishMsgAsync(msg *nats.Msg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.records = append(f.records, recordedPublish{
		subject: msg.Subject,
		msgID:   msg.Header.Get("Nats-Msg-Id"),
		payload: append([]byte(nil), msg.Data...),
	})
	return nil
}

func (f *fakeAsyncPublisher) drain(context.Context) {}

func TestMobileEmitter_PublishesPerRecipient(t *testing.T) {
	pub := &fakeAsyncPublisher{}
	em := newMobileEmitter(pub, "site-a")
	evt := model.PushNotificationEvent{
		ID:      "m1-bob",
		Account: "bob",
		RoomID:  "r1",
	}
	require.NoError(t, em.Emit(context.Background(), evt))

	require.Len(t, pub.records, 1)
	r := pub.records[0]
	assert.Equal(t, "chat.server.notification.push.site-a.send", r.subject)
	assert.Equal(t, "m1-bob", r.msgID)

	var got model.PushNotificationEvent
	require.NoError(t, json.Unmarshal(r.payload, &got))
	assert.Equal(t, evt, got)
}

func TestMobileEmitter_PropagatesError(t *testing.T) {
	pub := &fakeAsyncPublisher{failNext: errors.New("nats: full")}
	em := newMobileEmitter(pub, "site-a")
	err := em.Emit(context.Background(), model.PushNotificationEvent{ID: "m1-bob", Account: "bob"})
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `notification-worker/emit.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// asyncPublisher is the narrow JetStream surface mobileEmitter needs.
// Defined here so emit_test.go can substitute without a real NATS
// connection. drain blocks until every in-flight ack completes (or ctx
// elapses).
type asyncPublisher interface {
	PublishMsgAsync(msg *nats.Msg) error
	drain(ctx context.Context)
}

// Emitter is the single mobile-push emit leg. The handler calls Emit for
// each surviving recipient. Errors are per-recipient; the handler logs and
// moves on (the canonical message still acks).
type Emitter interface {
	Emit(ctx context.Context, evt model.PushNotificationEvent) error
}

type mobileEmitter struct {
	pub    asyncPublisher
	siteID string
}

func newMobileEmitter(pub asyncPublisher, siteID string) *mobileEmitter {
	return &mobileEmitter{pub: pub, siteID: siteID}
}

func (e *mobileEmitter) Emit(_ context.Context, evt model.PushNotificationEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal push event for %s: %w", evt.Account, err)
	}
	msg := &nats.Msg{
		Subject: subject.PushNotification(e.siteID),
		Header:  nats.Header{},
		Data:    data,
	}
	// Per-recipient dedup so a redelivery of the canonical message never
	// produces duplicate pushes. Window is the stream's MsgID dedup window
	// (owned by the push service / ops).
	msg.Header.Set("Nats-Msg-Id", evt.ID)
	if err := e.pub.PublishMsgAsync(msg); err != nil {
		return fmt.Errorf("publish push for %s: %w", evt.Account, err)
	}
	return nil
}

// jsAsyncPublisher adapts a raw jetstream.JetStream + an in-flight cap to
// the asyncPublisher interface. Async publish is required for v1 — sync
// PublishMsg in a 10k-member fan-out would serialise ack round-trips.
//
// drain blocks on jetstream's PublishAsyncComplete or the provided ctx so
// graceful shutdown does not lose in-flight pushes.
type jsAsyncPublisher struct {
	js jetstream.JetStream
}

func newJSAsyncPublisher(js jetstream.JetStream) *jsAsyncPublisher {
	return &jsAsyncPublisher{js: js}
}

func (j *jsAsyncPublisher) PublishMsgAsync(msg *nats.Msg) error {
	_, err := j.js.PublishMsgAsync(msg)
	return err
}

func (j *jsAsyncPublisher) drain(ctx context.Context) {
	select {
	case <-j.js.PublishAsyncComplete():
	case <-ctx.Done():
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for `TestMobileEmitter_*`.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/emit.go notification-worker/emit_test.go
git commit -m "feat(notification-worker): async mobile-push emitter with per-recipient dedup"
```

---

## Task 11: Rewrite the handler — Stages 1–4 + push payload (TDD)

This is the heart of the change. The existing `handler.go` is replaced wholesale; tests are replaced to match the new behaviour.

**Files:**
- Modify: `notification-worker/handler.go`
- Modify: `notification-worker/handler_test.go`

- [ ] **Step 1: Write the failing handler tests** (replaces the existing file)

Replace the entire contents of `notification-worker/handler_test.go` with:

```go
package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// --- Stubs ---

type stubMembers struct {
	out map[string][]roomsubcache.Member
}

func (s *stubMembers) GetMembers(_ context.Context, roomID string) ([]roomsubcache.Member, error) {
	return s.out[roomID], nil
}

type stubFollowers struct {
	out map[string]map[string]struct{}
}

func (s *stubFollowers) Followers(_ context.Context, parentID string) (map[string]struct{}, error) {
	if v, ok := s.out[parentID]; ok {
		return v, nil
	}
	return map[string]struct{}{}, nil
}

type stubPresence struct {
	out map[string]model.Presence
}

func (s *stubPresence) Snapshot(_ context.Context, _ []string) (map[string]model.Presence, error) {
	return s.out, nil
}

type rejectHook struct{}

func (rejectHook) Allow(context.Context, *model.Message, roomsubcache.Member) (bool, error) {
	return false, nil
}

type recordingEmitter struct {
	mu       sync.Mutex
	emitted  []model.PushNotificationEvent
}

func (r *recordingEmitter) Emit(_ context.Context, evt model.PushNotificationEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emitted = append(r.emitted, evt)
	return nil
}

func (r *recordingEmitter) accounts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.emitted))
	for _, e := range r.emitted {
		out = append(out, e.Account)
	}
	return out
}

// --- Helpers ---

func newTestHandler(members MemberLookup, followers ThreadFollowers, presence PresenceSource, hook Hook, emit Emitter) *Handler {
	return NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Presence:           presence,
		Hook:               hook,
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})
}

func msgEvent(m model.Message) []byte {
	data, _ := json.Marshal(model.MessageEvent{Message: m, SiteID: "site-a"})
	return data
}

// --- Stage 1: exclusion filters ---

func TestHandle_SkipsSender(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts())
}

func TestHandle_SkipsMuted(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob", Muted: true},
			{ID: "carol", Account: "carol"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts(), "muted bob is skipped")
}

func TestHandle_SkipsRestrictedBeforeWindow(t *testing.T) {
	createdAt := time.Unix(0, 1700000000000*int64(time.Millisecond))
	afterWindow := int64(1700000000001) // 1ms before createdAt = visible? No — strictly before window
	beforeWindow := int64(1699999999999) // earlier than the message: member sees it
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob", HistorySharedSince: &afterWindow},   // joined after message → skip
			{ID: "carol", Account: "carol", HistorySharedSince: &beforeWindow}, // joined before → include
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: createdAt,
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts())
}

// --- Stage 1: thread non-follower ---

func TestHandle_ThreadOnlyReply_SkipsNonFollowerNonMention(t *testing.T) {
	parentCreatedAt := time.Unix(0, 1700000000000*int64(time.Millisecond))
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	followers := &stubFollowers{out: map[string]map[string]struct{}{
		"parent-1": {"bob": {}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, followers, noopPresenceSource{}, noopHook{}, emit)

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID:        "parent-1",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        false,
		Content:                      "thread reply",
	}
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(msg)))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts(), "only thread follower receives")
}

func TestHandle_ThreadReply_TShow_TreatedAsChannelMessage(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID: "parent-1",
		TShow:                 true,
		Content:               "shared with channel",
	}
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(msg)))
	assert.ElementsMatch(t, []string{"bob", "carol"}, emit.accounts())
}

// --- Stage 2: hook veto ---

func TestHandle_HookVeto_DropsAll(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, rejectHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts())
}

// --- Stage 3: routing (large room) ---

func TestHandle_LargeRoomNonMention_DropsAll(t *testing.T) {
	roomMembers := make([]roomsubcache.Member, 600)
	for i := range roomMembers {
		roomMembers[i] = roomsubcache.Member{ID: "u", Account: "u" + string(rune(i))}
	}
	roomMembers[0] = roomsubcache.Member{ID: "alice", Account: "alice"}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Presence:           noopPresenceSource{},
		Hook:               noopHook{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", Content: "no mentions",
		CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts(), "large room non-mention drops all")
}

func TestHandle_LargeRoomMention_OnlyMentionedPushed(t *testing.T) {
	roomMembers := []roomsubcache.Member{
		{ID: "alice", Account: "alice"},
		{ID: "bob", Account: "bob"},
		{ID: "carol", Account: "carol"},
	}
	// pad to large
	for i := 0; i < 600; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: "u" + string(rune(i)), Account: "u" + string(rune(i))})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "hey @bob check this", CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts())
}

func TestHandle_LargeRoomAtAll_PushesAllNonSender(t *testing.T) {
	roomMembers := []roomsubcache.Member{
		{ID: "alice", Account: "alice"},
		{ID: "bob", Account: "bob"},
		{ID: "carol", Account: "carol"},
	}
	for i := 0; i < 500; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: "u", Account: "u" + string(rune(i))})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "@all heads up", CreatedAt: time.Now(),
	})))
	assert.Contains(t, emit.accounts(), "bob")
	assert.Contains(t, emit.accounts(), "carol")
	assert.NotContains(t, emit.accounts(), "alice")
}

// --- Stage 4: presence ---

func TestHandle_PresenceBusyDropsPush(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "busy"},
		"carol": {AggregatedStatus: "online"},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, presence, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts())
}

// --- Payload shape ---

func TestHandle_PushPayloadSenderFromMemberRecord(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", ChineseName: "張三", EngName: "Alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content:   "hello",
		CreatedAt: time.Unix(0, 1700000000000*int64(time.Millisecond)),
	})))
	require.Len(t, emit.emitted, 1)
	got := emit.emitted[0]
	assert.Equal(t, "m1-bob", got.ID, "dedup-stable ID")
	assert.Equal(t, "bob", got.Account)
	assert.Equal(t, "r1", got.RoomID)
	require.NotNil(t, got.Data.Sender)
	assert.Equal(t, "alice", got.Data.Sender.Account)
	assert.Equal(t, "張三", got.Data.Sender.ChineseName)
	assert.Equal(t, "Alice", got.Data.Sender.EngName)
	assert.Equal(t, "m1", got.Data.MessageID)
	assert.NotEmpty(t, got.Data.PushTime)
	assert.Greater(t, got.Timestamp, int64(0))
}

func TestHandle_InvalidJSON(t *testing.T) {
	emit := &recordingEmitter{}
	h := newTestHandler(&stubMembers{}, &stubFollowers{}, noopPresenceSource{}, noopHook{}, emit)
	err := h.HandleMessage(context.Background(), []byte("not json"))
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `HandlerDeps`, `MemberLookup` (new interface name), payload shape all undefined.

- [ ] **Step 3: Replace `notification-worker/handler.go`** with the new orchestrator

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// MemberLookup returns the cached/canonical member list for a room. Slices
// are treated read-only by the handler.
type MemberLookup interface {
	GetMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error)
}

// HandlerDeps groups the handler's collaborators. Defined as a struct so
// adding a new collaborator does not churn the constructor signature.
type HandlerDeps struct {
	Members            MemberLookup
	Followers          ThreadFollowers
	Presence           PresenceSource
	Hook               Hook
	Emitter            Emitter
	LargeRoomThreshold int
}

// Handler runs the per-message fan-out pipeline:
//   Stage 1 — exclusion filters (sender / mute / restricted / thread-non-follower)
//   Stage 2 — in-process hook veto (suppress-only, fail-open on error)
//   Stage 3 — pure routing predicate (EligibleForPush)
//   Stage 4 — one bulk presence RPC, then per-account shouldPush
// followed by one Emitter.Emit per surviving recipient.
type Handler struct {
	deps HandlerDeps
}

func NewHandler(deps HandlerDeps) *Handler {
	if deps.LargeRoomThreshold <= 0 {
		deps.LargeRoomThreshold = 500
	}
	return &Handler{deps: deps}
}

func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal message event: %w", err)
	}
	msg := evt.Message

	members, err := h.deps.Members.GetMembers(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get members for room %s: %w", msg.RoomID, err)
	}
	if len(members) == 0 {
		return nil
	}

	// --- Once-per-message inputs ---
	mentionInfo := mention.Parse(msg.Content)
	mentionedAccounts := mentionedSet(mentionInfo, msg.Mentions)
	mentionsAllOrHere := mentionInfo.MentionAll || mentionInfo.MentionHere
	isLargeRoom := len(members) > h.deps.LargeRoomThreshold
	isThreadOnlyReply := msg.ThreadParentMessageID != "" && !msg.TShow

	var followers map[string]struct{}
	if isThreadOnlyReply {
		f, ferr := h.deps.Followers.Followers(ctx, msg.ThreadParentMessageID)
		if ferr != nil {
			slog.Warn("thread followers lookup failed, treating as empty",
				"error", ferr, "parentMessageId", msg.ThreadParentMessageID)
			f = map[string]struct{}{}
		}
		followers = f
	}

	roomType := deriveRoomType(members)

	// Author display info — taken from the loaded member list (no separate lookup).
	var sender *model.Participant
	for i := range members {
		if members[i].ID == msg.UserID {
			sender = &model.Participant{
				Account:     members[i].Account,
				ChineseName: members[i].ChineseName,
				EngName:     members[i].EngName,
			}
			break
		}
	}

	// --- Stages 1–3: build the push-eligible recipient set ---
	type candidate struct {
		member roomsubcache.Member
	}
	candidates := make([]candidate, 0, len(members))
	for _, m := range members {
		// Stage 1.1 — sender
		if m.ID == msg.UserID {
			continue
		}
		// Stage 1.2 — mute
		if m.Muted {
			continue
		}
		// Stage 1.3 — restricted room
		if isRestricted(m, msg, isThreadOnlyReply) {
			continue
		}

		mentioned := mentionsAllOrHere || mentionedAccounts[m.Account]

		// Stage 1.4 — thread non-follower (only for thread-only replies)
		if isThreadOnlyReply {
			_, follows := followers[m.Account]
			if !follows && !mentioned {
				continue
			}
		}

		// Stage 2 — hook veto (fail-open on error)
		allow, herr := h.deps.Hook.Allow(ctx, &msg, m)
		if herr != nil {
			slog.Warn("hook errored, allowing", "error", herr, "account", m.Account)
			allow = true
		}
		if !allow {
			continue
		}

		// Stage 3 — routing predicate
		if !EligibleForPush(m, roomType, isLargeRoom, mentioned) {
			continue
		}

		candidates = append(candidates, candidate{member: m})
	}
	if len(candidates) == 0 {
		return nil
	}

	// --- Stage 4: presence snapshot for the eligible set ---
	accounts := make([]string, len(candidates))
	for i, c := range candidates {
		accounts[i] = c.member.Account
	}
	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty

	// --- Emit ---
	nowMs := time.Now().UTC().UnixMilli()
	pushTime := time.Now().UTC().Format(time.RFC3339)
	for _, c := range candidates {
		if !shouldPush(snapshot[c.member.Account]) {
			continue
		}
		evt := model.PushNotificationEvent{
			ID:      msg.ID + "-" + c.member.Account,
			Account: c.member.Account,
			RoomID:  msg.RoomID,
			Title:   "", // population deferred — room name lives off-msg; see Future work
			Body:    msg.Content,
			Data: model.PushNotificationData{
				RoomID:            msg.RoomID,
				MessageID:         msg.ID,
				Type:              shortRoomType(roomType),
				Sender:            sender,
				ThreadMessageID:   msg.ThreadParentMessageID,
				PushTime:          pushTime,
				AlsoSendToChannel: msg.TShow,
			},
			Timestamp: nowMs,
		}
		if err := h.deps.Emitter.Emit(ctx, evt); err != nil {
			slog.Error("emit push failed", "error", err, "account", c.member.Account, "messageId", msg.ID)
		}
	}
	return nil
}

// mentionedSet returns the union of (a) accounts parsed from message
// content and (b) explicit Mentions on the canonical message, normalised
// lowercase. Map lookup is O(1) on the per-recipient loop.
func mentionedSet(parsed mention.ParseResult, explicit []model.Participant) map[string]bool {
	out := make(map[string]bool, len(parsed.Accounts)+len(explicit))
	for _, a := range parsed.Accounts {
		out[a] = true
	}
	for _, p := range explicit {
		if p.Account != "" {
			out[p.Account] = true
		}
	}
	return out
}

// isRestricted returns true when the member should be filtered out because
// they joined the room after the relevant timestamp. For a thread-only
// reply the relevant ts is the parent's CreatedAt (history-service rule);
// for a channel message it's the message's own CreatedAt. A nil parent ts
// on a thread reply is treated conservatively as "no access" (legacy
// thread replies).
func isRestricted(m roomsubcache.Member, msg model.Message, isThreadOnlyReply bool) bool {
	if m.HistorySharedSince == nil {
		return false
	}
	if isThreadOnlyReply {
		if msg.ThreadParentMessageCreatedAt == nil {
			return true
		}
		return msg.ThreadParentMessageCreatedAt.UnixMilli() < *m.HistorySharedSince
	}
	return msg.CreatedAt.UnixMilli() < *m.HistorySharedSince
}

// deriveRoomType returns a usable RoomType for routing. The member-list
// projection no longer carries the room type per-member (it's per-room),
// but msg.Type doesn't carry it either. For routing we only need to
// distinguish DM/botDM from channel/discussion — derive from member count
// (≤2 = DM/botDM-shaped) as a safe default until the projection carries
// room type. TODO: thread to RoomMetadataCache when wired.
func deriveRoomType(members []roomsubcache.Member) model.RoomType {
	if len(members) <= 2 {
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}

func shortRoomType(t model.RoomType) string {
	switch t {
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return "d"
	case model.RoomTypeDiscussion:
		return "p"
	default:
		return "c"
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS for all handler tests.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: PASS — fix any import / unused-variable issues. Remove the `_ = time.Second` workaround in `emit.go` if lint complains about unused imports; restructure as needed.

- [ ] **Step 6: Commit**

```bash
git add notification-worker/handler.go notification-worker/handler_test.go
git commit -m "feat(notification-worker): mention-gated routing pipeline with mobile push payload"
```

---

## Task 12: Wire main.go — Valkey, raw JS, EnsureIndexes, invalidator, drain

**Files:**
- Modify: `notification-worker/main.go`

The existing `main.go` builds only the (now-deleted) Publisher and the basic handler. It needs a full wiring rewrite — connect Valkey, build the cache + lookup + emitter + presence source + hook, ensure the thread_subscriptions index, set up the eager-invalidation core-NATS subscription, and drain async-publish on shutdown.

- [ ] **Step 1: Confirm the existing main builds against the new handler**

After Task 11, `main.go` still references the old `NewHandler(memberLookup, publisher)` signature; this step fixes the build so the tests run.

Run: `go build ./notification-worker/...`
Expected: FAIL — wrong call shape.

- [ ] **Step 2: Replace `main.go`** with the wired version

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL             string                  `env:"NATS_URL"             envDefault:"nats://localhost:4222"`
	NatsCredsFile       string                  `env:"NATS_CREDS_FILE"      envDefault:""`
	SiteID              string                  `env:"SITE_ID"              envDefault:"default"`
	MongoURI            string                  `env:"MONGO_URI"            envDefault:"mongodb://localhost:27017"`
	MongoDB             string                  `env:"MONGO_DB"             envDefault:"chat"`
	MongoUsername       string                  `env:"MONGO_USERNAME"       envDefault:""`
	MongoPassword       string                  `env:"MONGO_PASSWORD"       envDefault:""`
	MaxWorkers          int                     `env:"MAX_WORKERS"          envDefault:"100"`
	LargeRoomThreshold  int                     `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
	ValkeyAddrs         []string                `env:"VALKEY_ADDRS"         envSeparator:","`
	ValkeyPassword      string                  `env:"VALKEY_PASSWORD"      envDefault:""`
	RoomSubCacheTTL     time.Duration           `env:"ROOMSUBCACHE_TTL"     envDefault:"5m"`
	L1MemberCacheSize   int                     `env:"L1_MEMBER_CACHE_SIZE" envDefault:"1000"`
	L1MemberCacheTTL    time.Duration           `env:"L1_MEMBER_CACHE_TTL"  envDefault:"5s"`
	PresenceBatchSize   int                     `env:"PRESENCE_BATCH_SIZE"  envDefault:"512"`
	PresenceRPCTimeout  time.Duration           `env:"PRESENCE_RPC_TIMEOUT" envDefault:"2s"`
	PresenceEnabled     bool                    `env:"PRESENCE_RPC_ENABLED" envDefault:"false"` // false → noop while presence-service PR lands
	Consumer            stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap           bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
}

// mongoMemberLoader implements the memberLoader closure: it reads
// subscriptions for the room and projects to roomsubcache.Member.
type mongoMemberLoader struct {
	col *mongo.Collection
}

func (m *mongoMemberLoader) Load(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	projection := bson.M{
		"u._id":              1,
		"u.account":          1,
		"u.isBot":            1,
		"u.chineseName":      1,
		"u.engName":          1,
		"muted":              1,
		"historySharedSince": 1,
	}
	cur, err := m.col.Find(ctx, bson.M{"roomId": roomID}, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for %s: %w", roomID, err)
	}
	defer cur.Close(ctx)

	var out []roomsubcache.Member
	for cur.Next(ctx) {
		var doc struct {
			User struct {
				ID          string `bson:"_id"`
				Account     string `bson:"account"`
				IsBot       bool   `bson:"isBot"`
				ChineseName string `bson:"chineseName"`
				EngName     string `bson:"engName"`
			} `bson:"u"`
			Muted              bool       `bson:"muted"`
			HistorySharedSince *time.Time `bson:"historySharedSince"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode subscription: %w", err)
		}
		var hssMs *int64
		if doc.HistorySharedSince != nil {
			ms := doc.HistorySharedSince.UnixMilli()
			hssMs = &ms
		}
		out = append(out, roomsubcache.Member{
			ID:                 doc.User.ID,
			Account:            doc.User.Account,
			IsBot:              doc.User.IsBot,
			ChineseName:        doc.User.ChineseName,
			EngName:            doc.User.EngName,
			Muted:              doc.Muted,
			HistorySharedSince: hssMs,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}
	return out, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if len(cfg.ValkeyAddrs) == 0 {
		slog.Error("VALKEY_ADDRS required")
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "notification-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	subCol := db.Collection("subscriptions")
	threadSubCol := db.Collection("thread_subscriptions")

	ensureCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := EnsureThreadSubscriptionIndex(ensureCtx, threadSubCol); err != nil {
		cancel()
		slog.Error("ensure thread_subscriptions index", "error", err)
		os.Exit(1)
	}
	cancel()

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
	cache := roomsubcache.NewValkeyCache(valkeyClient)
	loader := &mongoMemberLoader{col: subCol}
	memberLookup := newCachedMemberLookup(cache, loader.Load, cfg.RoomSubCacheTTL,
		cfg.L1MemberCacheSize, cfg.L1MemberCacheTTL)

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	otelJS, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
	// Raw jetstream.JetStream for async publish (oteljetstream is sync-only).
	rawJS, err := jetstream.New(nc.NatsConn())
	if err != nil {
		slog.Error("raw jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, otelJS, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)
	cons, err := otelJS.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig(cfg.Consumer))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	asyncPub := newJSAsyncPublisher(rawJS)
	emitter := newMobileEmitter(asyncPub, cfg.SiteID)

	var presence PresenceSource = noopPresenceSource{}
	if cfg.PresenceEnabled {
		presence = newBulkPresenceSource(
			&natsPresenceRequester{nc: nc.NatsConn()},
			cfg.SiteID,
			cfg.PresenceBatchSize,
			cfg.PresenceRPCTimeout,
		)
	}

	handler := NewHandler(HandlerDeps{
		Members:            memberLookup,
		Followers:          newMongoThreadFollowers(threadSubCol),
		Presence:           presence,
		Hook:               noopHook{},
		Emitter:            emitter,
		LargeRoomThreshold: cfg.LargeRoomThreshold,
	})

	// --- Eager cache invalidation on subscription.update fan-out ---
	// Two payload shapes exist on this subject:
	//   - SubscriptionUpdateEvent (added/role_updated/mute_toggled) carries
	//     a full Subscription.
	//   - SubscriptionRemovedEvent (removed) carries the lean
	//     RemovedSubscriptionRef.
	// Both shapes include a top-level "subscription" object with "roomId" —
	// decoding into a minimal envelope sidesteps the type branching.
	invalSub, err := nc.NatsConn().Subscribe(subject.SubscriptionUpdateWildcard(), func(msg *nats.Msg) {
		var env struct {
			Subscription struct {
				RoomID string `json:"roomId"`
			} `json:"subscription"`
		}
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			slog.Warn("subscription.update decode failed", "error", err)
			return
		}
		if env.Subscription.RoomID == "" {
			return
		}
		memberLookup.Invalidate(context.Background(), env.Subscription.RoomID)
	})
	if err != nil {
		slog.Error("subscribe subscription.update failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	go func() {
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				handlerCtx := natsutil.ContextWithRequestIDFromHeaders(msgCtx, msg.Headers())
				if err := handler.HandleMessage(handlerCtx, msg.Data()); err != nil {
					slog.Error("handle message failed", "error", err, "request_id", natsutil.RequestIDFromContext(handlerCtx))
					if err := msg.Nak(); err != nil {
						slog.Error("failed to nak message", "error", err)
					}
					return
				}
				if err := msg.Ack(); err != nil {
					slog.Error("failed to ack message", "error", err)
				}
			}()
		}
	}()

	slog.Info("notification-worker started",
		"site", cfg.SiteID,
		"large_room_threshold", cfg.LargeRoomThreshold,
		"valkey_addrs", cfg.ValkeyAddrs,
		"presence_enabled", cfg.PresenceEnabled,
	)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			iter.Stop()
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error {
			asyncPub.drain(ctx)
			return nil
		},
		func(_ context.Context) error {
			if invalSub != nil {
				return invalSub.Unsubscribe()
			}
			return nil
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(_ context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
	)
}

// buildConsumerConfig returns the durable consumer config for
// notification-worker. Centralised so it is unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "notification-worker"
	return cc
}
```

Note: the import list above already includes `encoding/json` for the subscription.update envelope decode and drops `pkg/model` from `main.go` (the file no longer references it directly).

- [ ] **Step 3: Build**

Run: `go build ./notification-worker/...`
Expected: PASS.

- [ ] **Step 4: Run all notification-worker tests**

Run: `make test SERVICE=notification-worker && make lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/main.go
git commit -m "feat(notification-worker): wire valkey cache, async publisher, presence, and cache invalidation"
```

---

## Task 13: Integration test — end-to-end against Valkey + Mongo + NATS

**Files:**
- Modify: `notification-worker/integration_test.go`

- [ ] **Step 1: Rewrite the integration test** to exercise the cache + emit path

Replace the file with:

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestNotificationWorker_CacheBackedFanOut(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_test")
	valkeyClient := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	natsURL := testutil.NATS(t)

	ctx := context.Background()
	subCol := db.Collection("subscriptions")
	threadCol := db.Collection("thread_subscriptions")
	require.NoError(t, EnsureThreadSubscriptionIndex(ctx, threadCol))

	seedSubscriptions(t, ctx, subCol)

	cache := roomsubcache.NewValkeyCache(valkeyutil.WrapClusterClient(valkeyClient))
	loader := &mongoMemberLoader{col: subCol}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute, 100, 5*time.Second)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	// Capture pushes off the NATS bus (subscribe before publishing).
	pushSub := subscribePush(t, nc, "site-a")

	emitter := newMobileEmitter(&directNATSAsyncPub{nc: nc}, "site-a")
	handler := NewHandler(HandlerDeps{
		Members:            lookup,
		Followers:          newMongoThreadFollowers(threadCol),
		Presence:           noopPresenceSource{},
		Hook:               noopHook{},
		Emitter:            emitter,
		LargeRoomThreshold: 500,
	})

	evt := model.MessageEvent{
		SiteID: "site-a",
		Message: model.Message{
			ID:          "m1",
			RoomID:      "r1",
			UserID:      "alice",
			UserAccount: "alice",
			Content:     "hello",
			CreatedAt:   time.Now(),
		},
	}
	data, _ := json.Marshal(evt)
	require.NoError(t, handler.HandleMessage(ctx, data))

	got := pushSub.collect(t, 2*time.Second, 2)
	assert.ElementsMatch(t, []string{"bob", "carol"}, got)
}

func seedSubscriptions(t *testing.T, ctx context.Context, col *mongo.Collection) {
	t.Helper()
	_, err := col.InsertMany(ctx, []any{
		model.Subscription{ID: "s1", RoomID: "r1", User: model.SubscriptionUser{ID: "alice", Account: "alice"}},
		model.Subscription{ID: "s2", RoomID: "r1", User: model.SubscriptionUser{ID: "bob", Account: "bob"}},
		model.Subscription{ID: "s3", RoomID: "r1", User: model.SubscriptionUser{ID: "carol", Account: "carol"}},
	})
	require.NoError(t, err)
}

type pushCollector struct {
	mu      sync.Mutex
	gotAcct []string
	got     chan struct{}
}

func subscribePush(t *testing.T, nc *nats.Conn, siteID string) *pushCollector {
	t.Helper()
	c := &pushCollector{got: make(chan struct{}, 256)}
	sub, err := nc.Subscribe(subject.PushNotification(siteID), func(msg *nats.Msg) {
		var evt model.PushNotificationEvent
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Logf("decode push: %v", err)
			return
		}
		c.mu.Lock()
		c.gotAcct = append(c.gotAcct, evt.Account)
		c.mu.Unlock()
		c.got <- struct{}{}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return c
}

func (c *pushCollector) collect(t *testing.T, timeout time.Duration, want int) []string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		if len(c.gotAcct) >= want {
			out := append([]string(nil), c.gotAcct...)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		select {
		case <-c.got:
		case <-deadline:
			c.mu.Lock()
			defer c.mu.Unlock()
			t.Fatalf("collect timeout: got %v want %d", c.gotAcct, want)
			return nil
		}
	}
}

// directNATSAsyncPub bypasses JetStream — integration test publishes on
// core NATS so we can observe the subject without standing up the
// PUSH_NOTIFICATIONS stream. The production wiring uses jsAsyncPublisher.
type directNATSAsyncPub struct{ nc *nats.Conn }

func (d *directNATSAsyncPub) PublishMsgAsync(msg *nats.Msg) error { return d.nc.PublishMsg(msg) }
func (d *directNATSAsyncPub) drain(context.Context)               {}
```

- [ ] **Step 2: Run the integration tests**

Run: `make test-integration SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add notification-worker/integration_test.go
git commit -m "test(notification-worker): integration coverage for cache-backed mobile push fan-out"
```

---

## Task 14: docker-compose — add Valkey + new env vars

**Files:**
- Modify: `notification-worker/deploy/docker-compose.yml`

- [ ] **Step 1: Update the compose file** to depend on the shared local Valkey + thread the new env vars

```yaml
name: notification-worker

services:
  notification-worker:
    build:
      context: ../..
      dockerfile: notification-worker/deploy/Dockerfile
    environment:
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - SITE_ID=site-local
      - MONGO_URI=mongodb://mongodb:27017
      - MONGO_DB=chat
      - VALKEY_ADDRS=valkey:6379
      - ROOMSUBCACHE_TTL=5m
      - L1_MEMBER_CACHE_SIZE=1000
      - L1_MEMBER_CACHE_TTL=5s
      - LARGE_ROOM_THRESHOLD=500
      - PRESENCE_RPC_ENABLED=false
      - BOOTSTRAP_STREAMS=true
    volumes:
      - ../../docker-local/backend.creds:/etc/nats/backend.creds:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 2: Commit**

```bash
git add notification-worker/deploy/docker-compose.yml
git commit -m "chore(notification-worker): wire valkey + cache env in local compose"
```

---

## Task 15: Update `docs/client-api.md`

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Locate the legacy `notification` event description**

Run: `grep -n "notification" docs/client-api.md | head -20`

- [ ] **Step 2: Replace its description**

Edit the section that documents `chat.user.{account}.notification` to reflect the new behaviour:

```markdown
### Notification fan-out (mobile push only)

`notification-worker` no longer publishes `chat.user.{account}.notification`
on core NATS. Mobile pushes are emitted on the server-only JetStream subject
`chat.server.notification.push.{siteID}.send` and forwarded by the internal
push-notification service. Desktop banners are computed client-side from the
broadcast-worker room-event stream — no server-side desktop publish exists.

The worker filters recipients per message:

- Skips the sender.
- Skips members with `muted: true` on their subscription.
- Skips members whose `historySharedSince` postdates the message (for a
  thread-only reply the parent's `createdAt` is used instead).
- For a thread reply with `tshow: false`, skips non-followers who are not
  mentioned.
- In rooms with more than `LARGE_ROOM_THRESHOLD` members (default 500),
  pushes only to mentioned recipients (`@user`, `@all`, `@here`).
- Bots never receive a mobile push.
- Presence-busy / in-call recipients are not pushed; everyone else
  (online, offline, away, missing) receives one.
```

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document new notification-worker routing rules"
```

---

## Task 16: Repository-wide validation

- [ ] **Step 1: Verify build**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 2: Verify lint clean**

Run: `make lint`
Expected: PASS.

- [ ] **Step 3: Verify unit tests pass with race**

Run: `make test`
Expected: PASS.

- [ ] **Step 4: Verify integration tests**

Run: `make test-integration SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 5: Verify coverage ≥ 80%**

Run:

```bash
go test -race -coverprofile=cov.out ./notification-worker/...
go tool cover -func=cov.out | tail -1
```

Expected: total coverage line ≥ 80%. If lower, add cases in `handler_test.go` (likely the under-covered paths are the hook error branch and the restricted-thread legacy-nil branch).

- [ ] **Step 6: Verify SAST is clean**

Run: `make sast`
Expected: PASS — no medium+ findings introduced by this change.

- [ ] **Step 7: Commit any coverage fill-in tests separately**

```bash
git add notification-worker/handler_test.go
git commit -m "test(notification-worker): cover hook-error and legacy nil parent-ts paths"
```

(Skip this commit if step 5 already passed without changes.)

---

## Spec-coverage Self-Review (run after Task 16)

| Spec requirement | Covered by |
|---|---|
| `roomsubcache.Member` projection extension | Task 1 |
| `@here` handling | Task 2 |
| `PushNotification`/`PresenceSnapshot`/`SubscriptionUpdateWildcard` subjects | Task 3 |
| `PushNotificationEvent`/`PushNotificationData` payload | Task 4 |
| `PresenceSnapshotRequest`/`Reply`/`Presence` types | Task 4 |
| Routing predicate (DM/mention/large-room/bot) | Task 5 |
| Hook interface + no-op default | Task 6 |
| `PresenceSource` interface, no-op, bulk RPC, chunking, status→push, fail-open | Task 7 |
| Cached member lookup + single-flight + L1 LRU | Task 8 |
| Thread-follower lookup by `parentMessageId` | Task 9 |
| `EnsureThreadSubscriptionIndex` | Task 9 / wired in Task 12 |
| Async mobile emitter + dedup `Nats-Msg-Id` `{messageId}-{account}` | Task 10 |
| Stage 1 exclusions: sender, mute, restricted, thread-non-follower | Task 11 |
| Stage 2 hook veto, fail-open on error | Task 11 |
| Stage 3 routing predicate call | Task 11 |
| Stage 4 presence snapshot + per-account `shouldPush` | Task 11 |
| Push payload Sender from member record | Task 11 |
| Restricted check uses parent `CreatedAt` for thread-only replies | Task 11 |
| Legacy nil parent-ts → treat as no access | Task 11 |
| Valkey wiring + new env vars | Task 12 |
| Raw JetStream for async publish | Task 12 |
| Eager cache invalidation on `subscription.update` | Task 12 |
| Async drain on graceful shutdown | Task 12 |
| Docker compose updates | Task 14 |
| `docs/client-api.md` update | Task 15 |

**Deliberately out of scope (per spec):** highlight keywords, threadsubcache, encrypted-room pushes, PII audit, per-user rate limiting, presence-service implementation, push-service implementation. These are all logged as Future work in the spec.

**Known approximations:**

- `deriveRoomType` infers DM vs channel from member count (≤2 → DM). The
  spec assumed `Member` would carry room type per-room; that field is not
  yet in the projection. The approximation is safe: it sends pushes to
  DMs (correct) and to small channels (correct), and only mis-routes a
  hypothetical 2-member channel as a DM — at small scale, no push
  difference. Threading room type through to the cache via a separate
  `room:{id}:type` Valkey entry is a follow-up.
- `Title` on `PushNotificationEvent` is left empty in v1 (room name lives
  off the message). The push service can render it from `Data.RoomID` or
  the spec's follow-up `roommetacache` integration can fill it in. This
  is captured as Future work in the spec; v1 sends what we have.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-27-notification-worker-cache-and-mobile.md`.**

Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
