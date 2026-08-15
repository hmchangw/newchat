# Add-Member historySharedSince Inheritance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a member whose own room history is capped (`historySharedSince` non-nil) adds new members with share-all history, the new members inherit the adder's cap instead of getting unrestricted history.

**Architecture:** room-service `addMembers` already loads the requester's subscription; it stamps the effective cap onto the canonical `AddMembersRequest` as a new server-set `historySharedSince` field (epoch ms). room-worker's `historySharedSincePtr` honors that cap on the share-all branch, so subscriptions, `MemberAddEvent` (local, internal-inbox, federated) and remote replica subs all pick it up with no further changes.

**Tech Stack:** Go 1.25, gomock (`go.uber.org/mock`), testify, testcontainers (integration), NATS JetStream.

**Spec:** `docs/superpowers/specs/2026-08-14-addmember-hss-inheritance-design.md`

## Global Constraints

- TDD Red-Green-Refactor for every task — run the failing test before implementing.
- All commands via `make` targets, never raw `go` (`make test SERVICE=<name>`, `make lint`, `make generate`). The Makefile already applies `-race`.
- Error wrapping: `fmt.Errorf("what this fn was doing: %w", err)`; no new client-facing errors are introduced by this plan.
- The new wire field is **server-set**: room-service must overwrite it unconditionally so client input never passes through.
- Never emit `&0` / non-positive `historySharedSince` on events — emit nil instead (existing invariant, `pkg/model/event.go:125`).
- Client-facing handler touched ⇒ `docs/client-api.md` and derived views updated in the same PR.
- Commit after each green task; commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01UTH1LA8hLwcXZBEf2NT1AN`
- Pre-commit hook runs lint+tests; if a commit is rejected, fix and retry (do not `--no-verify` code commits).

---

### Task 1: `pkg/model` — `HistorySharedSince` field on `AddMembersRequest`

**Files:**
- Modify: `pkg/model/member.go:31-40` (struct `AddMembersRequest`)
- Test: `pkg/model/model_test.go` (extend `TestAddMembersRequestJSON`, ~line 1640)

**Interfaces:**
- Consumes: nothing.
- Produces: `model.AddMembersRequest.HistorySharedSince *int64` with tags `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`. Tasks 2 and 3 rely on this exact name/type.

- [ ] **Step 1: Write the failing test**

In `pkg/model/model_test.go`, replace `TestAddMembersRequestJSON` (~line 1640) with:

```go
func TestAddMembersRequestJSON(t *testing.T) {
	hss := int64(1700000000000)
	req := model.AddMembersRequest{
		RoomID:             "r1",
		Users:              []string{"alice", "bob"},
		Orgs:               []string{"engineering"},
		Channels:           []model.ChannelRef{{RoomID: "general", SiteID: "site-a"}},
		History:            model.HistoryConfig{Mode: model.HistoryModeAll},
		HistorySharedSince: &hss,
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"historySharedSince":1700000000000`)
	var dst model.AddMembersRequest
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, req, dst)

	// nil cap must be omitted from the wire, not serialized as null/0.
	req.HistorySharedSince = nil
	data, err = json.Marshal(req)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "historySharedSince")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model` (if the Makefile target doesn't accept a pkg path, run the repo's unit-test target for the model package the way the Makefile supports; check `make help` / Makefile first).
Expected: FAIL — `unknown field HistorySharedSince in struct literal`.

- [ ] **Step 3: Write minimal implementation**

In `pkg/model/member.go`, add one field to `AddMembersRequest` after `History`:

```go
// AddMembersRequest is the event published by room-service when a user requests to add members to a room.
type AddMembersRequest struct {
	RoomID   string        `json:"roomId"           bson:"roomId"`
	Users    []string      `json:"users"            bson:"users"`
	Orgs     []string      `json:"orgs"             bson:"orgs"`
	Channels []ChannelRef  `json:"channels"         bson:"channels"`
	History  HistoryConfig `json:"history"          bson:"history"`
	// HistorySharedSince is the inherited history cap (epoch ms UTC), server-set
	// by room-service: on a share-all add it carries the requester's own
	// historySharedSince so new members can never see more history than their
	// adder. nil = no cap. Client-supplied values are always overwritten.
	HistorySharedSince *int64 `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"`
	RequesterID        string `json:"requesterId"      bson:"requesterId"`
	RequesterAccount   string `json:"requesterAccount" bson:"requesterAccount"`
	Timestamp          int64  `json:"timestamp"        bson:"timestamp"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: same command as Step 2. Expected: PASS (whole model package green).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/member.go pkg/model/model_test.go
git commit -m "feat(model): server-set historySharedSince cap on AddMembersRequest"
```

---

### Task 2: room-worker — `historySharedSincePtr` honors the inherited cap

**Files:**
- Modify: `room-worker/handler.go:136-145` (func `historySharedSincePtr`) and its two call sites `room-worker/handler.go:973` and `room-worker/handler.go:1154`
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.AddMembersRequest.HistorySharedSince *int64` (Task 1).
- Produces: `historySharedSincePtr(history model.HistoryConfig, inherited *int64, timestamp int64, roomID string) *int64`. Both call sites pass `req.HistorySharedSince` as `inherited`.

- [ ] **Step 1: Write the failing tests**

Add to `room-worker/handler_test.go` (near `TestProcessAddMembers_HistoryNone_NoTimestamp`, ~line 2452):

```go
// historySharedSincePtr: the share-all branch must honor an inherited cap
// (requester's own HSS, stamped by room-service); mode none keeps flooring at
// the accept timestamp and ignores the cap (now >= any cap).
func TestHistorySharedSincePtr_InheritedCap(t *testing.T) {
	ts := int64(1740000000000)
	capMs := int64(1700000000000)
	nonPositive := int64(0)
	cases := []struct {
		name      string
		history   model.HistoryConfig
		inherited *int64
		timestamp int64
		want      *int64
	}{
		{"mode none uses timestamp", model.HistoryConfig{Mode: model.HistoryModeNone}, nil, ts, &ts},
		{"mode none ignores inherited cap", model.HistoryConfig{Mode: model.HistoryModeNone}, &capMs, ts, &ts},
		{"mode none missing timestamp emits nil", model.HistoryConfig{Mode: model.HistoryModeNone}, nil, 0, nil},
		{"mode all without cap emits nil", model.HistoryConfig{Mode: model.HistoryModeAll}, nil, ts, nil},
		{"mode all inherits cap", model.HistoryConfig{Mode: model.HistoryModeAll}, &capMs, ts, &capMs},
		{"empty mode inherits cap", model.HistoryConfig{}, &capMs, ts, &capMs},
		{"non-positive cap emits nil", model.HistoryConfig{Mode: model.HistoryModeAll}, &nonPositive, ts, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := historySharedSincePtr(tc.history, tc.inherited, tc.timestamp, "r1")
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, *tc.want, *got)
		})
	}
}

// End-to-end through processAddMembers: an inherited cap on a share-all add
// must land on the created subscription and on the MemberAddEvent.
func TestProcessAddMembers_InheritedCapPropagates(t *testing.T) {
	h, mockStore, published := newAddMembersTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0004")

	const inheritedMs = int64(1700000000000)

	mockStore.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u_bob", Account: "bob", SiteID: "site-A", EngName: "X", ChineseName: "X"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	mockStore.EXPECT().HasAnyRoomMembers(gomock.Any(), "r1").Return(false, nil)
	expectGetRoom(mockStore, "r1", "eng")
	mockStore.EXPECT().ApplyMemberCountDelta(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)

	hss := inheritedMs
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice",
		Timestamp:          1740000000000,
		History:            model.HistoryConfig{Mode: model.HistoryModeAll},
		HistorySharedSince: &hss,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	require.Len(t, capturedSubs, 1)
	require.NotNil(t, capturedSubs[0].HistorySharedSince,
		"share-all add by a capped requester must cap the new subscription")
	assert.Equal(t, time.UnixMilli(inheritedMs).UTC(), *capturedSubs[0].HistorySharedSince)

	evt, _ := findMemberAddEvent(t, published(), "r1")
	require.NotNil(t, evt.HistorySharedSince, "MemberAddEvent must carry the inherited cap")
	assert.Equal(t, inheritedMs, *evt.HistorySharedSince)
}
```

Note: `newAddMembersTestHandler` returns the published-message accessor as its third value — call it as `published()` when asserting (see `room-worker/handler_test.go:2337`). `findMemberAddEvent` is an existing helper; check its exact signature (~line 1131 usage) — if it takes `[]publishedMsg`, pass `published()`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker`
Expected: compile FAIL — `too many arguments in call to historySharedSincePtr` (test file references the 4-arg signature) and `unknown field HistorySharedSince` is already resolved by Task 1.

- [ ] **Step 3: Write minimal implementation**

Replace `historySharedSincePtr` in `room-worker/handler.go:136`:

```go
// historySharedSincePtr resolves the HSS for members added by this request.
// mode "none" floors new members at the accept timestamp. Any other mode is
// share-all, but capped by the inherited value (the requester's own HSS,
// stamped by room-service) so a restricted adder can never grant more history
// than they can see. Non-positive values are never emitted (see
// model.RoomMemberEvent invariant: nil, never &0).
func historySharedSincePtr(history model.HistoryConfig, inherited *int64, timestamp int64, roomID string) *int64 {
	if history.Mode != model.HistoryModeNone {
		if inherited != nil && *inherited > 0 {
			return inherited
		}
		return nil
	}
	if timestamp <= 0 {
		slog.Error("restricted history with missing timestamp, emitting nil", "room_id", roomID, "mode", history.Mode)
		return nil
	}
	return &timestamp
}
```

Update both call sites in `processAddMembers`:
- `room-worker/handler.go:973`: `if ms := historySharedSincePtr(req.History, req.HistorySharedSince, req.Timestamp, req.RoomID); ms != nil {`
- `room-worker/handler.go:1154`: `historySharedSince := historySharedSincePtr(req.History, req.HistorySharedSince, req.Timestamp, req.RoomID)`

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker`
Expected: PASS, including all pre-existing HSS tests (`TestProcessAddMembers_HistoryNone_NoTimestamp`, `TestProcessAddMembers_NoHistoryConfig_LeavesNil`, `TestHandler_ProcessAddMembers_RestrictedPropagatesPointer`, `TestHandler_ProcessAddMembers_UnrestrictedOmitsFieldFromWire`).

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): cap share-all history adds at the inherited historySharedSince"
```

---

### Task 3: room-service — stamp the inherited cap in `addMembers`

**Files:**
- Modify: `room-service/handler.go` — `addMembers` (~line 962, the "9. Normalize and publish" block)
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `model.AddMembersRequest.HistorySharedSince` (Task 1); requester subscription `sub` already fetched at `room-service/handler.go:859` (`GetSubscription`), whose `HistorySharedSince` is `*time.Time` (`pkg/model/subscription.go:35`).
- Produces: published canonical payload with `historySharedSince` set per the spec rule. No signature changes.

- [ ] **Step 1: Write the failing test**

Add to `room-service/handler_test.go` (near the other `TestHandler_AddMembers_*` tests, e.g. after `TestHandler_AddMembers_CapacityShortCircuit`, ~line 1052):

```go
// A requester whose own history is capped must not grant more via share-all:
// the published canonical request carries the requester's cap. Mode "none"
// keeps flooring at the accept timestamp (no cap on the wire), and any
// client-supplied historySharedSince is overwritten server-side.
func TestHandler_AddMembers_HistorySharedSinceInheritance(t *testing.T) {
	requesterHSS := time.UnixMilli(1700000000000).UTC()
	clientForged := int64(1)

	cases := []struct {
		name       string
		reqHistory model.HistoryConfig
		reqHSS     *int64
		subHSS     *time.Time
		wantHSS    *int64
	}{
		{"capped requester, mode all → inherit", model.HistoryConfig{Mode: model.HistoryModeAll}, nil, &requesterHSS, ptrInt64(1700000000000)},
		{"capped requester, empty mode → inherit", model.HistoryConfig{}, nil, &requesterHSS, ptrInt64(1700000000000)},
		{"uncapped requester, mode all → nil", model.HistoryConfig{Mode: model.HistoryModeAll}, nil, nil, nil},
		{"capped requester, mode none → nil (worker floors at accept ts)", model.HistoryConfig{Mode: model.HistoryModeNone}, nil, &requesterHSS, nil},
		{"client-forged value overwritten", model.HistoryConfig{Mode: model.HistoryModeAll}, &clientForged, nil, nil},
		{"zero-time requester HSS not inherited", model.HistoryConfig{Mode: model.HistoryModeAll}, nil, &time.Time{}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)

			store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
				User:               model.SubscriptionUser{ID: "u1", Account: "alice"},
				Roles:              []model.Role{model.RoleMember},
				HistorySharedSince: tc.subHSS,
			}, nil)
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
				ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 3,
			}, nil)
			expectAllAccountsExist(store)

			var publishedReq model.AddMembersRequest
			h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
				publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
					require.NoError(t, json.Unmarshal(data, &publishedReq))
					return nil
				},
			}
			req := model.AddMembersRequest{Users: []string{"bob"}, History: tc.reqHistory, HistorySharedSince: tc.reqHSS}

			resp, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
			require.NoError(t, err)
			assert.Equal(t, "accepted", resp.Status)

			if tc.wantHSS == nil {
				assert.Nil(t, publishedReq.HistorySharedSince)
			} else {
				require.NotNil(t, publishedReq.HistorySharedSince)
				assert.Equal(t, *tc.wantHSS, *publishedReq.HistorySharedSince)
			}
		})
	}
}
```

If `ptrInt64` does not exist in the package's test files (`grep -rn "func ptrInt64" room-service/`), add it to the new test's file scope:

```go
func ptrInt64(v int64) *int64 { return &v }
```

Note the zero-time case: `&time.Time{}` cannot be taken inline in a struct literal in the table — if the compiler rejects `&time.Time{}` in the slice literal, hoist `zero := time.Time{}` above `cases` and use `&zero`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL on the two inherit cases — `publishedReq.HistorySharedSince` is nil because the handler never sets it (and the forged case passes only if the handler resets; it currently fails because the forged value passes through).

- [ ] **Step 3: Write minimal implementation**

In `room-service/handler.go` `addMembers`, at the top of the "9. Normalize and publish" block (before `normalized, err := json.Marshal(req)`, alongside the other `req.*` normalization at ~line 964):

```go
	// History-cap inheritance: a requester whose own history is capped must not
	// grant new members more than they can see. On the share-all branch, stamp
	// the requester's cap onto the canonical event (room-worker applies it to
	// every materialized subscription and the member_added events). Mode "none"
	// needs no cap — the worker floors those members at the accept timestamp,
	// which is always ≥ the requester's cap. Reset first: the field is
	// server-set and client input must never pass through.
	req.HistorySharedSince = nil
	if req.History.Mode != model.HistoryModeNone && sub.HistorySharedSince != nil && !sub.HistorySharedSince.IsZero() {
		if ms := sub.HistorySharedSince.UnixMilli(); ms > 0 {
			req.HistorySharedSince = &ms
		}
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-service`
Expected: PASS (all `TestHandler_AddMembers_*` green).

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): inherit requester historySharedSince on share-all member adds"
```

---

### Task 4: room-service — project `historySharedSince` in `GetSubscription`

Without this, the handler from Task 3 always sees a nil `HistorySharedSince` in production (the projection strips it) and the feature silently no-ops. Unit tests pass regardless (mocked store), which is why this task exists separately with an integration test.

**Files:**
- Modify: `room-service/store_mongo.go:317-321` (`subscriptionReadProjection`)
- Test: `room-service/integration_test.go` — `TestMongoStore_GetSubscription_ProjectionFields_Integration` (~line 167)

**Interfaces:**
- Consumes: `model.Subscription.HistorySharedSince *time.Time` (bson key `historySharedSince`, `pkg/model/subscription.go:35`).
- Produces: `GetSubscription` results now populate `HistorySharedSince`; Task 3's handler logic depends on it in production.

- [ ] **Step 1: Write the failing test**

In `room-service/integration_test.go`, extend `TestMongoStore_GetSubscription_ProjectionFields_Integration` (~line 167). Add to the inserted doc:

```go
	hss := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)
```

and in the `mustInsertSub` literal add `HistorySharedSince: &hss,`; then at the end of the assertions add:

```go
	require.NotNil(t, got.HistorySharedSince, "historySharedSince must be in the projection (addMembers inherits the requester's cap from it)")
	assert.WithinDuration(t, hss, *got.HistorySharedSince, time.Second)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=room-service` (requires Docker; testcontainers-backed).
Expected: FAIL — `got.HistorySharedSince` is nil (field not projected).
If Docker is unavailable in the environment, record that in the task report, verify the test at least compiles via the integration build tag (`go vet -tags integration ./room-service/` equivalent through make, or the closest make target), and flag the gap for the final review step — do NOT silently skip the red/green evidence.

- [ ] **Step 3: Write minimal implementation**

In `room-service/store_mongo.go`, extend `subscriptionReadProjection`:

```go
var subscriptionReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "u", Value: 1}, {Key: "roomId", Value: 1},
	{Key: "siteId", Value: 1}, {Key: "roles", Value: 1}, {Key: "alert", Value: 1},
	{Key: "threadUnread", Value: 1}, {Key: "lastSeenAt", Value: 1},
	{Key: "historySharedSince", Value: 1},
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=room-service`
Expected: PASS. Also run `make test SERVICE=room-service` (unit suite must stay green).

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): project historySharedSince in GetSubscription reads"
```

---

### Task 5: docs — client-api.md + derived views

**Files:**
- Modify: `docs/client-api.md:1249` (Add Members request `history.mode` row), `docs/client-api.md:1251` (server-set fields note), `docs/client-api.md:1397` (`member_added` event `historySharedSince` row)
- Modify: `docs/client-api/request-reply.md` and `docs/client-api/events.md` — locate the mirrored rows (`grep -n "history.mode" docs/client-api/request-reply.md`; `grep -n "historySharedSince" docs/client-api/events.md`) and apply the same edits so the derived views don't drift.

**Interfaces:**
- Consumes: behavior defined in Tasks 1–4.
- Produces: documentation only.

- [ ] **Step 1: Edit `docs/client-api.md`**

Line 1249 — replace the `history.mode` row with:

```markdown
| `history.mode` | string | no | `"none"` (default) or `"all"` — controls whether new members see history before they joined. `"all"` is capped by the **requester's own** `historySharedSince`: when the adder's history is restricted, the new members inherit the adder's boundary instead of unrestricted history (members can never see more history than whoever added them). `"none"` restricts new members to messages from the add time onward. |
```

Line 1251 — replace the server-set note with:

```markdown
The fields `requesterId`, `requesterAccount`, `timestamp`, and `historySharedSince` on the Go `AddMembersRequest` are server-set — the client should omit them (any client-supplied `historySharedSince` is overwritten).
```

Line 1397 — replace the `historySharedSince` row of the `member_added` event with:

```markdown
| `historySharedSince` | number | Optional. Epoch ms (UTC); the new members' history boundary — present when their history is restricted (add with `history.mode: "none"`, or a share-all add by a requester whose own history is capped, in which case this is the requester's inherited boundary). Absent = unrestricted. |
```

- [ ] **Step 2: Mirror in derived views**

Apply the same three edits wherever the corresponding rows appear in `docs/client-api/request-reply.md` (Add Members request table + server-set note) and `docs/client-api/events.md` (`member_added` table). Keep wording identical to the canonical file.

- [ ] **Step 3: Verify no drift**

Run: `grep -n "capped by the" docs/client-api.md docs/client-api/request-reply.md` and `grep -n "inherited boundary" docs/client-api.md docs/client-api/events.md` — each edit must appear in both canonical and derived files.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): document historySharedSince inheritance on share-all member adds"
```

---

### Task 6: full gates

**Files:** none (verification only).

- [ ] **Step 1:** `make lint` — expected clean.
- [ ] **Step 2:** `make test` — full unit suite with race detector, expected green.
- [ ] **Step 3:** `make test-integration SERVICE=room-service` (if Docker available) — expected green.
- [ ] **Step 4:** `make sast` — expected no medium+ findings (new code introduces no crypto/network/exec surface; expect clean).
- [ ] **Step 5:** Coverage spot-check on touched packages:
  the repo floor is 80%; the new branches are small and fully table-tested, so
  no gap is expected. If the Makefile lacks a coverage target, note it and rely
  on the per-task test evidence.
- [ ] **Step 6:** No commit — report results.

---

## Amendments (2026-08-14, post-review — PR #277)

The steps above are the executed plan and are kept as written. Code review
(multi-expert + CodeRabbit) amended the design after execution; the shipped
implementation differs from the Step 3 snippet as follows:

1. **Clock-skew guard on `mode: "none"`** (CodeRabbit, security-major): the
   accept timestamp alone could predate the requester's own boundary when
   stamped by a skewed clock, leaking the skew window's history. room-service
   now stamps the requester's cap for **every** mode (the `SharesAll`
   condition was removed from the stamp site), and room-worker's
   `historySharedSincePtr` floors mode-`"none"` members at the **later** of
   the accept timestamp and the inherited cap. Regression tests: room-worker
   "mode none floors at inherited cap under clock skew"; room-service
   "capped requester, mode none → cap forwarded".
2. **Fail-closed malformed-event fallback**: mode `"none"` with a
   missing/non-positive timestamp falls back to the inherited cap when one is
   present (previously nil = unrestricted), nil only when no cap exists.
3. **`history.mode` validation**: room-service rejects unrecognized modes
   with `badRequest` (`history.mode must be "none" or "all"`) instead of
   treating them as share-all; documented in `docs/client-api.md` + views.
4. **Rollout order (required)**: deploy **room-worker before room-service** —
   an old worker ignores the new wire field, so the escalation window closes
   only once the worker is upgraded. See the design spec's
   "Rollout / mixed versions" section.
