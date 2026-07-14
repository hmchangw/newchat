# user-service Review-Round Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply six targeted correctness/efficiency fixes to `user-service` raised in code review, plus a constants catalog/extraction. No new endpoints, no wire-schema changes.

**Architecture:** Three cohesive tasks, each handed to a fresh implementer subagent. Task 1 = service-layer RPC dedup. Task 2 = downstream behavioral safety (mongo window, roomclient success, outbox dest guard). Task 3 = constant extraction. All changes are TDD (Red-Green-Refactor) and keep `make lint`/`make test`/`make test-integration` green.

**Tech Stack:** Go 1.25, MongoDB aggregation (`go.mongodb.org/mongo-driver/v2`), NATS request/reply, `go.uber.org/mock`, `testify`, testcontainers (`pkg/testutil`).

---

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `user-service/service/subscriptions.go` | enrich + countUnread dedup; GetDM literal extraction | 1, 3 |
| `user-service/service/enrich_test.go` | enrich dedup unit test | 1 |
| `user-service/service/subscriptions_test.go` | countUnread dedup unit test | 1 |
| `user-service/mongorepo/subscriptions.go` | getCurrent window removal; `^Del-` literal extraction | 2, 3 |
| `user-service/mongorepo/subscriptions_test.go` | current-ignores-window integration test | 2 |
| `user-service/roomclient/client.go` | CreateDMRoom `Success` validation | 2 |
| `user-service/roomclient/client_integration_test.go` | `!Success` → error integration test | 2 |
| `user-service/service/status.go` | publishStatus empty-dest guard | 2 |
| `user-service/service/status_test.go` | empty-dest skip unit test | 2 |
| `docs/client-api.md` | document `current` ignores `updatedWithinDays` | 2 |

---

## Task 1: Dedup roomIDs in enrichment & unread count

**Files:**
- Modify: `user-service/service/subscriptions.go` (`enrichWithRoomInfo` ~line 77-80; `countUnread` ~line 267-270)
- Test: `user-service/service/enrich_test.go`, `user-service/service/subscriptions_test.go`

Both paths build a per-site `roomIDs` slice by appending `subs[j].RoomID` for every sub. A room appearing in N subscriptions on the same site is sent N times in one `GetRoomsInfo` request. Dedup the slice (stable order) before the RPC. The result is keyed by roomID, so applying it back to every matching sub is unaffected — only the request payload shrinks. Unread counting still iterates **all** subs.

- [ ] **Step 1: Write the failing enrich dedup test**

Add to `user-service/service/enrich_test.go`:

```go
func TestEnrichWithRoomInfo_DedupsRoomIDs(t *testing.T) {
	svc, _, rooms, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs := []model.Subscription{
		{ID: "a", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ID: "b", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, // same room, second sub
	}
	// EXPECT exactly ["r1"], not ["r1","r1"] — gomock fails the call on arg mismatch.
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, Name: "Eng", LastMsgAt: &newer}}, nil)
	svc.enrichWithRoomInfo(ctx("alice", "site-a"), subs)
	assert.Equal(t, "Eng", subs[0].Name)
	assert.Equal(t, "Eng", subs[1].Name) // both subs enriched from the single deduped RPC
}
```

- [ ] **Step 2: Write the failing countUnread dedup test**

Add to `user-service/service/subscriptions_test.go` (import `time` if not present):

```go
func TestCountUnread_DedupsRoomIDs(t *testing.T) {
	svc, store, rooms, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	store.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 2).Return([]model.Subscription{
		{ID: "a", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ID: "b", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, // same room
	}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, LastMsgAt: &newer}}, nil)
	resp, err := svc.countUnread(ctx("alice", "site-a"), "alice", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, resp.Count) // both subs counted unread; RPC roomIDs deduped to ["r1"]
}
```

- [ ] **Step 3: Run both tests to verify they FAIL**

Run: `make test SERVICE=user-service`
Expected: FAIL — `GetRoomsInfo` called with `["r1","r1"]` does not match the `["r1"]` expectation.

- [ ] **Step 4: Implement dedup in `enrichWithRoomInfo`**

Replace the per-site roomID collection (the loop building `roomIDs` from `idxBySite[site]`) with a deduped version:

```go
			roomIDs := make([]string, 0, len(idxBySite[site]))
			seen := make(map[string]struct{}, len(idxBySite[site]))
			for _, j := range idxBySite[site] {
				rid := subs[j].RoomID
				if _, dup := seen[rid]; dup {
					continue
				}
				seen[rid] = struct{}{}
				roomIDs = append(roomIDs, rid)
			}
```

- [ ] **Step 5: Implement dedup in `countUnread`**

Replace the per-site roomID collection inside the `g.Go` closure (the loop building `roomIDs` from `siteSubs`) with:

```go
			roomIDs := make([]string, 0, len(siteSubs))
			seenRooms := make(map[string]struct{}, len(siteSubs))
			for j := range siteSubs {
				rid := siteSubs[j].RoomID
				if _, dup := seenRooms[rid]; dup {
					continue
				}
				seenRooms[rid] = struct{}{}
				roomIDs = append(roomIDs, rid)
			}
```

The unread-counting loop below it (iterating all `siteSubs` against `lastMsg[...]`) is unchanged.

- [ ] **Step 6: Run tests to verify they PASS**

Run: `make test SERVICE=user-service`
Expected: PASS — all user-service unit tests green, including the two new dedup tests.

- [ ] **Step 7: Commit**

```bash
git add user-service/service/subscriptions.go user-service/service/enrich_test.go user-service/service/subscriptions_test.go
git commit -m "fix(user-service): dedup roomIDs before per-site room-info RPC"
```

---

## Task 2: getCurrent window removal, CreateDMRoom success validation, publishStatus dest guard

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` (`AggregateSubscriptions` ~line 46, `aggregateCurrent` ~line 77-87)
- Modify: `user-service/roomclient/client.go` (`CreateDMRoom` ~line 71-75)
- Modify: `user-service/service/status.go` (`publishStatus` ~line 62-70)
- Test: `user-service/mongorepo/subscriptions_test.go`, `user-service/roomclient/client_integration_test.go`, `user-service/service/status_test.go`
- Doc: `docs/client-api.md:2775`

### 2a — `current` returns everything (no time window) [#10]

- [ ] **Step 1: Invert the existing `withinDays/current` subtest to the new contract**

`TestAggregateSubscriptions_Integration` already seeds `sub-old` (a 100-day-old channel sub, roomId `r-eng`, site-a) and has a subtest asserting `current` with a 30-day window **drops** it. After #10, `current` must **ignore** the window and **keep** `sub-old`. Replace the existing subtest at `subscriptions_test.go:141-148`:

```go
	t.Run("current ignores withinDays — keeps stale rows", func(t *testing.T) {
		within := 30
		subs, err := s.AggregateSubscriptions(ctx, "alice", "current", &within, 100)
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["sub-old"], "current returns the full active set; updatedWithinDays is ignored")
	})
```

The sibling `rooms` subtest at `subscriptions_test.go:132-139` (`withinDays drops stale rows`) stays unchanged — `rooms` still honors the window.

- [ ] **Step 2: Run the integration test to verify it FAILS**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `current` currently applies the window, so `sub-old` is absent (`got["sub-old"]` is false, want true).

- [ ] **Step 3: Remove the window from `aggregateCurrent`**

In `aggregateCurrent`, delete the `if withinDays != nil { match["updatedAt"] = ... }` block and the now-unused `withinDays *int` parameter. Update the signature and its sole caller:

```go
// aggregateCurrent merges the rooms branch (dm/channel, joined to users) and the
// apps branch (botDM, joined to apps) via $facet + $concatArrays. `current`
// intentionally applies NO time window — it returns the user's full active set,
// sorted (favorite desc, name asc). Age-windowing is a `rooms`-listType concern.
func (s *Store) aggregateCurrent(ctx context.Context, account string, limit int) ([]model.Subscription, error) {
	match := bson.M{"u.account": account, "$or": bson.A{
		bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}, "muted": bson.M{"$ne": true}},
		bson.M{"roomType": "botDM", "muted": bson.M{"$ne": true}, "isSubscribed": true},
	}}
	pipeline := bson.A{bson.M{"$match": match}}
	pipeline = append(pipeline, roomsEnrichStages(s.siteID)...)
	// ... rest of the facet/sort/limit pipeline unchanged ...
```

And in `AggregateSubscriptions` change the `current` dispatch:

```go
	if listType == "current" {
		return s.aggregateCurrent(ctx, account, limit)
	}
```

- [ ] **Step 4: Run the integration test to verify it PASSES**

Run: `make test-integration SERVICE=user-service`
Expected: PASS — `current` returns the old sub; `rooms` still excludes it.

- [ ] **Step 5: Update client-api.md**

Replace `docs/client-api.md:2775` with:

```markdown
| `updatedWithinDays` | number  | no       | When set, filters **`rooms`-type** results to subscriptions whose `updatedAt` is within the last N days. **Ignored for `current`** (always returns the full active set) and for `apps`. Must be non-negative; a negative value is rejected with `bad_request`. |
```

- [ ] **Step 6: Commit**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_test.go docs/client-api.md
git commit -m "fix(user-service): subscription.list current ignores time window"
```

### 2b — CreateDMRoom validates Success [#7]

- [ ] **Step 7: Write the failing integration test**

Add a subtest to `TestCreateDMRoom_Integration` in `user-service/roomclient/client_integration_test.go`:

```go
	t.Run("success=false reply — returns error", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(subject.RoomCreateDMSync("site-a"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.SyncCreateDMReply{Success: false}) // explicit not-success, no errcode envelope
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeInternal, e.Code)
	})
```

- [ ] **Step 8: Run it to verify it FAILS**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — current code returns `(reply.Subscription, nil)` for `Success:false`, so `require.Error` fails.

- [ ] **Step 9: Implement the Success check**

In `CreateDMRoom`, after the `SyncCreateDMReply` decode and before `return reply.Subscription, nil`, add:

```go
	if !reply.Success {
		return model.Subscription{}, errcode.Internal("create-dm reported failure")
	}
	return reply.Subscription, nil
```

(`errcode` is already imported in this file.)

- [ ] **Step 10: Run it to verify it PASSES**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add user-service/roomclient/client.go user-service/roomclient/client_integration_test.go
git commit -m "fix(user-service): roomclient errors when CreateDMRoom reply is not success"
```

### 2c — publishStatus skips empty dest [#13]

- [ ] **Step 12: Write the failing unit test**

Add to `user-service/service/status_test.go` (imports `config` and `mocks` are already available via the package; add them if missing):

```go
func TestPublishStatus_SkipsEmptyDest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockUserStore(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "", "site-b"}, MaxSubscriptionLimit: 1000}
	svc := New(store, rooms, pub, cfg)
	// Only "site-b" must receive a publish; self "site-a" and the blank "" are skipped.
	// A single EXPECT means any extra Publish (e.g. to "") fails the test.
	pub.EXPECT().Publish(gomock.Any(), subject.Outbox("site-a", "site-b", model.OutboxUserStatusUpdated), gomock.Any()).Return(nil)
	svc.publishStatus(ctx("alice", "site-a"), "alice", "busy", nil)
}
```

Add imports to `status_test.go` if not present:

```go
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
```

- [ ] **Step 13: Run it to verify it FAILS**

Run: `make test SERVICE=user-service`
Expected: FAIL — current loop publishes to `""` too, producing an unexpected `Publish` call.

- [ ] **Step 14: Implement the empty-dest guard**

In `publishStatus`, change the loop guard:

```go
	for _, dest := range s.allSiteIDs {
		if dest == "" || dest == s.siteID {
			continue
		}
		// ... unchanged publish body ...
	}
```

- [ ] **Step 15: Run it to verify it PASSES**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 16: Commit**

```bash
git add user-service/service/status.go user-service/service/status_test.go
git commit -m "fix(user-service): skip blank dest site in status outbox fan-out"
```

---

## Task 3: Extract magic string literals into named constants [#8]

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` (`^Del-` in `roomsEnrichStages` ~line 28 and `FindChannelsByMembers` ~line 156)
- Modify: `user-service/service/subscriptions.go` (`p_` / `.bot` in `GetDM` ~line 186)

Pure refactor — no behavior change. Existing tests (unit + integration) are the safety net. The constants catalog itself already lives in the spec (`docs/superpowers/specs/2026-06-08-user-service-review-fixes-design.md`).

- [ ] **Step 1: Confirm the baseline is green**

Run: `make test SERVICE=user-service`
Expected: PASS (no new tests; this is a refactor guarded by existing coverage).

- [ ] **Step 2: Extract the deleted-room prefix in mongorepo**

At the top of `user-service/mongorepo/subscriptions.go` (after the imports), add:

```go
// deletedRoomNameRegex matches the name room-service/room-worker assign to a
// soft-deleted room ("Del-" + original name). Local subs whose joined room name
// matches are excluded by the read-path deleted-filter.
const deletedRoomNameRegex = "^Del-"
```

Replace both `"$regex": "^Del-"` occurrences (in `roomsEnrichStages` and `FindChannelsByMembers`) with `"$regex": deletedRoomNameRegex`.

- [ ] **Step 3: Extract the DM-target markers in service**

At the top of `user-service/service/subscriptions.go` (near the existing `maxSiteFanout` const), add:

```go
// DM-target markers rejected by GetDM: platform/system accounts are prefixed
// "p_" and bot accounts end in ".bot" — neither is a valid human DM counterpart.
const (
	dmTargetSystemPrefix = "p_"
	dmTargetBotSuffix    = ".bot"
)
```

In `GetDM`, replace the literal check:

```go
	if strings.HasPrefix(req.AccountName, dmTargetSystemPrefix) || strings.HasSuffix(req.AccountName, dmTargetBotSuffix) {
		return nil, errcode.BadRequest("invalid DM target", errcode.WithReason(errcode.UserInvalidDMTarget))
	}
```

- [ ] **Step 4: Verify lint + tests still pass**

Run: `make lint && make test SERVICE=user-service`
Expected: PASS — no behavior change, constants are drop-in.

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/subscriptions.go user-service/service/subscriptions.go
git commit -m "refactor(user-service): extract magic string literals to named constants"
```

---

## Final Verification (controller-run, after all tasks)

- [ ] `make lint` — 0 issues
- [ ] `make build SERVICE=user-service` — compiles
- [ ] `make test SERVICE=user-service` — unit tests green (race detector on)
- [ ] `make test-integration SERVICE=user-service` — integration tests green (uses `mirror.gcr.io/library/` mirrored images)
- [ ] `make sast` — gosec/govulncheck/semgrep clean
- [ ] `git status` — working tree clean

## Deferred (do NOT implement)

- ~~**#9 30-day retention/window policy**~~ — **resolved in Round 2** (see `## Round 2 — Task 4` below); the deferred retention discussion happened and the design is in the spec's `## Changes → #9`.

---

## Execution record (2026-06-08)

What was actually done this session, end to end.

### Implementation — 3 subagents, TDD (Red→Green→Refactor)
- **Subagent 1 — dedup** (`21e3f1a2`): roomID dedup before the per-site `GetRoomsInfo` RPC in `enrichWithRoomInfo` and `countUnread`; tests assert the deduped RPC arg (`[]string{"r1"}`).
- **Subagent 2 — data/RPC safety**: `aggregateCurrent` drops the time window (#10, integration test inverted to assert `current` keeps a 90-day-old sub); `roomclient.CreateDMRoom` errors on `!reply.Success` (#7); `publishStatus` skips a blank dest (#13). `docs/client-api.md` updated so `updatedWithinDays` is documented as `rooms`-only / ignored for `current`.
- **Subagent 3 — constants** (`fce28a45`): extracted `deletedRoomNameRegex`, `dmTargetSystemPrefix`, `dmTargetBotSuffix`.

Each task was followed by a two-stage review (spec-compliance + code-quality) subagent. All passed.

### Deferred / confirmed-no-change (from triage)
- **#9 30-day retention/window policy** — left intact; flagged for a dedicated discussion.
- **#1, #3, #4, #5, #11, #12** — confirmed already-correct; no change. (#11 reversed to "subscription.updatedAt applied uniformly", which the code already does.)

### Verification
- `make lint` 0 issues · `make build SERVICE=user-service` ok · `make test SERVICE=user-service` (`-race`) green · `make test-integration SERVICE=user-service` green (Docker + `mirror.gcr.io` registry mirror; mongo 8.2.9 + nats 2.11) · `make sast`: **gosec clean**, local `.semgrep/errcode.yml` clean (govulncheck + semgrep-registry network-blocked in sandbox — they run in CI).

### Code review (CodeRabbit, PR #279)
- 0 code findings on the pushed diff (all 10 code files passed). 1 doc nitpick (spec "Out of Scope" wording contradicting Task 2 Step 5) → fixed in `ec9442e4`, thread resolved. Subsequent CodeRabbit run was rate-limited (org out of credits) — no further findings.

### Branch review (`/branch_review`, 6 lenses)
Per-service generalist found **0 critical / 0 high** on the review-round fixes themselves. Aggregate over the whole branch vs `main`: **0 critical / 4 high / 9 medium / 6 low / 11 nitpick**. The 4 high are all pre-existing in the broader user-service feature, not introduced this round.

### Minor issues resolved (post-review-2, this round)
- Added `account` to the degraded-enrichment warn log; added `site` to the publish-failure log (observability correlation).
- Renamed shadowing loop var `in` → `info` in `enrichWithRoomInfo` / `countUnread`.
- Documented the `roomclient` `!reply.Success` branch as a defensive backstop.
- Added unit tests: `StatusGetByName` empty-name → NotFound passthrough; `StatusSet` text-only (nil `IsShow`) path.

### Open items handed back for decision (NOT resolved this round)
These are non-minor or need product/domain input:
1. **[high] `AllowDiskUse` on the `$facet`/`FindChannelsByMembers` aggregations** — scalability hardening.
2. **[high] roomclient `errcode.Parse` `e.Code.Valid()` gate** — dropping it would preserve a remote error carrying an unrecognised code; shared pattern with `GetRoomsInfo` — behavioural, needs a call.
3. **[high] `aggregateCurrent` `$facet` double `$lookup` / `FindChannelsByMembers` uncapped inner `$lookup`** — pipeline-cost reductions.
4. **[medium] `FindChannelsByMembers` inner `$lookup` has no `siteId` filter** — genuinely unsure whether cross-site members should count toward a channel-by-members match (federation semantics). **Needs product input.**
5. **[medium] `enrichWithRoomInfo` fan-out style** — uses WaitGroup+semaphore by design (documented: errgroup would cancel siblings on first error, breaking per-site degradation); left as-is.
6. **[low] `models` package rename** (`models` → `userdto`/`userapi`) — broad churn; left pending a call.
7. **[low] `config` `notEmpty` vs repo-standard `required`** — kept `notEmpty` (stricter/safer for secrets); flagged for consistency preference.
8. **[medium] `countUnread` fetch cap `total` vs `s.maxSubs`** — kept `total` (the prior CodeRabbit fix, keeps the count consistent with the reported total); benign TOCTOU trade-off documented.

### Round-2 review decisions & fixes (applied)

After the post-squash `/branch_review`, the open items were resolved as follows:

**Applied:**
- **[high] `AllowDiskUse` on aggregations** — added an opt-in `mongoutil.WithAllowDiskUse()` (additive `QueryOption`; `Aggregate` now variadic, backward-compatible) and applied it to every `user-service/mongorepo` subscription aggregation (and `SetAllowDiskUse(true)` on the two `Raw().Aggregate` call sites). Prevents a hard 100 MB in-memory-cap error at scale.
- **[high] `countUnread` fetch cap** — `GetActiveSubscriptions(ctx, account, min(total, s.maxSubs))` (`service/subscriptions.go`), bounding a previously-uncapped fetch.
- **[high] Relay unknown-code room RPC errors** — dropped the `e.Code.Valid()` pre-return gate in `roomclient` (`GetRoomsInfo` + `CreateDMRoom`); a remote error envelope carrying a code outside our closed set is now relayed as-is instead of masked. Added integration tests asserting the relay for both RPCs.

**Decided "no change" (documented):**
- **Federation `FindChannelsByMembers` member scope → ANY site.** A member counts if they hold any subscription for the room (local or cross-site mirror). Confirmed intentional; added a code comment on the inner `$lookup` so it isn't re-flagged as a missing `siteId` filter.
- **Convention items left as-is (document-only):** `UserService` struct + handler methods stay exported; `models` package name + the `mongorepo`→`models` import stay; `config` keeps `,notEmpty` (stricter than the repo-standard `,required`). These are noted deviations, not defects — to be reconciled repo-wide later if desired.

Verification after the round-2 fixes: `make lint` 0 · full `make test` (`-race`) green · `make test-integration SERVICE=user-service` green · `pkg/mongoutil` integration green · `gosec` clean.

---

## Round 2 — Task 4: #9 — Room-activity retention window

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.

**Goal:** Make `subscription.list?type=rooms&updatedWithinDays=N` actually filter on whole-room activity (`rooms.lastMsgAt`) instead of a phantom `subscriptions.updatedAt` that no service writes, and remove the dead field + index key.

**Why:** The current `rooms` window matches on `subscriptions.updatedAt`, which is never written (confirmed repo-wide) — so it returns an empty list against real data and only passes CI because fixtures hand-seed the field. See spec `## Changes → #9`.

**Architecture:** Drop the `subscriptions.updatedAt` `$match`; trim the index to `{u.account, roomType}`; add an optional post-`$lookup` window on `room.lastMsgAt` inside `roomsEnrichStages` (cross-site rows kept regardless). Server contract unchanged: `updatedWithinDays == nil ⇒ no filter`; no env var; negative rejected.

**File Structure:**

| File | Responsibility | Sub-task |
|------|----------------|----------|
| `user-service/mongorepo/store.go` | trim `{u.account,roomType,updatedAt}` → `{u.account,roomType}` | 4a |
| `user-service/mongorepo/store_test.go` | index assertion → 2-key | 4a |
| `user-service/mongorepo/subscriptions.go` | drop sub `updatedAt` window; add `room.lastMsgAt` window in `roomsEnrichStages` | 4b |
| `user-service/mongorepo/subscriptions_test.go` | room-activity window integration test | 4b |
| `docs/client-api.md` | reword `updatedWithinDays` (room activity) | 4c |

### Sub-task 4a — Trim the subscriptions index to `{u.account, roomType}`

**Files:** Modify `user-service/mongorepo/store.go`, `user-service/mongorepo/store_test.go`

- [ ] **Step 1: Update the index assertion (Red).** In `store_test.go`, replace the `updatedAt` comment+assertion:

```go
	subKeys := indexKeySpecs(t, s.subscriptions.Raw())
	// {u.account, roomType} serves the account+roomType match on every list/count
	// path. (The retention window keys on room.lastMsgAt, not a subscription field.)
	require.Contains(t, subKeys, "u.account:1,roomType:1")
	require.Contains(t, subKeys, "roomId:1,u.account:1")
	require.Contains(t, subKeys, "name:1,roomType:1")
```

- [ ] **Step 2: Run — expect FAIL.** `make test-integration SERVICE=user-service` (the `TestEnsureIndexes_Integration` assertion fails: the live index is still the 3-key compound, so `u.account:1,roomType:1` isn't present as an exact spec).

- [ ] **Step 3: Trim the index (Green).** In `store.go`, replace the compound index key+comment:

```go
		// {u.account, roomType}: serves the account+roomType match on every
		// list/count path. The retention window keys on room.lastMsgAt (a room
		// doc field), not a subscription field, so no trailing time key is needed.
		{Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomType", Value: 1}}},
```

- [ ] **Step 4: Run — expect PASS.** `make test-integration SERVICE=user-service` (index test green).

- [ ] **Step 5: Commit.**

```bash
git add user-service/mongorepo/store.go user-service/mongorepo/store_test.go
git commit -m "refactor(user-service): trim subscriptions index to {u.account,roomType}

The trailing updatedAt key backed a window on a subscription field nothing
writes. The retention window moves to room.lastMsgAt (4b)."
```

### Sub-task 4b — Re-point the rooms window onto `room.lastMsgAt`

**Files:** Modify `user-service/mongorepo/subscriptions.go`, `user-service/mongorepo/subscriptions_test.go`

- [ ] **Step 1: Rewrite the window test + fixtures (Red).** In `subscriptions_test.go`:
  (a) give the stale room a stale `lastMsgAt`, and make the stale sub's own `updatedAt` recent so the OLD (subscription-field) filter would keep it — proving the new code keys on the room:

```go
		// distinct room for the stale sub-old row (a user can't sub the same room twice).
		// lastMsgAt set 100d ago so the room-activity window excludes it.
		bson.M{"_id": "r-eng-old", "name": "EngOld", "siteId": "site-a", "userCount": 1, "lastMsgAt": old},
```
```go
		// stale-ROOM row: its own updatedAt is recent (proving we key on the room, not the sub)
		bson.M{"_id": "sub-old", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-eng-old",
			"name": "EngOld", "roomType": "channel", "siteId": "site-a", "updatedAt": now},
```
  (b) replace the `withinDays drops stale rows` subtest:

```go
	t.Run("rooms window drops stale-ROOM subs, keeps fresh + cross-site", func(t *testing.T) {
		within := 30
		subs, err := s.AggregateSubscriptions(ctx, "alice", "rooms", &within, 100)
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.False(t, got["sub-old"], "stale room (lastMsgAt 100d ago) excluded by 30-day window")
		assert.True(t, got["sub-eng"], "fresh room (lastMsgAt now) kept")
		assert.True(t, got["sub-xsite"], "cross-site sub kept regardless of window")
	})
```

- [ ] **Step 2: Run — expect FAIL.** `make test-integration SERVICE=user-service` — under current code the window matches `subscriptions.updatedAt` (sub-old's is `now`), so `sub-old` is **kept** and `assert.False(got["sub-old"])` fails.

- [ ] **Step 3: Add the window to `roomsEnrichStages` (Green, part 1).** In `subscriptions.go`, replace the `roomsEnrichStages` function signature+body:

```go
// roomsEnrichStages builds the shared rooms-join + deleted-filter + (optional)
// room-activity window + local enrichment tail. windowCutoff != nil restricts
// LOCAL rows to rooms whose lastMsgAt is within the window ("no message in the
// room since cutoff"; a room with no lastMsgAt is treated as outside the window).
// CROSS-SITE rows (no local room doc) are kept regardless, matching the
// deleted-filter's always-keep-cross-site rule. nil ⇒ no window.
func roomsEnrichStages(localSiteID string, windowCutoff *time.Time) bson.A {
	stages := bson.A{
		bson.M{"$lookup": bson.M{"from": "rooms", "localField": "roomId", "foreignField": "_id", "as": "room"}},
		bson.M{"$unwind": bson.M{"path": "$room", "preserveNullAndEmptyArrays": true}},
		bson.M{"$match": bson.M{"$or": bson.A{
			bson.M{"siteId": bson.M{"$ne": localSiteID}}, // cross-site: keep regardless
			bson.M{"$and": bson.A{ // local: room must exist AND not be Del-prefixed
				bson.M{"room": bson.M{"$ne": nil}},
				bson.M{"room.name": bson.M{"$not": bson.M{"$regex": deletedRoomNameRegex}}},
			}},
		}}},
	}
	if windowCutoff != nil {
		stages = append(stages, bson.M{"$match": bson.M{"$or": bson.A{
			bson.M{"siteId": bson.M{"$ne": localSiteID}},               // cross-site: keep regardless
			bson.M{"room.lastMsgAt": bson.M{"$gte": *windowCutoff}},    // local: room active within N days
		}}})
	}
	return append(stages,
		bson.M{"$addFields": bson.M{
			"userCount": "$room.userCount",
			"lastMsgAt": "$room.lastMsgAt",
			"lastMsgId": "$room.lastMsgId",
		}},
		bson.M{"$project": bson.M{"room": 0}},
	)
}
```

- [ ] **Step 4: Compute the cutoff and drop the dead window (Green, part 2).** Replace `AggregateSubscriptions`'s body down to the `roomsEnrichStages` call:

```go
func (s *Store) AggregateSubscriptions(ctx context.Context, account, listType string, withinDays *int, limit int) ([]model.Subscription, error) {
	if listType == "current" {
		return s.aggregateCurrent(ctx, account, limit)
	}
	match := bson.M{"u.account": account, "muted": bson.M{"$ne": true}}
	var windowCutoff *time.Time
	switch listType {
	case "rooms":
		match["roomType"] = bson.M{"$in": bson.A{"dm", "channel"}}
		if withinDays != nil {
			// Window on whole-room activity (room.lastMsgAt), applied post-$lookup
			// in roomsEnrichStages — NOT on the subscription (it has no updatedAt).
			cutoff := time.Now().UTC().AddDate(0, 0, -*withinDays)
			windowCutoff = &cutoff
		}
	case "apps":
		// withinDays is intentionally not applied to apps subscriptions.
		match["roomType"] = "botDM"
		match["isSubscribed"] = true
	}
	pipeline := bson.A{bson.M{"$match": match}}
	pipeline = append(pipeline, roomsEnrichStages(s.siteID, windowCutoff)...)
	pipeline = append(pipeline,
		bson.M{"$sort": bson.D{{Key: "favorite", Value: -1}, {Key: "name", Value: 1}}},
		bson.M{"$limit": int64(limit)},
	)
	return s.subscriptions.Aggregate(ctx, pipeline, mongoutil.WithAllowDiskUse())
}
```

- [ ] **Step 5: Update the `aggregateCurrent` call site (Green, part 3).** In `aggregateCurrent`, change the `roomsEnrichStages` call to pass `nil`:

```go
	pipeline = append(pipeline, roomsEnrichStages(s.siteID, nil)...)
```

- [ ] **Step 6: Run — expect PASS.** `make test-integration SERVICE=user-service` (window test green; `current ignores withinDays`, deleted-filter, limit, dedup, count tests all still green). Then `make test SERVICE=user-service` (unit) and `make lint`.

- [ ] **Step 7: Commit.**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_test.go
git commit -m "fix(user-service): window subscription.list rooms on room.lastMsgAt

The previous window matched subscriptions.updatedAt, a field no service writes,
so it returned empty against real data. Filter on whole-room activity
(rooms.lastMsgAt) post-lookup; keep cross-site rows regardless. Server contract
unchanged (nil = no filter; negative rejected)."
```

### Sub-task 4c — Reword the client-api doc

**Files:** Modify `docs/client-api.md`

- [ ] **Step 1: Reword the `updatedWithinDays` row.** Replace it with:

```text
| `updatedWithinDays` | number  | no       | When set, filters **`rooms`-type** results to subscriptions **whose room had a message within the last N days** (whole-room activity, `room.lastMsgAt`). **Ignored for `current`** (always returns the full active set) and for `apps`. Cross-site rooms are kept regardless (their activity isn't known locally). Omit for no age filter — the server applies no default; the client supplies any default it wants. Must be non-negative; a negative value is rejected with `bad_request`. |
```

- [ ] **Step 2: Commit.**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): updatedWithinDays now means room activity, not subscription updatedAt"
```

### Round-2 Final Verification (controller-run, after 4a–4c)
- [ ] `make lint` → 0 issues
- [ ] `make test SERVICE=user-service` (race) → pass
- [ ] `make test-integration SERVICE=user-service` → pass
- [ ] `git grep -n 'updatedAt' user-service/mongorepo` → only the room-enrichment `lastMsgAt`/`room.lastMsgAt` references remain; **no `subscriptions.updatedAt`**
- [ ] `git status` — working tree clean

**Migration note (ops, non-blocking):** trimming the index only affects newly-provisioned DBs; the old `{u.account,roomType,updatedAt}` index persists on existing environments (harmless — the 2-key prefix is served by it). Reclaim with a one-off `dropIndex` if desired.

---

# Round 3 — PR #279 Human-Review Comment Resolutions

> Spec: `docs/superpowers/specs/2026-06-08-user-service-review-fixes-design.md` → "Round 3". Execute with superpowers:subagent-driven-development. Wire contract unchanged except apps.list pagination (client-api.md updated in Task 3).

### Task R3-1: Store split + method renames

**Files:**
- Delete: `user-service/mongorepo/store.go`
- Modify: `user-service/mongorepo/subscriptions.go` (becomes SubscriptionRepo: struct, NewSubscriptionRepo, collection consts, pipeline helpers, subscription EnsureIndexes, + GetAppSubscription/SetAppSubscribed moved in)
- Modify: `user-service/mongorepo/users.go` (UserRepo + NewUserRepo + users EnsureIndexes)
- Modify: `user-service/mongorepo/apps.go` (AppRepo + NewAppRepo)
- Modify: `user-service/service/service.go` (3 interfaces, struct fields subs/users/apps, New signature, mockgen directive)
- Modify: `user-service/service/status.go` (renames: GetStatusByName, SetStatus; s.store→s.users)
- Modify: `user-service/service/apps.go` (rename AppsList→ListApps; s.store→s.apps/s.subs)
- Modify: `user-service/service/subscriptions.go` (s.store→s.subs)
- Modify: `user-service/service/mocks/mock_repository.go` (3 mocks — `make generate SERVICE=user-service`, hand-maintain if mockgen broken)
- Modify: `user-service/main.go` (wire 3 repos, 2 EnsureIndexes calls, compile-time checks)
- Modify: `user-service/mongorepo/setup_test.go` (newTestStore → per-repo constructors; seed via testutil db handle)
- Modify: all `user-service/**/*_test.go` referencing Store/UserStore/mock/renamed methods

- [ ] Steps: adapt tests first (constructors/mocks/renames) → run `make test SERVICE=user-service` (RED: compile fails) → implement split + renames → GREEN → `go vet -tags integration ./user-service/...` → commit `refactor(user-service): split mongo store per collection; {Action}{Resource} method names`

### Task R3-2: natsrouter empty-payload tolerance

**Files:** Modify `pkg/natsrouter/register.go`; Test `pkg/natsrouter/*_test.go`

- [ ] RED: test Register with `nil`/empty payload → handler receives zero-value req (currently bad_request)
- [ ] GREEN: `if len(c.Msg.Data) > 0 { unmarshal }`
- [ ] Commit `feat(natsrouter): treat empty request payload as zero-value request`

### Task R3-3: apps.list pagination

**Files:** Modify `user-service/models/app.go` (AppsListRequest), `user-service/mongorepo/apps.go` ($facet pagination), `user-service/service/apps.go` (Register w/ body, NewOffsetPageRequest), `user-service/service/service.go` (registration + AppRepository signature), mocks, `docs/client-api.md` §3.4; Tests: `mongorepo/apps_test.go`, `service/apps_test.go`

- [ ] RED: integration tests (default page, offset paging, beyond-catalog empty page + total, injection guard retained); unit test request plumbing
- [ ] GREEN: $facet implementation + service wiring
- [ ] docs/client-api.md apps.list request/response/example updated
- [ ] Commit `feat(user-service): offset pagination for apps.list`

### Task R3-4: comment cleanups + doc deletion (controller-run)

- [ ] `/remove_comments` on touched user-service files; shorten `room-service/store_mongo.go` 85/86 block to ≤2 lines
- [ ] `git rm docs/user-service-endpoint-consolidation.md`
- [ ] Commit `chore: comment cleanup per review; drop consolidation working note`

### Round-3 Final Verification
- [ ] `make lint` → 0 issues; `make test` → pass; `make test-integration SERVICE=user-service` → pass; coverage ≥80% on touched packages
- [ ] Reply to + resolve all 14 PR comments (6 explanatory, 8 fixed-in-commit)
- [ ] `/branch_review`
