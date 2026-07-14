# Subscription Timestamp Fields Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the coarse `Subscription.UpdatedAt` with per-attribute timestamps (`muteUpdatedAt`, `rolesUpdatedAt`, `nameUpdatedAt`, `restrictUpdatedAt`), exposed on the wire, and rename the existing `visibilityUpdatedAt` guard field to `restrictUpdatedAt`.

**Architecture:** The per-attribute guard timestamps already exist in Mongo (written by room-service/room-worker/inbox-worker). This change promotes four of them onto the `Subscription` model, exposes them over the client wire, surfaces them through user-service's projection, and renames one field. No write-path logic changes.

**Tech Stack:** Go 1.25, MongoDB (`mongo-driver/v2`), `go.uber.org/mock`, testify. Commands via `make`.

**Spec:** `docs/superpowers/specs/2026-06-29-subscription-timestamp-fields-design.md`

**Committing as logical units:** four commits, executed in order. Each compiles and passes unit tests (`make test`) before commit (enforced by the pre-commit hook).

---

### Task 1 (Commit 1): Model — per-attribute timestamp fields

**Files:**
- Modify: `pkg/model/subscription.go` (Subscription struct, ~lines 58-66)
- Test: `pkg/model/model_test.go` — `TestSubscriptionBaseMetadata_RoundTrip` (~line 4065)

The only Go reference to `Subscription.UpdatedAt` outside the model is this one test (all other `.UpdatedAt` uses are `ThreadSubscription`).

- [ ] **Step 1: Update the round-trip test (red).**

In `pkg/model/model_test.go`, `TestSubscriptionBaseMetadata_RoundTrip`: drop the `updatedAt` local + `UpdatedAt` field, add the four new fields, and update the omitempty absence list.

```go
func TestSubscriptionBaseMetadata_RoundTrip(t *testing.T) {
	favoriteUpdatedAt := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	muteUpdatedAt := time.Date(2026, 2, 2, 9, 0, 0, 0, time.UTC)
	rolesUpdatedAt := time.Date(2026, 2, 3, 9, 0, 0, 0, time.UTC)
	nameUpdatedAt := time.Date(2026, 2, 4, 9, 0, 0, 0, time.UTC)
	restrictUpdatedAt := time.Date(2026, 2, 5, 9, 0, 0, 0, time.UTC)
	src := model.Subscription{
		ID:                "s1",
		User:              model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:            "r1",
		SiteID:            "site-a",
		Roles:             []model.Role{model.RoleMember},
		RoomType:          model.RoomTypeChannel,
		JoinedAt:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		HasUnread:         true,
		FavoriteUpdatedAt: &favoriteUpdatedAt,
		MuteUpdatedAt:     &muteUpdatedAt,
		RolesUpdatedAt:    &rolesUpdatedAt,
		NameUpdatedAt:     &nameUpdatedAt,
		RestrictUpdatedAt: &restrictUpdatedAt,
	}
	dst := model.Subscription{}
	roundTrip(t, &src, &dst)

	// hasUnread is always emitted; the nullable metadata is omitted when unset.
	raw := map[string]any{}
	b, err := json.Marshal(&src)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &raw))
	assert.Equal(t, true, raw["hasUnread"])

	zero := map[string]any{}
	zb, err := json.Marshal(&model.Subscription{ID: "z", JoinedAt: time.Now().UTC()})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(zb, &zero))
	for _, k := range []string{"favoriteUpdatedAt", "muteUpdatedAt", "rolesUpdatedAt", "nameUpdatedAt", "restrictUpdatedAt", "updatedAt"} {
		_, present := zero[k]
		assert.False(t, present, "%q must be omitted when unset", k)
	}
}
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `make test SERVICE=pkg/model` (or `go test ./pkg/model/ -run TestSubscriptionBaseMetadata_RoundTrip`)
Expected: FAIL — `unknown field MuteUpdatedAt` (and the other three) — compile error, implementation absent.

- [ ] **Step 3: Update the model.**

In `pkg/model/subscription.go`, remove:

```go
	// Stored as `_updatedAt` in Mongo (matches the canonical subscriptions schema);
	// serialized on the wire as `updatedAt`. Rooms keep the plain `updatedAt` field.
	UpdatedAt *time.Time `json:"updatedAt,omitempty" bson:"_updatedAt,omitempty"`
```

Add, directly after the existing `FavoriteUpdatedAt` field:

```go
	// Per-attribute last-change timestamps, stamped by the write path as
	// order-safety guards and surfaced to clients. Each is nil until the first
	// guarded write. RestrictUpdatedAt was formerly `visibilityUpdatedAt`.
	MuteUpdatedAt     *time.Time `json:"muteUpdatedAt,omitempty"     bson:"muteUpdatedAt,omitempty"`
	RolesUpdatedAt    *time.Time `json:"rolesUpdatedAt,omitempty"    bson:"rolesUpdatedAt,omitempty"`
	NameUpdatedAt     *time.Time `json:"nameUpdatedAt,omitempty"     bson:"nameUpdatedAt,omitempty"`
	RestrictUpdatedAt *time.Time `json:"restrictUpdatedAt,omitempty" bson:"restrictUpdatedAt,omitempty"`
```

Also fix the now-stale comment above `FavoriteUpdatedAt` (it references `UpdatedAt`): keep only the favorite-relevant sentence.

- [ ] **Step 4: Run test to verify it passes.**

Run: `go test ./pkg/model/ -run TestSubscriptionBaseMetadata_RoundTrip -v`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): per-attribute subscription timestamps, drop updatedAt"
```

---

### Task 2 (Commit 2): Rename `visibilityUpdatedAt` → `restrictUpdatedAt`

Field name + Go param/var names only; method name `ApplySubscriptionVisibility` stays. No functional change. Call sites pass the value positionally, so only interface/impl param names + comments + bson key strings change.

**Files:**
- Modify: `room-service/store.go:213-216` (interface doc + param name)
- Modify: `room-service/store_mongo.go:1602,1620,1635` (param + two bson keys)
- Modify: `inbox-worker/handler.go:57-61` (interface doc + param name)
- Modify: `inbox-worker/main.go:340-377` (doc + param + `$exists`/`$lt` guard keys + two `$set` keys)
- Modify tests: `room-service/integration_test.go:2500-2525`, `inbox-worker/integration_test.go:1294-1315`, `inbox-worker/handler_test.go:314-318`
- Regenerate: `room-service/mock_store_test.go`, `inbox-worker/mock_store_test.go` (via `make generate`)

- [ ] **Step 1: Rename in room-service store.**

`room-service/store_mongo.go`: rename the param `visibilityUpdatedAt` → `restrictUpdatedAt` in the `ApplySubscriptionVisibility` signature (line 1602), and change both bson keys `"visibilityUpdatedAt": visibilityUpdatedAt` → `"restrictUpdatedAt": restrictUpdatedAt` (lines 1620, 1635).

`room-service/store.go`: rename the param in the interface signature (line 216) and update the doc comment on line 213 (`Stamps visibilityUpdatedAt` → `Stamps restrictUpdatedAt`).

- [ ] **Step 2: Rename in inbox-worker store.**

`inbox-worker/main.go`: in `ApplySubscriptionVisibility` (line 345) rename param `visibilityUpdatedAt` → `restrictUpdatedAt`; in the filter change both guard keys `"visibilityUpdatedAt"` (lines 349, 350) → `"restrictUpdatedAt"`; change both `$set` keys (lines 358, 372) → `"restrictUpdatedAt": restrictUpdatedAt`; update the doc comment (lines 340-341).

`inbox-worker/handler.go`: rename the interface param (line 61) and update the comment (line 57).

- [ ] **Step 3: Update tests' seed/assert keys.**

- `inbox-worker/integration_test.go:1294,1310,1313,1315`: `"visibilityUpdatedAt"` → `"restrictUpdatedAt"` in seed maps + comment.
- `inbox-worker/handler_test.go:314-318`: the stub method param name `visibilityUpdatedAt` → `restrictUpdatedAt` (cosmetic; keep the `updatedAt:` struct-field assignment as-is).
- `room-service/integration_test.go:2500,2520,2525`: comment + the two `subTimeField(t, db, "r1", "alice", "visibilityUpdatedAt")` → `"restrictUpdatedAt"`.

- [ ] **Step 4: Regenerate mocks.**

Run: `make generate SERVICE=room-service && make generate SERVICE=inbox-worker`
Expected: `room-service/mock_store_test.go` and `inbox-worker/mock_store_test.go` now use `restrictUpdatedAt` param names. Confirm with `git diff --stat`.

- [ ] **Step 5: Build + unit tests.**

Run: `make test SERVICE=room-service && make test SERVICE=inbox-worker`
Expected: PASS. (Integration tests are Docker-gated and not run here, but must compile — verify with `go vet -tags integration ./room-service/... ./inbox-worker/...` if Docker is unavailable.)

- [ ] **Step 6: Commit.**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/integration_test.go \
        inbox-worker/main.go inbox-worker/handler.go inbox-worker/mock_store_test.go inbox-worker/integration_test.go inbox-worker/handler_test.go
git commit -m "refactor: rename subscription guard field visibilityUpdatedAt to restrictUpdatedAt"
```

---

### Task 3 (Commit 3): user-service projection — surface the new timestamps

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` — `subscriptionProjection` (~lines 164-190)
- Test: `user-service/mongorepo/subscriptions_test.go`

- [ ] **Step 1: Add a projection/decoding test (red).**

In `user-service/mongorepo/subscriptions_test.go`, add a unit test asserting `subscriptionProjection(nil)` includes the four new keys and excludes `_updatedAt`. (This is a pure-function test — no Mongo needed.)

```go
func TestSubscriptionProjection_TimestampFields(t *testing.T) {
	proj := subscriptionProjection(nil)
	for _, k := range []string{"favoriteUpdatedAt", "muteUpdatedAt", "rolesUpdatedAt", "nameUpdatedAt", "restrictUpdatedAt"} {
		_, ok := proj[k]
		assert.True(t, ok, "projection must include %q", k)
	}
	_, hasOld := proj["_updatedAt"]
	assert.False(t, hasOld, "projection must not include dead _updatedAt")
}
```

If `subscriptionProjection` is unexported and the test file is `package mongorepo` (same package), this compiles directly. Confirm the test file's package clause matches.

- [ ] **Step 2: Run test to verify it fails.**

Run: `go test ./user-service/mongorepo/ -run TestSubscriptionProjection_TimestampFields -v`
Expected: FAIL — the four keys absent, `_updatedAt` present.

- [ ] **Step 3: Update the projection.**

In `subscriptionProjection`, replace the `_updatedAt` line with the four new fields (keep `favoriteUpdatedAt`):

```go
		"favoriteUpdatedAt": 1,
		"muteUpdatedAt":     1,
		"rolesUpdatedAt":    1,
		"nameUpdatedAt":     1,
		"restrictUpdatedAt": 1,
```

Remove:

```go
		"_updatedAt":        1, // subscription's Mongo field (wire: updatedAt)
```

- [ ] **Step 4: Run test to verify it passes.**

Run: `go test ./user-service/mongorepo/ -run TestSubscriptionProjection_TimestampFields -v`
Expected: PASS.

- [ ] **Step 5: Full package unit tests.**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_test.go
git commit -m "feat(user-service): project new subscription timestamps, drop dead _updatedAt"
```

---

### Task 4 (Commit 4): Docs — `docs/client-api.md`

**Files:**
- Modify: `docs/client-api.md` (subscription field table ~735, shared-field prose ~4113, subscription.list JSON examples)

- [ ] **Step 1: Update the subscription field table.**

Around line 735, remove the `updatedAt` row. After the existing `favoriteUpdatedAt` row (~734), add:

```markdown
| `muteUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's mute state changed. |
| `rolesUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's roles changed. |
| `nameUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's room name changed. |
| `restrictUpdatedAt` | RFC3339 timestamp | Optional. Last time the room's restrict/external-access visibility changed. |
```

- [ ] **Step 2: Update the shared-field prose (~line 4113).**

Remove `updatedAt` from the "Every field except the ones below is identical across the three types (…, `updatedAt`, and the rest of `room`)" enumeration.

- [ ] **Step 3: Update subscription.list JSON examples.**

For each subscription row in the `subscription.list` response examples (~lines 4143, 4172, 4195, 4272, 4345, 4410), remove the `"updatedAt": "..."` line. Verify each is a subscription row (not a nested message). Optionally add a `"muteUpdatedAt"`/`"restrictUpdatedAt"` line to one example row to illustrate the new shape.

- [ ] **Step 4: Verify no stray subscription `updatedAt` remains.**

Run: `grep -n "updatedAt" docs/client-api.md | sed -n '1,40p'`
Expected: remaining `updatedAt` hits are all message/reaction/avatar/thread rows — none inside a subscription field table or subscription.list example.

- [ ] **Step 5: Commit.**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): replace subscription updatedAt with per-attribute timestamps"
```

---

## Final verification

- [ ] `make generate` (idempotent — no further mock diff)
- [ ] `make lint`
- [ ] `make test`
- [ ] `make sast`
- [ ] `git push --force-with-lease -u origin claude/subscription-timestamp-fields-jcro8y` (branch already diverged from origin via earlier rebase)
