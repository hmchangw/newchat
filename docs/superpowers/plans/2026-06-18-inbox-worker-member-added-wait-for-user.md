# inbox-worker `member_added` wait-for-user Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `inbox-worker`'s `handleMemberAdded` return an error (→ Nak/redeliver) instead of silently skipping a subscription whose referenced user isn't present yet, so a `member_added` for a not-yet-synced user waits rather than being lost.

**Architecture:** This is §7 of the collections-migration design (`docs/superpowers/specs/2026-06-16-oplog-transformer-collections-design.md`) — the single, live-safe `inbox-worker` change the collections apply-path depends on. It is correct steady-state cross-site behavior (a `member_added` for an unknown user should wait, not drop), so it is retained after the source is sunset. The consumer already bounds retries via `MaxDeliver` (default 5, env `MAX_DELIVER`) and Naks non-permanent errors (`inbox-worker/main.go:478-493`), so no consumer-config change is needed.

**Tech Stack:** Go 1.25, `go.uber.org/mock`-free in-memory stub store (`stubInboxStore` in `inbox-worker/handler_test.go`), `stretchr/testify`. Tests run via `make test SERVICE=inbox-worker` (race detector on).

---

## File Structure

- `inbox-worker/handler.go` — `handleMemberAdded` (the loop at lines ~137-171). One responsibility change: collect accounts with no resolved user and, after creating the resolvable ones, return an error naming the missing accounts.
- `inbox-worker/handler_test.go` — add unit tests for the unknown-user and mixed (some-present, some-missing) cases. The existing `TestHandleEvent_MemberAdded` already covers the all-present happy path and stays green as a regression guard.

No new files, no interface changes, no new dependencies.

---

## Context the engineer needs

Current `handleMemberAdded` (in `inbox-worker/handler.go`) resolves users with `FindUsersByAccounts`, builds a `userMap`, then loops over `event.Accounts`. Today the loop **skips** an account with no resolved user:

```go
subs := make([]*model.Subscription, 0, len(event.Accounts))
for _, account := range event.Accounts {
	user, ok := userMap[account]
	if !ok {
		slog.Warn("user not found for account", "account", account)
		continue
	}
	sub := &model.Subscription{
		ID:                 idgen.GenerateUUIDv7(),
		User:               model.SubscriptionUser{ID: user.ID, Account: user.Account},
		RoomID:             event.RoomID,
		RoomType:           roomType,
		SiteID:             event.SiteID,
		Roles:              rolesForType(roomType),
		Name:               subscriptionName(roomType, event.RoomName, event.RequesterAccount),
		IsSubscribed:       subscriptionIsSubscribed(roomType, &user),
		HistorySharedSince: historySharedSince,
		JoinedAt:           joinedAt,
	}
	subs = append(subs, sub)
}

if len(subs) == 0 {
	return nil
}
if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
	if !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("bulk create subscriptions: %w", err)
	}
}

// No SubscriptionUpdateEvent is published here — room-worker already publishes
// to the user's subject and the NATS supercluster routes it to the user's
// home site.
return nil
```

Disposition (`inbox-worker/main.go:478-493`): a returned **non-permanent** error → `msg.Nak()` (redeliver up to `MaxDeliver`); a permanent error → `Ack` (drop). A plain `fmt.Errorf` is non-permanent, so returning one makes the event redeliver — exactly the wait-for-user behavior we want. `fmt` and `mongo` are already imported in `handler.go`.

`stubInboxStore` (`handler_test.go`): `FindUsersByAccounts` returns only the users present in its `users` slice; `BulkCreateSubscriptions` appends to `subscriptions`; `getSubscriptions()` returns a copy. An empty `users` slice means every account resolves as missing.

---

## Task 1: `member_added` returns an error when a referenced user is unknown

**Files:**
- Modify: `inbox-worker/handler.go` (the `handleMemberAdded` loop + post-loop block, ~lines 137-171)
- Test: `inbox-worker/handler_test.go` (add two tests)

- [ ] **Step 1: Write the failing tests**

Add these two tests to `inbox-worker/handler_test.go` (after `TestHandleEvent_MemberAdded`, around line 400):

```go
func TestHandleEvent_MemberAdded_UnknownUser_ReturnsError(t *testing.T) {
	// Store has NO users, so the referenced account cannot resolve.
	store := &stubInboxStore{}
	h := NewHandler(store)

	change := model.MemberAddEvent{
		Type:     "member_added",
		RoomID:   "room-1",
		Accounts: []string{"ghost"},
		SiteID:   "site-b",
		JoinedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
	}
	changeData, err := json.Marshal(change)
	require.NoError(t, err)

	evt := model.OutboxEvent{Type: "member_added", SiteID: "site-b", DestSiteID: "site-a", Payload: changeData}
	evtData, err := json.Marshal(evt)
	require.NoError(t, err)

	err = h.HandleEvent(context.Background(), evtData)

	// Returns an error (→ Nak/redeliver) naming the missing account.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
	// Not classified as permanent — a permanent error would Ack-drop instead of redeliver.
	_, isPermanent := errcode.IsPermanent(err)
	assert.False(t, isPermanent, "missing-user error must be transient so the event redelivers")
	// No subscription was created.
	assert.Empty(t, store.getSubscriptions())
}

func TestHandleEvent_MemberAdded_PartialUsers_CreatesPresentAndErrors(t *testing.T) {
	// "bob" resolves; "ghost" does not.
	store := &stubInboxStore{users: []model.User{{ID: "uid-bob", Account: "bob", SiteID: "site-a"}}}
	h := NewHandler(store)

	change := model.MemberAddEvent{
		Type:     "member_added",
		RoomID:   "room-1",
		Accounts: []string{"bob", "ghost"},
		SiteID:   "site-b",
		JoinedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
	}
	changeData, err := json.Marshal(change)
	require.NoError(t, err)

	evt := model.OutboxEvent{Type: "member_added", SiteID: "site-b", DestSiteID: "site-a", Payload: changeData}
	evtData, err := json.Marshal(evt)
	require.NoError(t, err)

	err = h.HandleEvent(context.Background(), evtData)

	// Errors so the whole event redelivers...
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
	// ...but the resolvable subscription was still created (progress; redelivery re-upserts idempotently).
	subs := store.getSubscriptions()
	require.Len(t, subs, 1)
	assert.Equal(t, "bob", subs[0].User.Account)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — `TestHandleEvent_MemberAdded_UnknownUser_ReturnsError` fails because `HandleEvent` returns `nil` (today's skip), and `TestHandleEvent_MemberAdded_PartialUsers_CreatesPresentAndErrors` fails for the same reason (no error returned).

- [ ] **Step 3: Make the change in `handleMemberAdded`**

In `inbox-worker/handler.go`, replace the loop + post-loop block (the code quoted in "Context", ~lines 137-171) with:

```go
	subs := make([]*model.Subscription, 0, len(event.Accounts))
	var missing []string
	for _, account := range event.Accounts {
		user, ok := userMap[account]
		if !ok {
			missing = append(missing, account)
			continue
		}
		sub := &model.Subscription{
			ID:                 idgen.GenerateUUIDv7(),
			User:               model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:             event.RoomID,
			RoomType:           roomType,
			SiteID:             event.SiteID,
			Roles:              rolesForType(roomType),
			Name:               subscriptionName(roomType, event.RoomName, event.RequesterAccount),
			IsSubscribed:       subscriptionIsSubscribed(roomType, &user),
			HistorySharedSince: historySharedSince,
			JoinedAt:           joinedAt,
		}
		subs = append(subs, sub)
	}

	if len(subs) > 0 {
		if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
			if !mongo.IsDuplicateKeyError(err) {
				return fmt.Errorf("bulk create subscriptions: %w", err)
			}
		}
	}

	// A referenced user that isn't present yet is a federation/migration race, not a
	// permanent failure: return a (transient) error so JetStream redelivers the event
	// until the user lands. The resolvable subscriptions above are created first to make
	// progress; redelivery re-upserts them idempotently (guarded by the unique index).
	if len(missing) > 0 {
		return fmt.Errorf("member_added references unknown users %v in room %s", missing, event.RoomID)
	}

	// No SubscriptionUpdateEvent is published here — room-worker already publishes
	// to the user's subject and the NATS supercluster routes it to the user's
	// home site.
	return nil
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=inbox-worker`
Expected: PASS — the two new tests pass, and the existing `TestHandleEvent_MemberAdded*` tests remain green (all-present path unchanged).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: no findings in `inbox-worker`.

- [ ] **Step 6: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/handler_test.go
git commit -m "feat(inbox-worker): member_added waits for unknown user instead of skipping"
```

---

## Task 2: Document the behavior change in `docs/client-api.md` (only if member_added is documented there)

**Files:**
- Check: `docs/client-api.md`

- [ ] **Step 1: Determine whether this is a client-facing change**

Run: `grep -n "member_added" docs/client-api.md`
Expected: `member_added` is a cross-site OUTBOX/INBOX event, **not** a `chat.user.*` client RPC. If grep returns no client-RPC request/response schema for it, **no doc change is required** (per CLAUDE.md, only `chat.user.*` handlers and `auth-service` HTTP routes require `docs/client-api.md` updates). Record "n/a — internal federation event" and skip to done.

- [ ] **Step 2: If (and only if) a client-facing schema references it, update it**

If grep shows a documented client contract whose behavior changed, edit that section to note that a `member_added` referencing an unsynced user is retried (not dropped). Then:

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note member_added retry-on-unknown-user"
```

Otherwise no commit for this task.

---

## Self-Review

- **Spec coverage:** Implements §7's sole `inbox-worker` change (skip→error on unknown user) and confirms the §5 "bounded MaxDeliver" requirement is already met by the existing consumer config (default 5, `MAX_DELIVER`) — noted in Architecture, no task needed. The dropped `MemberAddEvent` extension (§7, post-correction) is correctly absent. ✓
- **Placeholder scan:** No TBD/placeholder steps; every code step shows complete code; every run step has an exact command + expected outcome. ✓
- **Type consistency:** Uses `model.MemberAddEvent`, `model.OutboxEvent`, `model.SubscriptionUser`, `errcode.IsPermanent`, `stubInboxStore.getSubscriptions()`, `NewHandler` — all matching existing signatures in `inbox-worker/handler.go` and `handler_test.go`. The `missing []string` variable and the `fmt.Errorf` message are self-contained. ✓

---

## Notes for the broader effort (not part of this plan)

This is plan 1 of the collections-migration sequence. Remaining plans (to be written next):

1. **(this plan) inbox-worker member_added wait-for-user** — §7.
2. **oplog-connector checkpoint start-mode** — add a start mode that begins the change stream at a supplied resume token/checkpoint (today: `now`/`time` only); §0/§4.0 (N1).
3. **`pkg/migration` shared extraction** — lift `sourceLookup`, disposition (`errPoison`/`errSkipped` + the processOne loop), base metrics, consume loop, and shared config out of `oplog-transformer` into `pkg/migration`; refactor `oplog-transformer` to import it, keeping its tests green.
4. **`oplog-collections-transformer`** — the new consumer (router + rooms/subscriptions/threadsubs/users mappers + inbox publisher + target-Mongo store + classify), built on `pkg/migration`. Likely split per collection.

Correctness hardening flagged in the design review (C1 non-monotonic `member_removed`; C3 strict `siteId` parse) should be scheduled as their own plan before the collections-transformer goes live.
