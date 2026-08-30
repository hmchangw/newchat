# member.list Bot App-Name Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `RoomMemberEntry.Name` to `AppName` and populate it for bot members on both `member.list` lookup paths, using one batched indexed query per path.

**Architecture:** `ListRoomMembers` picks one of two paths — `getRoomMembers` (the `room_members` collection) or `getRoomSubscriptions` (the fallback). Each path already walks its result rows once; we collect bot accounts in that existing walk and hand them to a shared `attachAppNames` helper that issues a single `apps.assistant.name $in [...]` query. No `$lookup` is added.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), testify, testcontainers-go.

**Spec:** `docs/superpowers/specs/2026-08-30-member-list-bot-app-name-design.md`

## Global Constraints

- Never run raw `go` commands — always `make` targets (`make test SERVICE=room-service`, `make test-integration SERVICE=room-service`, `make lint`, `make fmt`, `make sast`).
- TDD is mandatory: write the test, run it, confirm it FAILS, then implement.
- Integration tests carry the `//go:build integration` tag and live in `package main`.
- No `$lookup` — server-side joins are forbidden by CLAUDE.md.
- Every Mongo read specifies an explicit projection.
- Wrap errors as `fmt.Errorf("short description: %w", err)`; never return bare `err`.
- Client-facing shape changes must update `docs/client-api.md` **and** its derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) in the same PR.
- Comments: short and neat, max ~2 lines, explain WHY not WHAT.

---

### Task 1: Rename `Name` → `AppName` on the model

**Files:**
- Modify: `pkg/model/member.go:69-72`
- Modify: `room-service/store_mongo.go:820` (the single writer — keeps the build green)
- Modify: `room-service/integration_test.go:1150,1152,1196,1225` (existing assertions)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `model.RoomMemberEntry.AppName string` with JSON tag `appName,omitempty` and bson tag `-`. Every later task writes to this field.

- [ ] **Step 1: Write the failing round-trip test**

Append to `pkg/model/model_test.go` (adapt to the file's existing `roundTrip`/subtest style near the other `RoomMemberEntry` cases around line 2072):

```go
func TestRoomMemberEntry_AppNameRoundTrip(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID:      "u-bot",
		Type:    model.RoomMemberIndividual,
		Account: "weather.bot",
		AppName: "Weather App",
	}
	data, err := json.Marshal(&entry)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"appName":"Weather App"`)

	var dst model.RoomMemberEntry
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, entry, dst)
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `make test SERVICE=../pkg/model` — if that target does not resolve, use `make test` and read the `pkg/model` section.
Expected: compile error — `unknown field AppName in struct literal`.

- [ ] **Step 3: Rename the field**

In `pkg/model/member.go`, replace the `Name` field and its comment:

```go
	// AppName is the app's display name for bot members, from apps.name.
	// Humans carry EngName/ChineseName instead; display: appName ?? engName ?? account.
	AppName string `json:"appName,omitempty"     bson:"-"`
```

- [ ] **Step 4: Fix the two existing writers/readers so the tree compiles**

`room-service/store_mongo.go:820`: `members[i].Member.Name = name` → `members[i].Member.AppName = name`.

`room-service/integration_test.go`: rename `.Member.Name` → `.Member.AppName` and `human.Name` → `human.AppName`, `bot.Name` → `bot.AppName` at lines 1150, 1152, 1196, 1225. Update the assertion messages ("no Name on a human member" → "no AppName on a human member").

- [ ] **Step 5: Verify green**

Run: `make test SERVICE=room-service && make lint`
Expected: PASS, no lint findings.

- [ ] **Step 6: Commit**

```bash
git add pkg/model/member.go room-service/store_mongo.go room-service/integration_test.go pkg/model/model_test.go
git commit -m "refactor(model): rename RoomMemberEntry.Name to AppName"
```

---

### Task 2: Extract the shared `attachAppNames` helper

**Files:**
- Modify: `room-service/store_mongo.go:770-824` (`attachUserDisplayNames`)
- Test: `room-service/integration_test.go` (existing bot-enrichment suite must stay green)

**Interfaces:**
- Consumes: `model.RoomMemberEntry.AppName` (Task 1); the existing `(s *MongoStore) findAppsForDisplay(ctx context.Context, botAccounts []string) (map[string]string, error)` at `store_mongo.go:853`, which returns `apps.name` keyed by `assistant.name`.
- Produces: `func (s *MongoStore) attachAppNames(ctx context.Context, roomID string, members []model.RoomMember, botAccounts []string) error` — no-ops on an empty `botAccounts`; both Task 3 and Task 4 call it.

- [ ] **Step 1: Add the helper**

Insert into `room-service/store_mongo.go`, directly above `findUsersForDisplay`:

```go
// attachAppNames resolves bot members' app display names in one indexed batch
// and fills AppName in place. Callers pass botAccounts because the two
// ListRoomMembers paths detect bots differently.
func (s *MongoStore) attachAppNames(ctx context.Context, roomID string, members []model.RoomMember, botAccounts []string) error {
	if len(botAccounts) == 0 {
		return nil
	}
	appByAssistant, err := s.findAppsForDisplay(ctx, botAccounts)
	if err != nil {
		return fmt.Errorf("attach app names for %q: %w", roomID, err)
	}
	for i := range members {
		if name, ok := appByAssistant[members[i].Member.Account]; ok {
			members[i].Member.AppName = name
		}
	}
	return nil
}
```

Note: org members have an empty `Account`, which never keys into `appByAssistant`, so they are skipped without an explicit type check.

- [ ] **Step 2: Rewrite `attachUserDisplayNames` to take pre-partitioned accounts and delegate**

Replace the whole function (currently `store_mongo.go:770-824`) with:

```go
// attachUserDisplayNames batch-loads display fields for the individual members
// in the slice and copies them on in place. Humans resolve from users, bots
// from apps; each query fires only when its partition is non-empty.
func (s *MongoStore) attachUserDisplayNames(ctx context.Context, roomID string, members []model.RoomMember, humanAccounts, botAccounts []string) error {
	if len(humanAccounts) > 0 {
		userByAccount, err := s.findUsersForDisplay(ctx, humanAccounts)
		if err != nil {
			return fmt.Errorf("find users for room %q: %w", roomID, err)
		}
		for i := range members {
			u, ok := userByAccount[members[i].Member.Account]
			if !ok || members[i].Member.Type != model.RoomMemberIndividual {
				continue
			}
			members[i].Member.EngName = u.EngName
			members[i].Member.ChineseName = u.ChineseName
			members[i].Member.SectName = u.SectName
			members[i].Member.EmployeeID = u.EmployeeID
		}
	}
	return s.attachAppNames(ctx, roomID, members, botAccounts)
}
```

- [ ] **Step 3: Update the caller in `getRoomSubscriptions` to partition and pass the slices**

This is a compile-fix only; the real detection change lands in Task 4. In `getRoomSubscriptions`, replace the `attachUserDisplayNames` call block with:

```go
	if enrich && len(members) > 0 {
		var humanAccounts, botAccounts []string
		for i := range members {
			acct := members[i].Member.Account
			if acct == "" || members[i].Member.Type != model.RoomMemberIndividual {
				continue
			}
			if model.IsBot(acct) {
				botAccounts = append(botAccounts, acct)
			} else {
				humanAccounts = append(humanAccounts, acct)
			}
		}
		if err := s.attachUserDisplayNames(ctx, roomID, members, humanAccounts, botAccounts); err != nil {
			return nil, err
		}
	}
```

Note the dropped `fmt.Errorf` wrap at this call site — `attachUserDisplayNames` already wraps with the room ID, and double-wrapping would read `attach user display names for "r": find users for room "r": ...`.

- [ ] **Step 4: Delete the now-dead regexp**

Remove `var botAccountPattern = regexp.MustCompile(botAccountRegex)` (`store_mongo.go:26`) and the `"regexp"` import. Keep the `botAccountRegex` **string const** — `store_mongo.go:1743` embeds it in a `$regexMatch` stage.

- [ ] **Step 5: Verify the existing bot suite still passes (pure refactor, no behaviour change)**

Run: `make test-integration SERVICE=room-service` and `make lint`
Expected: `TestMongoStore_ListRoomMembers_BotEnrichment_Integration` PASSES unchanged; no unused-import or unused-var findings.

- [ ] **Step 6: Commit**

```bash
git add room-service/store_mongo.go
git commit -m "refactor(room-service): extract attachAppNames from attachUserDisplayNames"
```

---

### Task 3: Enrich bots on the `room_members` path

**Files:**
- Modify: `room-service/store_mongo.go:590-615` (the enriched branch of `getRoomMembers`)
- Test: `room-service/integration_test.go`

**Interfaces:**
- Consumes: `attachAppNames` (Task 2).
- Produces: nothing new — `getRoomMembers` keeps its signature.

- [ ] **Step 1: Write the failing test**

Append a new subtest inside `TestMongoStore_ListRoomMembers_BotEnrichment_Integration` (it already defines the `insertSub` / `insertUser` / `insertApp` closures at lines 1079-1092):

```go
	t.Run("room_members path: bot member gets AppName from apps", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"})
		insertApp(t, db, bson.M{
			"_id": "app-weather", "name": "Weather App",
			"assistant": bson.M{"enabled": true, "name": "weather.bot"},
		})

		// A room_members doc exists, so ListRoomMembers takes the aggregation path.
		_, err := db.Collection("room_members").InsertMany(ctx, []any{
			model.RoomMember{ID: "rm-alice", RoomID: "chan-1", Ts: base,
				Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"}},
			model.RoomMember{ID: "rm-bot", RoomID: "chan-1", Ts: base.Add(time.Second),
				Member: model.RoomMemberEntry{ID: "u-bot", Type: model.RoomMemberIndividual, Account: "weather.bot"}},
		})
		require.NoError(t, err)

		got, err := store.ListRoomMembers(ctx, "chan-1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)

		byAccount := make(map[string]model.RoomMemberEntry)
		for _, m := range got {
			byAccount[m.Member.Account] = m.Member
		}
		assert.Equal(t, "Weather App", byAccount["weather.bot"].AppName, "bot on room_members path must get AppName")
		assert.Empty(t, byAccount["weather.bot"].EngName, "bot has no users doc")
		assert.Equal(t, "Alice Wang", byAccount["alice"].EngName, "human enrichment must be unaffected")
		assert.Empty(t, byAccount["alice"].AppName, "human must not get AppName")
	})
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `Error "" does not equal "Weather App"` on the `AppName` assertion (the room_members path does no apps lookup yet).

- [ ] **Step 3: Collect bot accounts in the existing row loop and call the helper**

In `getRoomMembers`'s enriched branch, the loop over `rows` already builds `orgIDs`. Add a sibling `botAccounts` slice:

```go
	members := make([]model.RoomMember, len(rows))
	var orgIDs, botAccounts []string
	for i := range rows {
		rm := rows[i].RoomMember
		d := rows[i].Display
		rm.Member.EngName = d.EngName
		rm.Member.ChineseName = d.ChineseName
		rm.Member.IsOwner = d.IsOwner
		if rm.Member.Type == model.RoomMemberOrg {
			orgIDs = append(orgIDs, rm.Member.ID)
		} else {
			rm.Member.SectName = d.SectName
			rm.Member.EmployeeID = d.EmployeeID
			// room_members rows carry no isBot flag, so the suffix is the only signal.
			if model.IsBot(rm.Member.Account) {
				botAccounts = append(botAccounts, rm.Member.Account)
			}
		}
		members[i] = rm
	}
	if len(orgIDs) > 0 {
		if err := s.attachOrgDisplay(ctx, roomID, members, orgIDs); err != nil {
			return nil, err
		}
	}
	if err := s.attachAppNames(ctx, roomID, members, botAccounts); err != nil {
		return nil, err
	}
	return members, nil
```

- [ ] **Step 4: Run the test and confirm it PASSES**

Run: `make test-integration SERVICE=room-service`
Expected: PASS, all subtests.

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): enrich bot members with appName on the room_members path"
```

---

### Task 4: Detect bots by `u.isBot` on the subscriptions path

**Files:**
- Modify: `room-service/store_mongo.go:713-717` (`roomMemberSubProjection`), and the partition loop from Task 2 Step 3
- Test: `room-service/integration_test.go`

**Interfaces:**
- Consumes: `attachUserDisplayNames(ctx, roomID, members, humanAccounts, botAccounts)` (Task 2).
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Append another subtest to `TestMongoStore_ListRoomMembers_BotEnrichment_Integration`. The account deliberately lacks a `.bot` suffix so only the flag can classify it:

```go
	t.Run("subscriptions path: isBot flag enriches a non-suffix bot account", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)

		insertApp(t, db, bson.M{
			"_id": "app-legacy", "name": "Legacy Assistant",
			"assistant": bson.M{"enabled": true, "name": "legacy-assistant"},
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-legacy",
			User:     model.SubscriptionUser{ID: "u-legacy", Account: "legacy-assistant", IsBot: true},
			RoomID:   "botdm-2",
			RoomType: model.RoomTypeBotDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: base,
		})

		got, err := store.ListRoomMembers(ctx, "botdm-2", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Legacy Assistant", got[0].Member.AppName, "isBot=true must route the account to apps")
		assert.Empty(t, got[0].Member.EngName)
	})
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `AppName` is empty; `legacy-assistant` has no `.bot` suffix, so it is currently sent to the `users` lookup.

- [ ] **Step 3: Project `u.isBot`**

`RoomMember` carries no `IsBot`, so the flag must be read from `subs` before the entries are built. Add the field to the projection at `store_mongo.go:713`:

```go
// The nested path is u._id, not u.id — SubscriptionUser.ID is bson "_id".
var roomMemberSubProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "u._id", Value: 1}, {Key: "u.account", Value: 1},
	{Key: "u.isBot", Value: 1},
	{Key: "roles", Value: 1}, {Key: "joinedAt", Value: 1},
}
```

- [ ] **Step 4: Partition from `subs`, where the flag is readable**

Replace the Task 2 Step 3 block. Build the partitions in the existing `subs` loop rather than a second pass over `members`:

```go
	members := make([]model.RoomMember, 0, len(subs))
	var humanAccounts, botAccounts []string
	for i := range subs {
		sub := &subs[i]
		entry := model.RoomMemberEntry{
			ID:      sub.User.ID,
			Type:    model.RoomMemberIndividual,
			Account: sub.User.Account,
		}
		if enrich {
			entry.IsOwner = hasRole(sub.Roles, model.RoleOwner)
			if acct := sub.User.Account; acct != "" {
				// The flag is the authority; the suffix covers subs written before it existed.
				if sub.User.IsBot || model.IsBot(acct) {
					botAccounts = append(botAccounts, acct)
				} else {
					humanAccounts = append(humanAccounts, acct)
				}
			}
		}
		members = append(members, model.RoomMember{
			ID:     sub.ID,
			RoomID: roomID,
			Ts:     sub.JoinedAt,
			Member: entry,
		})
	}

	if enrich && len(members) > 0 {
		if err := s.attachUserDisplayNames(ctx, roomID, members, humanAccounts, botAccounts); err != nil {
			return nil, err
		}
	}
	return members, nil
```

- [ ] **Step 5: Run the test and confirm it PASSES**

Run: `make test-integration SERVICE=room-service`
Expected: PASS. `TestMongoStore_ListRoomMembers_SubscriptionProjection_Integration` must also stay green — it guards this exact projection.

- [ ] **Step 6: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): detect bot subscriptions via u.isBot for appName enrichment"
```

---

### Task 5: Edge cases — missing app doc, human-only room, dedup

**Files:**
- Test: `room-service/integration_test.go`
- Modify: `room-service/store_mongo.go` (only if a test exposes a defect)

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: nothing.

- [ ] **Step 1: Write the edge-case subtests**

```go
	t.Run("bot with no apps document leaves AppName empty without error", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)

		insertSub(t, db, model.Subscription{
			ID:       "sub-ghost",
			User:     model.SubscriptionUser{ID: "u-ghost", Account: "ghost.bot"},
			RoomID:   "botdm-3",
			RoomType: model.RoomTypeBotDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC),
		})

		got, err := store.ListRoomMembers(ctx, "botdm-3", nil, nil, true)
		require.NoError(t, err, "a bot with no apps doc must not fail the whole listing")
		require.Len(t, got, 1)
		assert.Empty(t, got[0].Member.AppName)
	})

	t.Run("enrich=false skips app enrichment entirely", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)

		insertApp(t, db, bson.M{
			"_id": "app-quiet", "name": "Quiet App",
			"assistant": bson.M{"enabled": true, "name": "quiet.bot"},
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-quiet",
			User:     model.SubscriptionUser{ID: "u-quiet", Account: "quiet.bot"},
			RoomID:   "botdm-4",
			RoomType: model.RoomTypeBotDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC),
		})

		got, err := store.ListRoomMembers(ctx, "botdm-4", nil, nil, false)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Empty(t, got[0].Member.AppName, "lean listing must carry no display fields")
	})
```

- [ ] **Step 2: Run them**

Run: `make test-integration SERVICE=room-service`
Expected: PASS without implementation changes — `attachAppNames` no-ops on an empty match and `enrich=false` never reaches it. If either FAILS, fix the store and re-run before committing.

- [ ] **Step 3: Commit**

```bash
git add room-service/integration_test.go
git commit -m "test(room-service): cover appName edge cases — missing app doc, enrich=false"
```

---

### Task 6: Update the client API docs

**Files:**
- Modify: `docs/client-api.md:1492` (Add Members note), `:1846` (enrich field list), `:1877` (RoomMemberEntry table row)
- Modify: `docs/client-api/events.md:1002`
- Modify: `docs/client-api/request-reply.md` (the mirrored List Members section)

**Interfaces:**
- Consumes: the `appName` JSON key (Task 1).
- Produces: nothing.

- [ ] **Step 1: Update the `RoomMemberEntry` table row**

`docs/client-api.md:1877` — replace the `name` row with:

```markdown
| `appName` | string | Optional. Bot/app display name from `apps.name`, set when the member is a bot (account ending `.bot`, or a subscription flagged `isBot`). Mutually exclusive with `engName`/`chineseName`. |
```

- [ ] **Step 2: Update the enrich field list**

`docs/client-api.md:1846` — in the `enrich` row, change `` `name` `` to `` `appName` `` in the populated-fields list.

- [ ] **Step 3: Update the Add Members note in both files**

In `docs/client-api.md:1492` and `docs/client-api/events.md:1002`, change `` and `name` (bot display name) `` to `` and `appName` (bot display name) ``.

- [ ] **Step 4: Mirror into the request/reply view**

Find the List Members section in `docs/client-api/request-reply.md` (`grep -n "name.*apps.name\|List Members" docs/client-api/request-reply.md`) and apply the same two edits (table row + enrich list). The derived views must not drift from `docs/client-api.md`.

- [ ] **Step 5: Verify no stale references remain**

Run: `grep -rn "bot display name\|apps.name\` when" docs/`
Expected: every hit reads `appName`, none reads a bare `name`.

- [ ] **Step 6: Commit**

```bash
git add docs/client-api.md docs/client-api/events.md docs/client-api/request-reply.md
git commit -m "docs(client-api): rename member.list name to appName"
```

---

### Task 7: Full verification sweep

**Files:** none modified unless a check fails.

- [ ] **Step 1: Format and lint**

Run: `make fmt && make lint`
Expected: clean.

- [ ] **Step 2: Full unit suite with race detector**

Run: `make test`
Expected: PASS (the rename touches `pkg/model`, consumed repo-wide).

- [ ] **Step 3: Integration suite**

Run: `make test-integration SERVICE=room-service`
Expected: PASS.

- [ ] **Step 4: Confirm no `Member.Name` references survive**

Run: `grep -rn "Member\.Name\b" --include=*.go . | grep -v OrgName`
Expected: no hits.

- [ ] **Step 5: SAST gate**

Run: `make sast`
Expected: no medium+ findings.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "chore: verification sweep fixes"
```

---

## Self-Review

**Spec coverage:** Goal 1 (rename) → Task 1. Goal 2 (both paths) → Tasks 3 and 4. Goal 3 (`isBot` OR suffix) → Task 4. Goal 4 (≤1 extra query, only when bots present) → Task 2's `len(botAccounts) == 0` guard, exercised by Task 5's `enrich=false` case and the pre-existing humans-only regression test. Compatibility section → Task 6. Testing table → Tasks 3, 4, 5 plus the round-trip in Task 1. The dead-regexp cleanup noted in the spec → Task 2 Step 4.

**Placeholder scan:** No TBDs; every code step carries the literal code, every test step the literal assertions and the expected failure message.

**Type consistency:** `attachAppNames(ctx, roomID string, members []model.RoomMember, botAccounts []string) error` is defined in Task 2 and called with that exact signature in Tasks 2 and 3. `attachUserDisplayNames` gains `humanAccounts, botAccounts []string` in Task 2 and is called with them in Tasks 2 and 4. `AppName` is spelled identically in Tasks 1, 3, 4, 5. `model.IsBot` is the stdlib-backed helper at `pkg/model/user.go:175`.

**Known rework:** Task 2 Step 3 writes a partition loop that Task 4 Step 4 replaces. This is deliberate — Task 2 must leave the tree compiling and green as a pure refactor, and Task 4 is where the behaviour change earns its own failing test.
